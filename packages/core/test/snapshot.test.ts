import { $ } from "bun"
import { describe, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SnapshotRepo } from "@opencode-ai/core/snapshot-repo"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { Git } from "@opencode-ai/core/git"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath, RelativePath } from "@opencode-ai/core/schema"
import { Snapshot } from "@opencode-ai/core/snapshot"
import { Hash } from "@opencode-ai/core/util/hash"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

// Each test runs many git processes, which outlast bun's 5 s default on slow runners.
const timeout = 60_000

describe("Snapshot", () => {
  testEffect(Layer.empty).live(
    "captures and restores Location-scoped changes",
    () =>
      Effect.acquireUseRelease(
        Effect.promise(() => tmpdir()),
        (tmp) =>
          Effect.gen(function* () {
            const project = path.join(tmp.path, "project")
            const location = path.join(project, "scope")
            yield* Effect.promise(async () => {
              await fs.mkdir(location, { recursive: true })
              await fs.writeFile(path.join(location, "tracked.txt"), "one\n")
              await fs.writeFile(path.join(project, "outside.txt"), "outside\n")
              await $`git init`.cwd(project).quiet()
              await $`git config core.fsmonitor false`.cwd(project).quiet()
              await $`git config commit.gpgsign false`.cwd(project).quiet()
              await $`git config user.email test@opencode.test`.cwd(project).quiet()
              await $`git config user.name Test`.cwd(project).quiet()
              await $`git add .`.cwd(project).quiet()
              await $`git commit -m initial`.cwd(project).quiet()
            })

            const layer = snapshotLayer(tmp.path, location)
            yield* Effect.gen(function* () {
              const snapshot = yield* Snapshot.Service
              const before = yield* snapshot.capture()
              expect(before).toBeDefined()
              if (!before) return

              yield* Effect.promise(async () => {
                await fs.writeFile(path.join(location, "tracked.txt"), "two\n")
                await fs.writeFile(path.join(location, "added.txt"), "added\n")
                await fs.writeFile(path.join(project, "outside.txt"), "changed outside\n")
              })
              const after = yield* snapshot.capture()
              expect(after).toBeDefined()
              if (!after) return

              expect(yield* snapshot.files({ from: before, to: after })).toEqual([
                RelativePath.make("scope/added.txt"),
                RelativePath.make("scope/tracked.txt"),
              ])
              const plan = new Map([[RelativePath.make("scope/tracked.txt"), before]])
              const preview = yield* snapshot.preview({ files: plan, context: 1 })
              expect(preview).toHaveLength(1)
              expect(preview[0]?.path).toBe(RelativePath.make("scope/tracked.txt"))
              yield* snapshot.restore({ files: plan })
              expect(yield* read(path.join(location, "tracked.txt"))).toBe("one\n")
              expect(yield* read(path.join(location, "added.txt"))).toBe("added\n")
              expect(yield* read(path.join(project, "outside.txt"))).toBe("changed outside\n")
            }).pipe(Effect.provide(layer))
          }),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      ),
    timeout,
  )

  testEffect(Layer.empty).live(
    "treats capture outside Git as unavailable",
    () =>
      Effect.acquireUseRelease(
        Effect.promise(() => tmpdir()),
        (tmp) =>
          Effect.gen(function* () {
            expect(
              yield* Effect.gen(function* () {
                const snapshot = yield* Snapshot.Service
                return yield* snapshot.capture()
              }).pipe(Effect.provide(snapshotLayer(tmp.path, tmp.path))),
            ).toBeUndefined()
          }),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      ),
    timeout,
  )

  testEffect(Layer.empty).live(
    "isolates snapshot indexes by canonical Git worktree",
    () =>
      Effect.acquireUseRelease(
        Effect.promise(() => tmpdir()),
        (tmp) =>
          Effect.gen(function* () {
            const project = path.join(tmp.path, "project")
            const linked = path.join(tmp.path, "linked")
            yield* Effect.promise(async () => {
              await fs.mkdir(project)
              await fs.writeFile(path.join(project, "tracked.txt"), "main\n")
              await $`git init`.cwd(project).quiet()
              await $`git config core.fsmonitor false`.cwd(project).quiet()
              await $`git config commit.gpgsign false`.cwd(project).quiet()
              await $`git config user.email test@opencode.test`.cwd(project).quiet()
              await $`git config user.name Test`.cwd(project).quiet()
              await $`git add .`.cwd(project).quiet()
              await $`git commit -m initial`.cwd(project).quiet()
              await $`git worktree add --detach ${linked} HEAD`.cwd(project).quiet()
            })

            const capture = (directory: string) =>
              Effect.gen(function* () {
                const snapshot = yield* Snapshot.Service
                return yield* snapshot.capture()
              }).pipe(Effect.provide(snapshotLayer(tmp.path, directory)))
            expect(yield* capture(project)).toBeDefined()
            expect(yield* capture(linked)).toBeDefined()

            const projectID = yield* Effect.gen(function* () {
              return (yield* Location.Service).project.id
            }).pipe(
              Effect.provide(
                AppNodeBuilder.build(Location.boundNode(Location.Ref.make({ directory: AbsolutePath.make(project) }))),
              ),
            )
            expect(
              yield* Effect.promise(() => fs.stat(path.join(tmp.path, "snapshot", projectID, Hash.fast(project)))),
            ).toBeDefined()
            expect(
              yield* Effect.promise(() => fs.stat(path.join(tmp.path, "snapshot", projectID, Hash.fast(linked)))),
            ).toBeDefined()
          }),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      ),
    timeout,
  )

  testEffect(Layer.empty).live(
    "cleanup compacts the repository and records its worktree",
    () =>
      Effect.acquireUseRelease(
        Effect.promise(() => tmpdir()),
        (tmp) =>
          Effect.gen(function* () {
            const project = path.join(tmp.path, "project")
            yield* Effect.promise(async () => {
              await fs.mkdir(project)
              await fs.writeFile(path.join(project, "tracked.txt"), "one\n")
              await $`git init`.cwd(project).quiet()
            })

            yield* Effect.gen(function* () {
              const snapshot = yield* Snapshot.Service
              yield* Effect.promise(() => fs.writeFile(path.join(project, "untracked.txt"), "loose\n"))
              expect(yield* snapshot.capture()).toBeDefined()
              const projectID = yield* Effect.gen(function* () {
                return (yield* Location.Service).project.id
              }).pipe(
                Effect.provide(
                  AppNodeBuilder.build(
                    Location.boundNode(Location.Ref.make({ directory: AbsolutePath.make(project) })),
                  ),
                ),
              )
              const worktree = yield* Effect.promise(() => fs.realpath(project))
              const gitDirectory = path.join(tmp.path, "snapshot", projectID, Hash.fast(worktree))
              // `count` is the number of loose objects, which gc packs.
              const count = () => Effect.promise(() => $`git --git-dir ${gitDirectory} count-objects -v`.quiet().text())
              expect(yield* count()).not.toMatch(/^count: 0$/m)

              yield* snapshot.cleanup()

              expect(yield* count()).toMatch(/^count: 0$/m)
              expect(yield* read(path.join(gitDirectory, "opencode-worktree"))).toBe(worktree)

              // The gc is claimed for the hour, so a second cleanup (here, or the v1 service) skips it.
              const claimed = yield* read(path.join(gitDirectory, "opencode-gc"))
              yield* Effect.promise(() => fs.writeFile(path.join(project, "later.txt"), "later\n"))
              expect(yield* snapshot.capture()).toBeDefined()
              yield* snapshot.cleanup()
              expect(yield* read(path.join(gitDirectory, "opencode-gc"))).toBe(claimed)
              expect(yield* count()).not.toMatch(/^count: 0$/m)

              // Capture waits for another holder of the repository lock, such as the sweep.
              const flock = yield* EffectFlock.Service
              const held = yield* Deferred.make<void>()
              const release = yield* Deferred.make<void>()
              const holder = yield* Effect.gen(function* () {
                yield* flock.acquire(SnapshotRepo.lockKey(gitDirectory))
                yield* Deferred.succeed(held, undefined)
                yield* Deferred.await(release)
              }).pipe(Effect.scoped, Effect.forkChild)
              yield* Deferred.await(held)
              const capturing = yield* snapshot.capture().pipe(Effect.forkChild)
              yield* Effect.sleep("300 millis")
              expect(capturing.pollUnsafe()).toBeUndefined()
              yield* Deferred.succeed(release, undefined)
              yield* Fiber.join(holder)
              expect(yield* Fiber.join(capturing)).toBeDefined()
            }).pipe(Effect.provide(snapshotLayer(tmp.path, project)))
          }),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      ),
    timeout,
  )

  testEffect(Layer.empty).live(
    "checks out a legacy revert snapshot without removing unrelated files",
    () =>
      Effect.acquireUseRelease(
        Effect.promise(() => tmpdir()),
        (tmp) =>
          Effect.gen(function* () {
            const project = path.join(tmp.path, "project")
            yield* Effect.promise(async () => {
              await fs.mkdir(project)
              await fs.writeFile(path.join(project, "tracked.txt"), "one\n")
              await $`git init`.cwd(project).quiet()
              await $`git config core.fsmonitor false`.cwd(project).quiet()
              await $`git config commit.gpgsign false`.cwd(project).quiet()
              await $`git config user.email test@opencode.test`.cwd(project).quiet()
              await $`git config user.name Test`.cwd(project).quiet()
              await $`git add .`.cwd(project).quiet()
              await $`git commit -m initial`.cwd(project).quiet()
            })

            yield* Effect.gen(function* () {
              const snapshot = yield* Snapshot.Service
              const before = yield* snapshot.capture()
              expect(before).toBeDefined()
              if (!before) return
              yield* Effect.promise(async () => {
                await fs.writeFile(path.join(project, "tracked.txt"), "two\n")
                await fs.writeFile(path.join(project, "unrelated.txt"), "keep\n")
              })
              yield* snapshot.checkout(before)
              expect(yield* read(path.join(project, "tracked.txt"))).toBe("one\n")
              expect(yield* read(path.join(project, "unrelated.txt"))).toBe("keep\n")
            }).pipe(Effect.provide(snapshotLayer(tmp.path, project)))
          }),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      ),
    timeout,
  )

  testEffect(Layer.empty).live(
    "cleanup runs gc without holding the repository lock",
    () =>
      Effect.acquireUseRelease(
        Effect.promise(() => tmpdir()),
        (tmp) =>
          Effect.gen(function* () {
            const project = path.join(tmp.path, "project")
            yield* Effect.promise(async () => {
              await fs.mkdir(project)
              await fs.writeFile(path.join(project, "tracked.txt"), "one\n")
              await $`git init`.cwd(project).quiet()
            })
            const started = yield* Deferred.make<void>()
            const proceed = yield* Deferred.make<void>()

            yield* Effect.gen(function* () {
              const snapshot = yield* Snapshot.Service
              const flock = yield* EffectFlock.Service
              expect(yield* snapshot.capture()).toBeDefined()
              const projectID = yield* Effect.gen(function* () {
                return (yield* Location.Service).project.id
              }).pipe(
                Effect.provide(
                  AppNodeBuilder.build(
                    Location.boundNode(Location.Ref.make({ directory: AbsolutePath.make(project) })),
                  ),
                ),
              )
              const worktree = yield* Effect.promise(() => fs.realpath(project))
              const gitDirectory = path.join(tmp.path, "snapshot", projectID, Hash.fast(worktree))

              const cleaning = yield* snapshot.cleanup().pipe(Effect.forkChild)
              yield* Deferred.await(started)
              // While gc runs, other writers still get the lock, and a capture goes through.
              const locked = yield* flock
                .withLock(Effect.void, SnapshotRepo.lockKey(gitDirectory), { timeout: "2 seconds" })
                .pipe(Effect.exit)
              expect(Exit.isSuccess(locked)).toBe(true)
              yield* Effect.promise(() => fs.writeFile(path.join(project, "during.txt"), "during\n"))
              expect(yield* snapshot.capture()).toBeDefined()
              yield* Deferred.succeed(proceed, undefined)
              yield* Fiber.join(cleaning)
              expect(yield* read(path.join(gitDirectory, "opencode-gc"))).toMatch(/^\d+$/)
            }).pipe(Effect.provide(snapshotLayer(tmp.path, project, gatedGit(started, proceed))))
          }),
        (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
      ),
    timeout,
  )
})

function snapshotLayer(data: string, directory: string, git?: Layer.Layer<Git.Service>) {
  return AppNodeBuilder.build(LayerNode.group([Snapshot.node, EffectFlock.node]), [
    [Location.node, Location.boundNode(Location.Ref.make({ directory: AbsolutePath.make(directory) }))],
    [Global.node, Global.layerWith({ data, config: path.join(data, "config") })],
    ...(git ? [[Git.node, git] as const] : []),
  ])
}

// Real git whose gc first signals `started`, then waits for `proceed`.
function gatedGit(started: Deferred.Deferred<void>, proceed: Deferred.Deferred<void>) {
  return Layer.effect(
    Git.Service,
    Effect.gen(function* () {
      const git = yield* Git.Service
      return Git.Service.of({
        ...git,
        repo: {
          ...git.repo,
          gc: (repository, input) =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(proceed)),
              Effect.andThen(git.repo.gc(repository, input)),
            ),
        },
      })
    }),
  ).pipe(Layer.provide(AppNodeBuilder.build(Git.node)))
}

function read(file: string) {
  return Effect.promise(() => fs.readFile(file, "utf8")).pipe(Effect.map((content) => content.replaceAll("\r\n", "\n")))
}
