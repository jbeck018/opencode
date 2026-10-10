import { afterEach, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Deferred, Effect, Fiber, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { SnapshotRepo } from "@opencode-ai/core/snapshot-repo"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { Snapshot } from "../../src/snapshot"
import { disposeAllInstances, testInstanceStoreLayer, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

// The sweep runs once per Snapshot layer, so this file builds its own layer instead of sharing one.
const it = testEffect(
  Layer.mergeAll(
    LayerNode.compile(LayerNode.group([Snapshot.node, FSUtil.node, EffectFlock.node])),
    testInstanceStoreLayer,
  ),
)

const DAY_MS = 24 * 60 * 60 * 1000

afterEach(async () => {
  await disposeAllInstances()
})

// Mimics a snapshot repository; the sweep only looks at the directory, the worktree record and the missing mark.
const repository = (project: string, name: string, input: { worktree?: string; missingDays?: number }) =>
  Effect.promise(async () => {
    const gitdir = path.join(Global.Path.data, "snapshot", project, name)
    await fs.mkdir(gitdir, { recursive: true })
    await fs.writeFile(path.join(gitdir, "HEAD"), "ref: refs/heads/main\n")
    if (input.worktree) await fs.writeFile(path.join(gitdir, SnapshotRepo.WORKTREE_FILE), input.worktree)
    if (input.missingDays !== undefined)
      await fs.writeFile(path.join(gitdir, SnapshotRepo.MISSING_FILE), String(Date.now() - input.missingDays * DAY_MS))
    return gitdir
  })

const exists = (file: string) =>
  Effect.promise(() =>
    fs.stat(file).then(
      () => true,
      () => false,
    ),
  )

const read = (file: string) => Effect.promise(() => fs.readFile(file, "utf8"))

const project = () => "sweep-" + Math.random().toString(36).slice(2)

it.instance(
  "deletes a snapshot repository only after its worktree stays missing for the grace period",
  Effect.gen(function* () {
    const tmp = yield* TestInstance
    const snapshot = yield* Snapshot.Service
    const id = project()
    const deleted = path.join(tmp.directory, "deleted-worktree")
    expect(yield* snapshot.track()).toBeTruthy()

    const fresh = yield* repository(id, "fresh", { worktree: deleted })
    const recent = yield* repository(id, "recent", { worktree: deleted, missingDays: 13 })
    const expired = yield* repository(id, "expired", { worktree: deleted, missingDays: 15 })
    const back = yield* repository(id, "back", { worktree: tmp.directory, missingDays: 30 })
    const volume = yield* repository(id, "volume", { worktree: "/Volumes/ejected-disk/app", missingDays: 365 })
    const media = yield* repository(id, "media", { worktree: "/run/media/user/usb/app", missingDays: 365 })
    const drive = yield* repository(id, "drive", { worktree: "D:\\projects\\app", missingDays: 365 })
    const legacy = yield* repository(id, "legacy", {})

    yield* snapshot.cleanup()

    // The first miss only records when it was seen.
    expect(yield* exists(fresh)).toBe(true)
    const since = Number(yield* read(path.join(fresh, SnapshotRepo.MISSING_FILE)))
    expect(Date.now() - since).toBeLessThan(60_000)
    expect(yield* exists(recent)).toBe(true)
    expect(yield* exists(expired)).toBe(false)
    // A worktree that came back clears its mark.
    expect(yield* exists(back)).toBe(true)
    expect(yield* exists(path.join(back, SnapshotRepo.MISSING_FILE))).toBe(false)
    // Removable and network volumes are kept however long they are gone.
    expect(yield* exists(volume)).toBe(true)
    expect(yield* exists(media)).toBe(true)
    expect(yield* exists(drive)).toBe(true)
    expect(yield* exists(legacy)).toBe(true)
  }),
  { git: true },
  30_000,
)

it.instance(
  "records the canonical worktree and gcs a repository at most once per hour",
  Effect.gen(function* () {
    const tmp = yield* TestInstance
    const snapshot = yield* Snapshot.Service
    expect(yield* snapshot.track()).toBeTruthy()

    const real = yield* Effect.promise(() => fs.realpath(tmp.directory))
    // Repositories recording this instance's worktree.
    const own = Effect.promise(async () => {
      const root = path.join(Global.Path.data, "snapshot")
      const dirs = (await fs.readdir(root, { recursive: true }))
        .filter((file) => file.endsWith(SnapshotRepo.WORKTREE_FILE))
        .map((file) => path.dirname(path.join(root, file)))
      const records = await Promise.all(
        dirs.map((dir) => fs.readFile(path.join(dir, SnapshotRepo.WORKTREE_FILE), "utf8")),
      )
      return dirs.filter((_, index) => records[index] === real)
    })
    // track does not record; cleanup records the realpath, matching what the v2 snapshot service writes.
    expect(yield* own).toEqual([])
    yield* snapshot.cleanup()
    const recorded = yield* own
    expect(recorded).toHaveLength(1)
    const marker = path.join(recorded[0]!, "opencode-gc")

    const claimed = yield* read(marker)
    yield* snapshot.cleanup()
    // A gc started within the hour, so the next cleanup (or the v2 service) leaves it to that claimant.
    expect(yield* read(marker)).toBe(claimed)

    const stale = Date.now() - 2 * 60 * 60 * 1000
    yield* Effect.promise(() => fs.writeFile(marker, String(stale)))
    yield* snapshot.cleanup()
    expect(Number(yield* read(marker))).toBeGreaterThan(stale)
  }),
  { git: true },
  30_000,
)

it.instance(
  "sweeps once per process, retries a failed sweep, and waits for the repository lock",
  Effect.gen(function* () {
    const tmp = yield* TestInstance
    const snapshot = yield* Snapshot.Service
    const flock = yield* EffectFlock.Service
    const id = project()
    const deleted = path.join(tmp.directory, "deleted-worktree")
    expect(yield* snapshot.track()).toBeTruthy()

    // A missing mark that cannot be read fails the sweep.
    const broken = yield* repository(id, "broken", { worktree: deleted })
    yield* Effect.promise(() => fs.mkdir(path.join(broken, SnapshotRepo.MISSING_FILE)))
    yield* snapshot.cleanup()
    expect(yield* exists(broken)).toBe(true)

    // The failed sweep was not cached: the next cleanup sweeps again, once another writer releases the lock.
    yield* Effect.promise(async () => {
      await fs.rm(path.join(broken, SnapshotRepo.MISSING_FILE), { recursive: true })
      await fs.writeFile(path.join(broken, SnapshotRepo.MISSING_FILE), String(Date.now() - 15 * DAY_MS))
    })
    const held = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const holder = yield* Effect.gen(function* () {
      yield* flock.acquire(SnapshotRepo.lockKey(broken))
      yield* Deferred.succeed(held, undefined)
      yield* Deferred.await(release)
    }).pipe(Effect.scoped, Effect.forkChild)
    yield* Deferred.await(held)
    const sweeping = yield* snapshot.cleanup().pipe(Effect.forkChild)
    yield* Effect.sleep("500 millis")
    expect(yield* exists(broken)).toBe(true)
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(holder)
    yield* Fiber.join(sweeping)
    expect(yield* exists(broken)).toBe(false)

    // A successful sweep is not repeated in this process.
    const later = yield* repository(id, "later", { worktree: deleted, missingDays: 15 })
    yield* snapshot.cleanup()
    expect(yield* exists(later)).toBe(true)
  }),
  { git: true },
  30_000,
)
