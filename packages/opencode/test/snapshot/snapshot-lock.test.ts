import { afterEach, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { SnapshotRepo } from "@opencode-ai/core/snapshot-repo"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { Hash } from "@opencode-ai/core/util/hash"
import { Snapshot } from "../../src/snapshot"
import { disposeAllInstances, requireInstance, testInstanceStoreLayer, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

// The sweep runs once per Snapshot layer, so this file builds its own layer and only one test here sweeps.
const it = testEffect(
  Layer.mergeAll(
    LayerNode.compile(LayerNode.group([Snapshot.node, FSUtil.node, EffectFlock.node])),
    testInstanceStoreLayer,
  ),
)

afterEach(async () => {
  await disposeAllInstances()
})

const exists = (file: string) =>
  Effect.promise(() =>
    fs.stat(file).then(
      () => true,
      () => false,
    ),
  )

const gitdir = Effect.gen(function* () {
  const ctx = yield* requireInstance
  return path.join(Global.Path.data, "snapshot", ctx.project.id, Hash.fast(ctx.worktree))
})

// Holds a repository's flock from another fiber; the returned effect releases it.
const hold = (gitdir: string) =>
  Effect.gen(function* () {
    const flock = yield* EffectFlock.Service
    const held = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const fiber = yield* Effect.gen(function* () {
      yield* flock.acquire(SnapshotRepo.lockKey(gitdir))
      yield* Deferred.succeed(held, undefined)
      yield* Deferred.await(release)
    }).pipe(Effect.scoped, Effect.forkChild)
    yield* Deferred.await(held)
    return Deferred.succeed(release, undefined).pipe(Effect.andThen(Fiber.join(fiber)))
  })

it.instance(
  "a track waiting for the repository lock neither blocks diffs nor resists interruption",
  Effect.gen(function* () {
    const snapshot = yield* Snapshot.Service
    const hash = yield* snapshot.track()
    expect(hash).toBeTruthy()
    if (!hash) return
    const release = yield* hold(yield* gitdir)

    const tracking = yield* snapshot.track().pipe(Effect.forkChild)
    yield* Effect.sleep("300 millis")
    expect(tracking.pollUnsafe()).toBeUndefined()
    // The flock is taken before the in-process semaphore, so the waiting track does not queue these.
    expect(yield* snapshot.diff(hash).pipe(Effect.timeout("10 seconds"))).toBe("")
    expect((yield* snapshot.patch(hash).pipe(Effect.timeout("10 seconds"))).files).toEqual([])
    yield* Fiber.interrupt(tracking).pipe(Effect.timeout("5 seconds"))
    expect(Exit.hasInterrupts(yield* Fiber.await(tracking))).toBe(true)

    yield* release
    expect(yield* snapshot.track()).toBe(hash)
  }),
  { git: true },
  60_000,
)

it.instance(
  "track skips the snapshot when the repository lock stays held past the timeout",
  Effect.gen(function* () {
    const snapshot = yield* Snapshot.Service
    expect(yield* snapshot.track()).toBeTruthy()
    const release = yield* hold(yield* gitdir)

    const started = Date.now()
    expect(yield* snapshot.track()).toBeUndefined()
    expect(Date.now() - started).toBeGreaterThanOrEqual(25_000)

    yield* release
    expect(yield* snapshot.track()).toBeTruthy()
  }),
  { git: true },
  120_000,
)

it.instance(
  "the sweep skips a repository another process deleted while it waited",
  Effect.gen(function* () {
    const tmp = yield* TestInstance
    const snapshot = yield* Snapshot.Service
    expect(yield* snapshot.track()).toBeTruthy()

    const project = "lock-" + Math.random().toString(36).slice(2)
    const repository = (name: string) =>
      Effect.promise(async () => {
        const dir = path.join(Global.Path.data, "snapshot", project, name)
        await fs.mkdir(dir, { recursive: true })
        await fs.writeFile(path.join(dir, SnapshotRepo.WORKTREE_FILE), path.join(tmp.directory, "deleted-worktree"))
        return dir
      })
    const gone = yield* repository("gone")
    const release = yield* hold(gone)
    const sweeping = yield* snapshot.cleanup().pipe(Effect.forkChild)
    yield* Effect.sleep("300 millis")
    // Another process's sweep removes it while this one waits for its lock.
    yield* Effect.promise(() => fs.rm(gone, { recursive: true, force: true }))
    yield* release
    yield* Fiber.join(sweeping)
    expect(yield* exists(gone)).toBe(false)

    // The sweep succeeded, so it is not repeated in this process.
    const later = yield* repository("later")
    yield* snapshot.cleanup()
    expect(yield* exists(path.join(later, SnapshotRepo.MISSING_FILE))).toBe(false)
  }),
  { git: true },
  60_000,
)
