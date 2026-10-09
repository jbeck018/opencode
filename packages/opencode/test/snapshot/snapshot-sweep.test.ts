import { afterEach, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { Snapshot } from "../../src/snapshot"
import { disposeAllInstances, testInstanceStoreLayer, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

// The sweep runs once per Snapshot layer, so this file builds its own layer instead of sharing one.
const it = testEffect(
  Layer.mergeAll(LayerNode.compile(LayerNode.group([Snapshot.node, FSUtil.node])), testInstanceStoreLayer),
)

const DAY_MS = 24 * 60 * 60 * 1000

afterEach(async () => {
  await disposeAllInstances()
})

// Mimics a snapshot repository; the sweep only looks at the directory, its mtime and the worktree record.
const repository = (project: string, name: string, input: { worktree?: string; days?: number }) =>
  Effect.promise(async () => {
    const gitdir = path.join(Global.Path.data, "snapshot", project, name)
    await fs.mkdir(gitdir, { recursive: true })
    await fs.writeFile(path.join(gitdir, "HEAD"), "ref: refs/heads/main\n")
    if (input.worktree) await fs.writeFile(path.join(gitdir, "opencode-worktree"), input.worktree)
    if (input.days) {
      const time = new Date(Date.now() - input.days * DAY_MS)
      if (input.worktree) await fs.utimes(path.join(gitdir, "opencode-worktree"), time, time)
      await fs.utimes(gitdir, time, time)
    }
    return gitdir
  })

const exists = (file: string) =>
  Effect.promise(() =>
    fs.stat(file).then(
      () => true,
      () => false,
    ),
  )

it.instance(
  "sweeps only snapshot repositories whose worktree is gone, once per process",
  Effect.gen(function* () {
    const tmp = yield* TestInstance
    const snapshot = yield* Snapshot.Service
    const project = "sweep-" + Math.random().toString(36).slice(2)
    expect(yield* snapshot.track()).toBeTruthy()

    const gone = yield* repository(project, "gone", { worktree: path.join(tmp.directory, "deleted-worktree") })
    const old = yield* repository(project, "old", { worktree: tmp.directory, days: 365 })
    const legacy = yield* repository(project, "legacy", { days: 365 })
    const active = yield* repository(project, "active", { worktree: tmp.directory })
    const recent = yield* repository(project, "recent", {})

    yield* snapshot.cleanup()

    expect(yield* exists(gone)).toBe(false)
    // Age alone never deletes: an old repository with a live worktree, or one without a record, is kept.
    expect(yield* exists(old)).toBe(true)
    expect(yield* exists(legacy)).toBe(true)
    expect(yield* exists(active)).toBe(true)
    expect(yield* exists(recent)).toBe(true)

    // The instance's own repository now records its worktree.
    const own = yield* Effect.promise(() =>
      fs
        .readdir(path.join(Global.Path.data, "snapshot"), { recursive: true })
        .then((files) =>
          files
            .filter((file) => file.endsWith("opencode-worktree"))
            .map((file) => path.join(Global.Path.data, "snapshot", file)),
        ),
    )
    const recorded = yield* Effect.promise(() => Promise.all(own.map((file) => fs.readFile(file, "utf8"))))
    expect(recorded).toContain(tmp.directory)

    const later = yield* repository(project, "later", { worktree: path.join(tmp.directory, "deleted-worktree") })
    yield* snapshot.cleanup()
    expect(yield* exists(later)).toBe(true)
  }),
  { git: true },
  30_000,
)
