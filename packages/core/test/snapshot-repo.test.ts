import { describe, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Effect } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { SnapshotRepo } from "@opencode-ai/core/snapshot-repo"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(FSUtil.node, []))

const withTmp = <A, E, R>(body: (dir: string) => Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.promise(() => tmpdir()),
    (tmp) => body(tmp.path),
    (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
  )

describe("SnapshotRepo", () => {
  it.live("records the realpath of the worktree and clears the missing mark", () =>
    withTmp((dir) =>
      Effect.gen(function* () {
        const fsu = yield* FSUtil.Service
        const real = path.join(dir, "real")
        const link = path.join(dir, "link")
        const gitdir = path.join(dir, "gitdir")
        yield* Effect.promise(async () => {
          await fs.mkdir(real)
          await fs.mkdir(gitdir)
          await fs.symlink(real, link, "dir")
          await fs.writeFile(path.join(gitdir, SnapshotRepo.MISSING_FILE), "0")
        })

        yield* SnapshotRepo.record(fsu, gitdir, link)

        expect(yield* Effect.promise(() => fs.readFile(path.join(gitdir, SnapshotRepo.WORKTREE_FILE), "utf8"))).toBe(
          yield* Effect.promise(() => fs.realpath(real)),
        )
        expect(
          yield* Effect.promise(() =>
            fs.stat(path.join(gitdir, SnapshotRepo.MISSING_FILE)).then(
              () => true,
              () => false,
            ),
          ),
        ).toBe(false)
      }),
    ),
  )

  it.live("claims a gc at most once per hour", () =>
    withTmp((dir) =>
      Effect.gen(function* () {
        const fsu = yield* FSUtil.Service
        expect(yield* SnapshotRepo.claimGc(fsu, dir)).toBe(true)
        expect(yield* SnapshotRepo.claimGc(fsu, dir)).toBe(false)
        yield* Effect.promise(() => fs.writeFile(path.join(dir, "opencode-gc"), String(Date.now() - 61 * 60 * 1000)))
        expect(yield* SnapshotRepo.claimGc(fsu, dir)).toBe(true)
        // A claim from the future (clock moved backwards) does not block gc forever.
        yield* Effect.promise(() => fs.writeFile(path.join(dir, "opencode-gc"), String(Date.now() + 60 * 60 * 1000)))
        expect(yield* SnapshotRepo.claimGc(fsu, dir)).toBe(true)
      }),
    ),
  )
})
