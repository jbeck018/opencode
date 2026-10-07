import { describe, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Effect } from "effect"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Glob } from "@opencode-ai/core/util/glob"
import { tmpdir } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"

const it = testEffect(LayerNode.compile(FSUtil.node))

describe("Glob.literalRoots", () => {
  it.live("reads literal and brace-list first segments", () =>
    Effect.sync(() => {
      expect(Glob.literalRoots("{agent,agents}/**/*.md")).toEqual(["agent", "agents"])
      expect(Glob.literalRoots("command/*.md")).toEqual(["command"])
      expect(Glob.literalRoots("*.md")).toBeUndefined()
      expect(Glob.literalRoots("**/SKILL.md")).toBeUndefined()
      expect(Glob.literalRoots("{*.md,**/SKILL.md}")).toBeUndefined()
      expect(Glob.literalRoots("a*/x.md")).toBeUndefined()
      expect(Glob.literalRoots("../x/*.md")).toBeUndefined()
      expect(Glob.literalRoots("/abs/*.md")).toBeUndefined()
    }),
  )
})

describe("FSUtil.glob", () => {
  it.live("finds matches under an existing literal root and nothing when it is missing", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
      )
      const fsys = yield* FSUtil.Service
      expect(yield* fsys.glob("{agent,agents}/**/*.md", { cwd: tmp.path })).toEqual([])
      yield* Effect.promise(() => fs.mkdir(path.join(tmp.path, "agents", "nested"), { recursive: true }))
      yield* Effect.promise(() => Bun.write(path.join(tmp.path, "agents", "nested", "a.md"), "x"))
      expect(yield* fsys.glob("{agent,agents}/**/*.md", { cwd: tmp.path })).toEqual([
        path.join("agents", "nested", "a.md"),
      ])
    }),
  )
})
