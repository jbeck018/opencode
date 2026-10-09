import { describe, expect } from "bun:test"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Database } from "@opencode-ai/core/database/database"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import { Effect, Layer } from "effect"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceState } from "@/effect/instance-state"
import { InstanceRef } from "@/effect/instance-ref"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceBootstrap } from "@/project/bootstrap"
import { InstanceStore } from "@/project/instance-store"
import { Session } from "@/session/session"
import { SessionSummary } from "@/session/summary"
import { Snapshot } from "@/snapshot"
import { TestConfig } from "../fixture/config"
import { testEffect } from "../lib/effect"

// Patch size per target tree, so a test can control how much each cached diff weighs.
const sizes = new Map<string, number>()
const calls: string[] = []

const snapshot = Layer.succeed(
  Snapshot.Service,
  Snapshot.Service.of({
    init: () => Effect.void,
    cleanup: () => Effect.void,
    track: () => Effect.succeed(undefined),
    patch: (hash) => Effect.succeed({ hash, files: [] }),
    restore: () => Effect.void,
    revert: () => Effect.void,
    diff: () => Effect.succeed(""),
    diffFull: (from, to) =>
      Effect.sync(() => {
        calls.push(`${from}:${to}`)
        return [{ file: "a.txt", patch: "x".repeat(sizes.get(to) ?? 10), additions: 1, deletions: 0 }]
      }),
  }),
)

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      SessionSummary.node,
      Session.node,
      Database.node,
      EventV2Bridge.node,
      SessionProjector.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [Snapshot.node, snapshot],
      [Config.node, TestConfig.layer()],
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
    ],
  ),
)

// computeDiff only reads the snapshot hashes of step-start and step-finish parts.
const turn = (from: string, to: string) =>
  [
    {
      parts: [
        { type: "step-start", snapshot: from },
        { type: "step-finish", snapshot: to },
      ],
    },
  ] as unknown as SessionV1.WithParts[]

const mb = 1024 * 1024

describe("session summary full diff cache", () => {
  it.instance("evicts the oldest diffs once cached patches exceed the byte budget", () =>
    Effect.gen(function* () {
      const summary = yield* SessionSummary.Service
      calls.length = 0
      sizes
        .set("bytes-1", 20 * mb)
        .set("bytes-2", 20 * mb)
        .set("bytes-huge", 40 * mb)

      yield* summary.computeDiff({ messages: turn("base", "bytes-1") })
      yield* summary.computeDiff({ messages: turn("base", "bytes-1") })
      expect(calls).toEqual(["base:bytes-1"])

      // Together the two diffs exceed 32 MB, so the first one is evicted.
      yield* summary.computeDiff({ messages: turn("base", "bytes-2") })
      yield* summary.computeDiff({ messages: turn("base", "bytes-2") })
      yield* summary.computeDiff({ messages: turn("base", "bytes-1") })
      expect(calls).toEqual(["base:bytes-1", "base:bytes-2", "base:bytes-1"])

      // A single diff over the budget is never cached.
      calls.length = 0
      yield* summary.computeDiff({ messages: turn("base", "bytes-huge") })
      yield* summary.computeDiff({ messages: turn("base", "bytes-huge") })
      expect(calls).toEqual(["base:bytes-huge", "base:bytes-huge"])
    }),
  )

  it.instance("does not share cached diffs between snapshot repositories", () =>
    Effect.gen(function* () {
      const summary = yield* SessionSummary.Service
      const ctx = yield* InstanceState.context
      calls.length = 0

      yield* summary.computeDiff({ messages: turn("base", "shared") })
      yield* summary.computeDiff({ messages: turn("base", "shared") })
      expect(calls).toEqual(["base:shared"])

      yield* summary
        .computeDiff({ messages: turn("base", "shared") })
        .pipe(Effect.provideService(InstanceRef, { ...ctx, worktree: `${ctx.worktree}-other` }))
      expect(calls).toEqual(["base:shared", "base:shared"])
    }),
  )
})
