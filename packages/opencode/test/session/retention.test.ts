import { describe, expect } from "bun:test"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Database } from "@opencode-ai/core/database/database"
import { Effect, Exit, Layer } from "effect"
import { Config } from "@/config/config"
import { Session as SessionNs } from "@/session/session"
import { SessionRetention } from "@/session/retention"
import type { SessionID } from "../../src/session/schema"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { testEffect } from "../lib/effect"
import { TestConfig } from "../fixture/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { EventV2Bridge } from "@/event-v2-bridge"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"

const retention: { archived_days?: number } = {}

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      SessionRetention.node,
      SessionNs.node,
      Database.node,
      EventV2Bridge.node,
      SessionProjector.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [Config.node, TestConfig.layer({ getGlobal: () => Effect.succeed({ retention: { ...retention } }) })],
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
    ],
  ),
)

const day = 24 * 60 * 60 * 1000

const exists = (id: SessionID) =>
  SessionNs.use.get(id).pipe(
    Effect.exit,
    Effect.map((exit) => Exit.isSuccess(exit)),
  )

describe("session retention", () => {
  it.instance("deletes only sessions archived longer ago than archived_days", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const sessionRetention = yield* SessionRetention.Service
      const active = yield* session.create({})
      const old = yield* session.create({})
      const oldChild = yield* session.create({ parentID: old.id })
      const recent = yield* session.create({})
      yield* session.setArchived({ sessionID: old.id, time: Date.now() - 10 * day })
      yield* session.setArchived({ sessionID: recent.id, time: Date.now() - day })
      // An archived child goes with its parent even when it was archived more recently.
      yield* session.setArchived({ sessionID: oldChild.id, time: Date.now() - day })

      retention.archived_days = undefined
      expect(yield* sessionRetention.run()).toBe(0)
      expect(yield* exists(old.id)).toBe(true)

      retention.archived_days = 7
      // The parent and its child are removed and counted once each.
      expect(yield* sessionRetention.run()).toBe(2)
      expect(yield* exists(old.id)).toBe(false)
      expect(yield* exists(oldChild.id)).toBe(false)
      expect(yield* exists(recent.id)).toBe(true)
      expect(yield* exists(active.id)).toBe(true)

      yield* session.remove(active.id)
      yield* session.remove(recent.id)
    }),
  )

  it.instance("keeps sessions whose legacy archived time is zero or negative", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const sessionRetention = yield* SessionRetention.Service
      const zero = yield* session.create({})
      const negative = yield* session.create({})
      yield* session.setArchived({ sessionID: zero.id, time: 0 })
      yield* session.setArchived({ sessionID: negative.id, time: -10 * day })

      retention.archived_days = 7
      expect(yield* sessionRetention.run()).toBe(0)
      expect(yield* exists(zero.id)).toBe(true)
      expect(yield* exists(negative.id)).toBe(true)

      yield* session.remove(zero.id)
      yield* session.remove(negative.id)
    }),
  )

  it.instance("keeps an archived parent that still has an unarchived descendant", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const sessionRetention = yield* SessionRetention.Service
      const parent = yield* session.create({})
      const child = yield* session.create({ parentID: parent.id })
      const grandchild = yield* session.create({ parentID: child.id })
      yield* session.setArchived({ sessionID: parent.id, time: Date.now() - 10 * day })
      yield* session.setArchived({ sessionID: child.id, time: Date.now() - 10 * day })

      retention.archived_days = 7
      expect(yield* sessionRetention.run()).toBe(0)
      expect(yield* exists(parent.id)).toBe(true)
      expect(yield* exists(child.id)).toBe(true)
      expect(yield* exists(grandchild.id)).toBe(true)

      yield* session.setArchived({ sessionID: grandchild.id, time: Date.now() - day })
      expect(yield* sessionRetention.run()).toBe(3)
      expect(yield* exists(parent.id)).toBe(false)
      expect(yield* exists(child.id)).toBe(false)
      expect(yield* exists(grandchild.id)).toBe(false)
    }),
  )
})
