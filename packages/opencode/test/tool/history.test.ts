import { afterEach, describe, expect } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Effect } from "effect"
import { Agent } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Config } from "@/config/config"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Session } from "@/session/session"
import { MessageID, PartID, type SessionID } from "../../src/session/schema"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { HistoryTool } from "../../src/tool/history"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"

afterEach(async () => {
  await disposeAllInstances()
})

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const layer = (flags: Partial<RuntimeFlags.Info> = {}) =>
  LayerNode.compile(
    LayerNode.group([
      Agent.node,
      BackgroundJob.node,
      EventV2Bridge.node,
      Config.node,
      CrossSpawnSpawner.node,
      Session.node,
      SessionProjector.node,
      SessionRunState.node,
      SessionStatus.node,
      Truncate.node,
      ToolRegistry.node,
      Database.node,
      RuntimeFlags.node,
      Ripgrep.node,
    ]),
    [[RuntimeFlags.node, RuntimeFlags.layer(flags)]],
  )

const enabled = testEffect(layer({ experimentalHistoryTool: true }))
const disabled = testEffect(layer())

const ctx = (sessionID: SessionID) => ({
  sessionID,
  messageID: MessageID.make("msg_test"),
  callID: "",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
})

// A parent session whose bash output was cleared by compaction pruning, and a subagent session
// spawned from it.
const seed = Effect.fn("HistoryToolTest.seed")(function* () {
  const session = yield* Session.Service
  const parent = yield* session.create({ title: "parent" })
  const user = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: parent.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: user.id,
    sessionID: parent.id,
    type: "text",
    text: "Deploy to the blue-7 cluster",
  })
  const assistant = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "assistant",
    parentID: user.id,
    sessionID: parent.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: assistant.id,
    sessionID: parent.id,
    type: "tool",
    tool: "bash",
    callID: "call-1",
    state: {
      status: "completed",
      input: { command: "cat .env" },
      output: "HOST=localhost\nSECRET_PORT=4821\nMODE=dev",
      title: "cat .env",
      metadata: {},
      time: { start: 1, end: 2, compacted: 3 },
    },
  })
  const child = yield* session.create({ title: "child", parentID: parent.id })
  return { parent, child, assistant }
})

describe("tool.history", () => {
  enabled.instance("searches cleared tool output from a subagent's parent session", () =>
    Effect.gen(function* () {
      const seeded = yield* seed()
      const info = yield* HistoryTool
      const tool = yield* info.init()

      const search = yield* tool.execute({ query: "secret_port=\\d+" }, ctx(seeded.child.id))
      expect(search.metadata.matches).toBe(1)
      expect(search.output).toContain(`[${seeded.assistant.id}] assistant bash output:`)
      expect(search.output).toContain("SECRET_PORT=4821")

      const read = yield* tool.execute({ messageID: seeded.assistant.id }, ctx(seeded.child.id))
      expect(read.output).toContain('--- bash input\n{"command":"cat .env"}')
      expect(read.output).toContain("HOST=localhost\nSECRET_PORT=4821\nMODE=dev")

      const user = yield* tool.execute({ query: "blue-\\d" }, ctx(seeded.parent.id))
      expect(user.output).toContain("user text: Deploy to the blue-7 cluster")

      expect((yield* tool.execute({ query: "no such thing" }, ctx(seeded.parent.id))).output).toBe("No matches")
    }),
  )

  enabled.instance("registers the tool and the recall subagent", () =>
    Effect.gen(function* () {
      const agents = yield* Agent.Service
      const build = yield* agents.get("build")
      const registry = yield* ToolRegistry.Service
      const tools = yield* registry.tools({ ...ref, agent: build })
      expect(tools.some((tool) => tool.id === "history")).toBe(true)
      expect((yield* agents.get("recall"))?.mode).toBe("subagent")
    }),
  )

  disabled.instance("stays off without the flag", () =>
    Effect.gen(function* () {
      const agents = yield* Agent.Service
      const build = yield* agents.get("build")
      const registry = yield* ToolRegistry.Service
      const tools = yield* registry.tools({ ...ref, agent: build })
      expect(tools.some((tool) => tool.id === "history")).toBe(false)
      expect(yield* agents.get("recall")).toBeUndefined()
    }),
  )
})
