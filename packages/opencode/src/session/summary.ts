import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { isDeepStrictEqual } from "node:util"
import { Effect, Layer, Context, Schema } from "effect"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Snapshot } from "@/snapshot"
import { Session } from "./session"
import { SessionID, MessageID } from "./schema"
import { Config } from "@/config/config"

export interface Interface {
  readonly summarize: (input: { sessionID: SessionID; messageID: MessageID }) => Effect.Effect<void>
  readonly diff: (input: { sessionID: SessionID; messageID?: MessageID }) => Effect.Effect<Snapshot.FileDiff[]>
  readonly computeDiff: (input: {
    messages: SessionV1.WithParts[]
    context?: number
  }) => Effect.Effect<Snapshot.FileDiff[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionSummary") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const snapshot = yield* Snapshot.Service
    const events = yield* EventV2Bridge.Service
    const config = yield* Config.Service
    // Snapshot trees are immutable, so a full-context diff for a (from, to) pair never changes. Caching it keeps
    // repeated review requests from re-running git work under the snapshot lock the running agent also needs.
    const fullDiffs = new Map<string, Snapshot.FileDiff[]>()

    const computeDiff = Effect.fn("SessionSummary.computeDiff")(function* (input: {
      messages: SessionV1.WithParts[]
      context?: number
    }) {
      let from: string | undefined
      let to: string | undefined
      for (const item of input.messages) {
        if (!from) {
          for (const part of item.parts) {
            if (part.type === "step-start" && part.snapshot) {
              from = part.snapshot
              break
            }
          }
        }
        for (const part of item.parts) {
          if (part.type === "step-finish" && part.snapshot) to = part.snapshot
        }
      }
      if (!from || !to) return []
      if (input.context !== undefined) return yield* snapshot.diffFull(from, to, input.context)
      const key = `${from}:${to}`
      const hit = fullDiffs.get(key)
      if (hit) {
        fullDiffs.delete(key)
        fullDiffs.set(key, hit)
        return hit
      }
      const result = yield* snapshot.diffFull(from, to)
      // Empty results also come from pruned snapshots or git failures, so only cache real diffs.
      if (!result.length) return result
      fullDiffs.set(key, result)
      while (fullDiffs.size > fullDiffCacheLimit) fullDiffs.delete(fullDiffs.keys().next().value!)
      return result
    })

    const summarize = Effect.fn("SessionSummary.summarize")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      yield* sessions.setSummary({
        sessionID: input.sessionID,
        summary: {
          additions: 0,
          deletions: 0,
          files: 0,
        },
      })
      yield* events.publish(Session.Event.Diff, { sessionID: input.sessionID, diff: [] })
      if ((yield* config.get()).snapshot === false) return
      const all = yield* sessions.messages({ sessionID: input.sessionID }).pipe(Effect.orDie)
      if (!all.length) return

      const messages = turn(all, input.messageID)
      const target = messages.find((m) => m.info.id === input.messageID)
      if (!target || target.info.role !== "user") return
      // Store standard-context patches; `diff` rebuilds full-file context from snapshots on demand.
      const msgDiffs = yield* computeDiff({ messages, context: 3 })
      // Every step re-summarizes; diffs can be megabytes, so skip the write when they are unchanged.
      // Compare as stored: JSON drops undefined fields the fresh diffs may carry.
      if (isDeepStrictEqual(target.info.summary?.diffs ?? [], JSON.parse(JSON.stringify(msgDiffs)))) return
      target.info.summary = { ...target.info.summary, diffs: msgDiffs }
      yield* sessions.updateMessage(target.info)
    })

    const diff = Effect.fn("SessionSummary.diff")(function* (input: { sessionID: SessionID; messageID?: MessageID }) {
      if (!input.messageID) return []
      const all = yield* sessions.messages({ sessionID: input.sessionID }).pipe(Effect.orDie)
      const message = all.find((item) => item.info.id === input.messageID)
      if (!message || message.info.role !== "user") return []
      const stored = message.info.summary?.diffs ?? []
      // Snapshots are pruned after a while; fall back to the stored patches once they are gone.
      const full = stored.length ? yield* computeDiff({ messages: turn(all, input.messageID) }) : []
      if (full.length) return full
      // Rows stored before snapshot diffs unquoted git paths can still carry quoted names.
      return stored.map((item) => {
        if (item.file === undefined) return item
        const file = Snapshot.unquoteGitPath(item.file)
        if (file === item.file) return item
        return { ...item, file }
      })
    })

    return Service.of({ summarize, diff, computeDiff })
  }),
)

const fullDiffCacheLimit = 16

function turn(messages: SessionV1.WithParts[], messageID: MessageID) {
  return messages.filter(
    (m) => m.info.id === messageID || (m.info.role === "assistant" && m.info.parentID === messageID),
  )
}

export const DiffInput = Schema.Struct({
  sessionID: SessionID,
  messageID: Schema.optional(MessageID),
})
export type DiffInput = Schema.Schema.Type<typeof DiffInput>

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [Session.node, Snapshot.node, EventV2Bridge.node, Config.node],
})

export * as SessionSummary from "./summary"
