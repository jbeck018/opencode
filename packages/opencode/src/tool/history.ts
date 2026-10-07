import { Effect, Schema } from "effect"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import { Session } from "@/session/session"
import type { SessionID } from "@/session/schema"
import DESCRIPTION from "./history.txt"
import * as Tool from "./tool"

export const Parameters = Schema.Struct({
  query: Schema.optional(Schema.String).annotate({
    description: "Case-insensitive regular expression to search for across the full session transcript",
  }),
  messageID: Schema.optional(Schema.String).annotate({
    description: "Read one message in full, using a message ID from a search result",
  }),
  limit: Schema.optional(Schema.Number).annotate({
    description: "Maximum number of search hits to return (default 20)",
  }),
})

const SNIPPET = 160
const MESSAGE_LIMIT = 50_000

// The transcript the model sees ends at the last compaction and has old tool outputs cleared, but
// every message and output stays in the database. This tool lets the model search and reread that
// full record (including the parent sessions of a subagent) instead of relying on the summary.
export const HistoryTool = Tool.define(
  "history",
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const transcript = yield* lineage(sessions, ctx.sessionID)
          if (params.messageID) {
            const found = transcript.find((message) => message.info.id === params.messageID)
            return {
              title: params.messageID,
              metadata: { matches: found ? 1 : 0 },
              output: found ? render(found).slice(0, MESSAGE_LIMIT) : `No message ${params.messageID} in this session`,
            }
          }
          if (!params.query) throw new Error("Provide a query to search or a messageID to read")
          const pattern = new RegExp(params.query, "i")
          const hits = transcript
            .flatMap((message) =>
              entries(message).flatMap((entry) => {
                const match = pattern.exec(entry.text)
                if (!match) return []
                const start = Math.max(0, match.index - SNIPPET)
                const end = Math.min(entry.text.length, match.index + match[0].length + SNIPPET)
                return [
                  `[${message.info.id}] ${message.info.role} ${entry.label}: ${start > 0 ? "…" : ""}${entry.text.slice(start, end)}${end < entry.text.length ? "…" : ""}`,
                ]
              }),
            )
            .slice(0, params.limit ?? 20)
          return {
            title: params.query,
            metadata: { matches: hits.length },
            output: hits.length ? hits.join("\n\n") : "No matches",
          }
        }),
    } satisfies Tool.DefWithoutID<typeof Parameters>
  }),
)

// Newest first: the current session, then each parent a subagent was spawned from.
function lineage(sessions: Session.Interface, sessionID: SessionID): Effect.Effect<SessionV1.WithParts[]> {
  return Effect.gen(function* () {
    const info = yield* sessions.get(sessionID).pipe(Effect.orDie)
    const messages = (yield* sessions.messages({ sessionID }).pipe(Effect.orDie)).toReversed()
    if (!info.parentID) return messages
    return messages.concat(yield* lineage(sessions, info.parentID))
  })
}

function entries(message: SessionV1.WithParts) {
  return message.parts.flatMap((part) => {
    if (part.type === "text" || part.type === "reasoning") return [{ label: part.type, text: part.text }]
    if (part.type !== "tool") return []
    const input = { label: `${part.tool} input`, text: JSON.stringify(part.state.input) }
    if (part.state.status === "completed") return [input, { label: `${part.tool} output`, text: part.state.output }]
    if (part.state.status === "error") return [input, { label: `${part.tool} error`, text: part.state.error }]
    return [input]
  })
}

function render(message: SessionV1.WithParts) {
  return [
    `[${message.info.id}] ${message.info.role}`,
    ...entries(message).map((entry) => `--- ${entry.label}\n${entry.text}`),
  ].join("\n")
}
