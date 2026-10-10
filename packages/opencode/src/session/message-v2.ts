import { SessionID, MessageID } from "./schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { ProviderV2 } from "@opencode-ai/core/provider"
import {
  APIError,
  AbortedError,
  Assistant,
  AuthError,
  CompactionPart,
  ContextOverflowError,
  Info,
  OutputLengthError,
  Part,
  SubtaskPart,
  User,
  WithParts,
} from "@opencode-ai/core/v1/session"

import { NamedError } from "@opencode-ai/core/util/error"
import { APICallError, convertToModelMessages, LoadAPIKeyError, type ModelMessage, type UIMessage } from "ai"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { NotFoundError } from "@/storage/storage"
import { and } from "drizzle-orm"
import { desc } from "drizzle-orm"
import { eq } from "drizzle-orm"
import { gt } from "drizzle-orm"
import { inArray } from "drizzle-orm"
import { lt } from "drizzle-orm"
import { or } from "drizzle-orm"
import { sql } from "drizzle-orm"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { EventV2 } from "@opencode-ai/core/event"
import { ProviderError } from "@/provider/error"
import { iife } from "@/util/iife"
import { errorMessage } from "@/util/error"
import { isMedia } from "@/util/media"
import type { SystemError } from "bun"
import type { Provider } from "@/provider/provider"
import { Effect, Schema } from "effect"

/** Error shape thrown by Bun's fetch() when gzip/br decompression fails mid-stream */
interface FetchDecompressionError extends Error {
  code: "ZlibError"
  errno: number
  path: string
}

export const SYNTHETIC_ATTACHMENT_PROMPT = "Attached media from tool result:"
export { isMedia }

function truncateToolOutput(text: string, maxChars?: number) {
  if (!maxChars || text.length <= maxChars) return text
  const omitted = text.length - maxChars
  return `${text.slice(0, maxChars)}\n[Tool output truncated for compaction: omitted ${omitted} chars]`
}

export const Event = {
  Updated: SessionV1.Event.MessageUpdated,
  Removed: SessionV1.Event.MessageRemoved,
  PartUpdated: SessionV1.Event.PartUpdated,
  PartDelta: SessionV1.Event.PartDelta,
  PartRemoved: SessionV1.Event.PartRemoved,
}

const Cursor = Schema.Struct({
  id: MessageID,
  time: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
})
type Cursor = typeof Cursor.Type

const decodeCursor = Schema.decodeUnknownSync(Cursor)

export const cursor = {
  encode(input: Cursor) {
    return Buffer.from(JSON.stringify(input)).toString("base64url")
  },
  decode(input: string) {
    return decodeCursor(JSON.parse(Buffer.from(input, "base64url").toString("utf8")))
  },
}

const info = (row: typeof MessageTable.$inferSelect) =>
  ({
    ...row.data,
    id: row.id,
    sessionID: row.session_id,
  }) as Info

const part = (row: typeof PartTable.$inferSelect) =>
  ({
    ...row.data,
    id: row.id,
    sessionID: row.session_id,
    messageID: row.message_id,
  }) as Part

// The prompt loop reloads history on every model call; build these statements once per database.
const prepared = new WeakMap<Database.Interface["db"], ReturnType<typeof prepare>>()

function statements(db: Database.Interface["db"]) {
  const existing = prepared.get(db)
  if (existing) return existing
  const created = prepare(db)
  prepared.set(db, created)
  return created
}

function prepare(db: Database.Interface["db"]) {
  const newestFirst = [desc(MessageTable.time_created), desc(MessageTable.id)] as const
  return {
    latest: db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.session_id, sql.placeholder("sessionID")))
      .orderBy(...newestFirst)
      .limit(sql.placeholder("limit"))
      .prepare(),
    older: db
      .select()
      .from(MessageTable)
      .where(
        and(
          eq(MessageTable.session_id, sql.placeholder("sessionID")),
          or(
            lt(MessageTable.time_created, sql.placeholder("time")),
            and(eq(MessageTable.time_created, sql.placeholder("time")), lt(MessageTable.id, sql.placeholder("id"))),
          ),
        ),
      )
      .orderBy(...newestFirst)
      .limit(sql.placeholder("limit"))
      .prepare(),
    parts: db
      .select()
      .from(PartTable)
      .where(eq(PartTable.message_id, sql.placeholder("messageID")))
      .orderBy(PartTable.id)
      .prepare(),
    message: db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.id, sql.placeholder("id")))
      .prepare(),
    part: db
      .select()
      .from(PartTable)
      .where(eq(PartTable.id, sql.placeholder("id")))
      .prepare(),
    sequence: db
      .select({ seq: EventSequenceTable.seq })
      .from(EventSequenceTable)
      .where(eq(EventSequenceTable.aggregate_id, sql.placeholder("sessionID")))
      .prepare(),
    // Only the ids an event touched: decoding whole payloads (tool outputs) would cost as much as reloading.
    // Snapshot events carry their message or part id as the snapshot key; older rows need the payload.
    changes: db
      .select({
        seq: EventTable.seq,
        type: EventTable.type,
        snapshotKey: EventTable.snapshot_key,
        messageID: sql<
          string | null
        >`CASE WHEN ${EventTable.snapshot_key} IS NULL THEN json_extract(${EventTable.data}, '$.info.id') END`,
        partID: sql<
          string | null
        >`CASE WHEN ${EventTable.snapshot_key} IS NULL THEN json_extract(${EventTable.data}, '$.part.id') END`,
      })
      .from(EventTable)
      .where(
        and(eq(EventTable.aggregate_id, sql.placeholder("sessionID")), gt(EventTable.seq, sql.placeholder("after"))),
      )
      .orderBy(EventTable.seq)
      .prepare(),
  }
}

function hydrate(db: Database.Interface["db"], rows: (typeof MessageTable.$inferSelect)[]) {
  const ids = rows.map((row) => row.id)
  const partByMessage = new Map<string, Part[]>()
  return Effect.gen(function* () {
    if (ids.length > 0) {
      const partRows = yield* db
        .select()
        .from(PartTable)
        .where(inArray(PartTable.message_id, ids))
        .orderBy(PartTable.message_id, PartTable.id)
        .all()
        .pipe(Effect.orDie)
      for (const row of partRows) {
        const next = part(row)
        const list = partByMessage.get(row.message_id)
        if (list) list.push(next)
        else partByMessage.set(row.message_id, [next])
      }
    }

    return rows.map((row) => ({
      info: info(row),
      parts: partByMessage.get(row.id) ?? [],
    }))
  })
}

function providerMeta(metadata: Record<string, any> | undefined) {
  if (!metadata) return undefined
  const { providerExecuted: _, ...rest } = metadata
  return Object.keys(rest).length > 0 ? rest : undefined
}

export const toModelMessagesEffect = Effect.fnUntraced(function* (
  input: WithParts[],
  model: Provider.Model,
  options?: { stripMedia?: boolean; toolOutputMaxChars?: number },
) {
  const result: UIMessage[] = []
  const toolNames = new Set<string>()
  // Track media from tool results that need to be injected as user messages
  // for providers that don't support that media type in tool results.
  //
  // OpenAI-compatible APIs only support string content in tool results, so we need
  // to extract media and inject as user messages. Some SDKs only support a subset
  // of media in tool results; e.g. Bedrock supports images but not PDFs there.
  //
  // Only apply this workaround if the model actually supports that media input -
  // otherwise unsupportedParts() will turn it into a user-visible error.
  const supportsMediaInToolResult = (attachment: { mime: string }) => {
    if (model.api.npm === "@ai-sdk/anthropic") return true
    if (model.api.npm === "@ai-sdk/openai") return true
    if (model.api.npm === "@ai-sdk/amazon-bedrock/mantle") return true
    if (model.api.npm === "@ai-sdk/amazon-bedrock") {
      if (!attachment.mime.startsWith("image/")) return false
      const id = model.api.id.toLowerCase()
      return id.includes("anthropic.") || id.includes("nova") || id.includes("llama4") || id.includes("llama-4")
    }
    if (model.api.npm === "@ai-sdk/xai") return attachment.mime.startsWith("image/")
    if (model.api.npm === "@ai-sdk/google-vertex/anthropic") return true
    if (model.api.npm === "@ai-sdk/google") {
      const id = model.api.id.toLowerCase()
      return id.includes("gemini-3") && !id.includes("gemini-2")
    }
    return false
  }

  const toModelOutput = (options: { toolCallId: string; input: unknown; output: unknown }) => {
    const output = options.output
    if (typeof output === "string") {
      return { type: "text", value: output }
    }

    if (typeof output === "object") {
      const outputObject = output as {
        text: string
        attachments?: Array<{ mime: string; url: string }>
      }
      const attachments = (outputObject.attachments ?? []).filter((attachment) => {
        return attachment.url.startsWith("data:") && attachment.url.includes(",")
      })

      return {
        type: "content",
        value: [
          ...(outputObject.text ? [{ type: "text", text: outputObject.text }] : []),
          ...attachments.map((attachment) => ({
            type: "media",
            mediaType: attachment.mime,
            data: iife(() => {
              const commaIndex = attachment.url.indexOf(",")
              return commaIndex === -1 ? attachment.url : attachment.url.slice(commaIndex + 1)
            }),
          })),
        ],
      }
    }

    return { type: "json", value: output as never }
  }

  for (const msg of input) {
    if (msg.parts.length === 0) continue

    if (msg.info.role === "user") {
      const userMessage: UIMessage = {
        id: msg.info.id,
        role: "user",
        parts: [],
      }
      for (const part of msg.parts) {
        // User message parts should never be empty
        if (part.type === "text" && !part.ignored && part.text !== "")
          userMessage.parts.push({
            type: "text",
            text: part.text,
          })
        // text/plain and directory files are converted into text parts, ignore them
        if (part.type === "file" && part.mime !== "text/plain" && part.mime !== "application/x-directory") {
          if (options?.stripMedia && isMedia(part.mime)) {
            userMessage.parts.push({
              type: "text",
              text: `[Attached ${part.mime}: ${part.filename ?? "file"}]`,
            })
          } else {
            userMessage.parts.push({
              type: "file",
              url: part.url,
              mediaType: part.mime,
              filename: part.filename,
            })
          }
        }

        if (part.type === "compaction") {
          userMessage.parts.push({
            type: "text",
            text: "What did we do so far?",
          })
        }
        if (part.type === "subtask") {
          userMessage.parts.push({
            type: "text",
            text: "The following tool was executed by the user",
          })
        }
      }
      if (userMessage.parts.length > 0) result.push(userMessage)
    }

    if (msg.info.role === "assistant") {
      const differentModel = `${model.providerID}/${model.id}` !== `${msg.info.providerID}/${msg.info.modelID}`
      const media: Array<{ mime: string; url: string; filename?: string }> = []

      if (
        msg.info.error &&
        !(
          AbortedError.isInstance(msg.info.error) &&
          msg.parts.some((part) => part.type !== "step-start" && part.type !== "reasoning")
        )
      ) {
        continue
      }
      const assistantMessage: UIMessage = {
        id: msg.info.id,
        role: "assistant",
        parts: [],
      }
      // Anthropic adaptive thinking can persist assistant turns like:
      // step-start, reasoning(signature), text(""), step-start,
      // reasoning(signature). The empty text part is a structural separator,
      // but it does not carry the signature metadata itself. Dropping it shifts
      // signed thinking positions after step-start splitting/provider regrouping;
      // keeping it as "" is filtered by the AI SDK and rejected by Anthropic.
      // It is unclear whether this shape originates in our stream processing,
      // a proxy, or a lower-level library, but preserving a non-empty separator
      // here is the only safe replay point we have.
      // Use a single space so the separator survives replay without changing
      // the neighboring signed reasoning blocks.
      const hasSignedReasoning = msg.parts.some((part) => {
        if (part.type !== "reasoning") return false
        return part.metadata?.anthropic?.signature != null
      })
      for (const part of msg.parts) {
        if (part.type === "text") {
          const text = part.text === "" && hasSignedReasoning ? " " : part.text
          assistantMessage.parts.push({
            type: "text",
            text,
            ...(differentModel ? {} : { providerMetadata: part.metadata }),
          })
        }
        if (part.type === "step-start")
          assistantMessage.parts.push({
            type: "step-start",
          })
        if (part.type === "tool") {
          toolNames.add(part.tool)
          if (part.state.status === "completed") {
            const outputText = part.state.time.compacted
              ? "[Old tool result content cleared]"
              : truncateToolOutput(part.state.output, options?.toolOutputMaxChars)
            const attachments = part.state.time.compacted || options?.stripMedia ? [] : (part.state.attachments ?? [])

            // For providers that don't support media in tool results, extract media files
            // (images, PDFs) to be sent as a separate user message
            const mediaAttachments = attachments.filter((a) => isMedia(a.mime))
            const extractedMedia = mediaAttachments.filter((a) => !supportsMediaInToolResult(a))
            if (extractedMedia.length > 0) {
              media.push(...extractedMedia)
            }
            const finalAttachments = attachments.filter((a) => !isMedia(a.mime) || supportsMediaInToolResult(a))

            const output =
              finalAttachments.length > 0
                ? {
                    text: outputText,
                    attachments: finalAttachments,
                  }
                : outputText

            assistantMessage.parts.push({
              type: ("tool-" + part.tool) as `tool-${string}`,
              state: "output-available",
              toolCallId: part.callID,
              input: part.state.input,
              output,
              ...(part.metadata?.providerExecuted ? { providerExecuted: true } : {}),
              ...(differentModel ? {} : { callProviderMetadata: providerMeta(part.metadata) }),
            })
          }
          if (part.state.status === "error") {
            const output = part.state.metadata?.interrupted === true ? part.state.metadata.output : undefined
            if (typeof output === "string") {
              assistantMessage.parts.push({
                type: ("tool-" + part.tool) as `tool-${string}`,
                state: "output-available",
                toolCallId: part.callID,
                input: part.state.input,
                output,
                ...(part.metadata?.providerExecuted ? { providerExecuted: true } : {}),
                ...(differentModel ? {} : { callProviderMetadata: providerMeta(part.metadata) }),
              })
            } else {
              assistantMessage.parts.push({
                type: ("tool-" + part.tool) as `tool-${string}`,
                state: "output-error",
                toolCallId: part.callID,
                input: part.state.input,
                errorText: part.state.error,
                ...(part.metadata?.providerExecuted ? { providerExecuted: true } : {}),
                ...(differentModel ? {} : { callProviderMetadata: providerMeta(part.metadata) }),
              })
            }
          }
          // Handle pending/running tool calls to prevent dangling tool_use blocks
          // Anthropic/Claude APIs require every tool_use to have a corresponding tool_result
          if (part.state.status === "pending" || part.state.status === "running")
            assistantMessage.parts.push({
              type: ("tool-" + part.tool) as `tool-${string}`,
              state: "output-error",
              toolCallId: part.callID,
              input: part.state.input,
              errorText: "[Tool execution was interrupted]",
              ...(part.metadata?.providerExecuted ? { providerExecuted: true } : {}),
              ...(differentModel ? {} : { callProviderMetadata: providerMeta(part.metadata) }),
            })
        }
        if (part.type === "reasoning") {
          if (differentModel) {
            if (part.text.trim().length > 0)
              assistantMessage.parts.push({
                type: "text",
                text: part.text,
              })
            continue
          }
          assistantMessage.parts.push({
            type: "reasoning",
            text: part.text,
            providerMetadata: part.metadata,
          })
        }
      }
      if (assistantMessage.parts.length > 0) {
        result.push(assistantMessage)
        // Inject pending media as a user message for providers that don't support
        // media (images, PDFs) in tool results
        if (media.length > 0) {
          result.push({
            id: MessageID.ascending(),
            role: "user",
            parts: [
              {
                type: "text" as const,
                text: SYNTHETIC_ATTACHMENT_PROMPT,
              },
              ...media.map((attachment) => ({
                type: "file" as const,
                url: attachment.url,
                mediaType: attachment.mime,
                filename: attachment.filename,
              })),
            ],
          })
        }
      }
    }
  }

  const tools = Object.fromEntries(Array.from(toolNames).map((toolName) => [toolName, { toModelOutput }]))

  return yield* Effect.promise(() =>
    convertToModelMessages(
      result.filter((msg) => msg.parts.some((part) => part.type !== "step-start")),
      {
        //@ts-expect-error (convertToModelMessages expects a ToolSet but only actually needs tools[name]?.toModelOutput)
        tools,
      },
    ),
  )
})

export function toModelMessages(
  input: WithParts[],
  model: Provider.Model,
  options?: { stripMedia?: boolean; toolOutputMaxChars?: number },
): Promise<ModelMessage[]> {
  return Effect.runPromise(toModelMessagesEffect(input, model, options))
}

export const page = Effect.fn("MessageV2.page")(function* (input: {
  sessionID: SessionID
  limit: number
  before?: string
}) {
  const { db } = yield* Database.Service
  const before = input.before ? cursor.decode(input.before) : undefined
  const rows = yield* (
    before
      ? statements(db).older.all({
          sessionID: input.sessionID,
          time: before.time,
          id: before.id,
          limit: input.limit + 1,
        })
      : statements(db).latest.all({ sessionID: input.sessionID, limit: input.limit + 1 })
  ).pipe(Effect.orDie)
  if (rows.length === 0) {
    const row = yield* db
      .select({ id: SessionTable.id })
      .from(SessionTable)
      .where(eq(SessionTable.id, input.sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!row) return yield* new NotFoundError({ message: `Session not found: ${input.sessionID}` })
    return {
      items: [] as WithParts[],
      more: false,
    }
  }

  const more = rows.length > input.limit
  const slice = more ? rows.slice(0, input.limit) : rows
  const items = yield* hydrate(db, slice)
  items.reverse()
  const tail = slice.at(-1)
  return {
    items,
    more,
    cursor: more && tail ? cursor.encode({ id: tail.id, time: tail.time_created }) : undefined,
  }
})

export function stream(sessionID: SessionID) {
  const size = 50
  return Effect.gen(function* () {
    const result = [] as WithParts[]
    let before: string | undefined
    while (true) {
      const next = yield* page({ sessionID, limit: size, before }).pipe(
        Effect.catchIf(NotFoundError.isInstance, () =>
          Effect.succeed({ items: [] as WithParts[], more: false, cursor: undefined }),
        ),
      )
      if (next.items.length === 0) break
      for (let i = next.items.length - 1; i >= 0; i--) {
        const item = next.items[i]
        if (item) result.push(item)
      }
      if (!next.more || !next.cursor) break
      before = next.cursor
    }
    return result
  })
}

export function parts(messageID: MessageID) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    const rows = yield* statements(db).parts.all({ messageID }).pipe(Effect.orDie)
    return rows.map(part)
  })
}

export const get = Effect.fn("MessageV2.get")(function* (input: { sessionID: SessionID; messageID: MessageID }) {
  const { db } = yield* Database.Service
  const row = yield* db
    .select()
    .from(MessageTable)
    .where(and(eq(MessageTable.id, input.messageID), eq(MessageTable.session_id, input.sessionID)))
    .get()
    .pipe(Effect.orDie)
  if (!row) return yield* new NotFoundError({ message: `Message not found: ${input.messageID}` })
  return {
    info: info(row),
    parts: yield* parts(input.messageID),
  }
})

export function filterCompacted(msgs: Iterable<WithParts>) {
  const result = [] as WithParts[]
  const done = compactionBoundary()
  for (const msg of msgs) {
    result.push(msg)
    if (done(msg)) break
  }
  return arrangeCompacted(result)
}

/**
 * Walks messages newest-first and reports, after each one, whether the history before it is
 * covered by a completed compaction and need not be read.
 */
function compactionBoundary() {
  const completed = new Set<string>()
  let retain: MessageID | undefined
  return (msg: WithParts) => {
    if (retain) return msg.info.id === retain
    if (msg.info.role === "user" && completed.has(msg.info.id)) {
      const part = msg.parts.find((item): item is CompactionPart => item.type === "compaction")
      if (!part) return false
      if (!part.tail_start_id) return true
      retain = part.tail_start_id
      return msg.info.id === retain
    }
    if (msg.info.role === "assistant" && msg.info.summary && msg.info.finish && !msg.info.error)
      completed.add(msg.info.parentID)
    return false
  }
}

function arrangeCompacted(result: WithParts[]) {
  result.reverse()
  const compactionIndex = result.findLastIndex(
    (msg) =>
      msg.info.role === "user" &&
      msg.parts.some((item): item is CompactionPart => item.type === "compaction" && item.tail_start_id !== undefined),
  )
  const compaction = result[compactionIndex]
  const part = compaction?.parts.find(
    (item): item is CompactionPart => item.type === "compaction" && item.tail_start_id !== undefined,
  )
  const summaryIndex = compaction
    ? result.findIndex(
        (msg, index) =>
          index > compactionIndex &&
          msg.info.role === "assistant" &&
          msg.info.summary &&
          msg.info.parentID === compaction.info.id,
      )
    : -1
  const tailIndex = part?.tail_start_id ? result.findIndex((msg) => msg.info.id === part.tail_start_id) : -1
  if (tailIndex >= 0 && tailIndex < compactionIndex && summaryIndex > compactionIndex) {
    return [
      ...result.slice(compactionIndex, summaryIndex + 1),
      ...result.slice(tailIndex, compactionIndex),
      ...result.slice(summaryIndex + 1),
    ]
  }
  return result
}

// Every model call reloads the history. It is cached per session, stamped with the session's durable
// event sequence: every write to a session's messages and parts goes through a sequenced event, so
// the events after the stamp name exactly the rows to re-read. Anything else (removals, messages
// older than the cached window) reloads from the database. The reload pages newest-first
// and stops at the compaction boundary instead of loading what a completed compaction replaced.
export const filterCompactedEffect = Effect.fnUntraced(function* (sessionID: SessionID) {
  const { db } = yield* Database.Service
  const sessions = histories.get(db) ?? new Map<SessionID, CachedHistory>()
  histories.set(db, sessions)
  const cached = sessions.get(sessionID)
  sessions.delete(sessionID)
  const seq = (yield* statements(db).sequence.get({ sessionID }).pipe(Effect.orDie))?.seq ?? -1
  const refreshed = cached && (yield* refresh(db, sessionID, cached, seq))
  const fromCache = refreshed && select(refreshed)
  // Without a usable cache, or when a compaction no longer bounds the cached window (it was
  // reverted), load from the database.
  const current = fromCache ? refreshed : yield* load(sessionID, seq)
  sessions.set(sessionID, current)
  if (sessions.size > MAX_CACHED_SESSIONS) sessions.delete(sessions.keys().next().value!)
  return fromCache || select(current)!
})

/** The model-ordered history from a cached window, or undefined when the window ends before its boundary. */
function select(history: CachedHistory) {
  const done = compactionBoundary()
  const result = [] as WithParts[]
  for (const item of history.messages) {
    // Callers append parts (reminders); copy the arrays so the cache keeps its own.
    result.push({ info: item.info, parts: [...item.parts] })
    if (done(item)) return arrangeCompacted(result)
  }
  return history.complete ? arrangeCompacted(result) : undefined
}

type CachedHistory = {
  seq: number
  // Newest first, down to the last compaction boundary or the start of the session.
  messages: WithParts[]
  complete: boolean
}

const MAX_CACHED_SESSIONS = 8
const histories = new WeakMap<Database.Interface["db"], Map<SessionID, CachedHistory>>()
const changeTypes = {
  message: EventV2.versionedType(SessionV1.Event.MessageUpdated.type, 1),
  part: EventV2.versionedType(SessionV1.Event.PartUpdated.type, 1),
  reload: new Set([
    EventV2.versionedType(SessionV1.Event.MessageRemoved.type, 1),
    EventV2.versionedType(SessionV1.Event.PartRemoved.type, 1),
    EventV2.versionedType(SessionV1.Event.Deleted.type, 1),
  ]),
}

const load = Effect.fnUntraced(function* (sessionID: SessionID, seq: number) {
  const messages = [] as WithParts[]
  const done = compactionBoundary()
  let before: string | undefined
  while (true) {
    const next = yield* page({ sessionID, limit: 50, before }).pipe(
      Effect.catchIf(NotFoundError.isInstance, () =>
        Effect.succeed({ items: [] as WithParts[], more: false, cursor: undefined }),
      ),
    )
    for (let i = next.items.length - 1; i >= 0; i--) {
      const item = next.items[i]
      if (!item) continue
      messages.push(item)
      if (done(item)) return { seq, messages, complete: false } satisfies CachedHistory
    }
    if (next.items.length === 0 || !next.more || !next.cursor)
      return { seq, messages, complete: true } satisfies CachedHistory
    before = next.cursor
  }
})

/** Applies the events after the cached stamp, or returns undefined when only a reload is safe. */
const refresh = Effect.fnUntraced(function* (
  db: Database.Interface["db"],
  sessionID: SessionID,
  cached: CachedHistory,
  seq: number,
) {
  if (seq === cached.seq) return cached
  if (seq < cached.seq) return undefined
  const changes = yield* statements(db).changes.all({ sessionID, after: cached.seq }).pipe(Effect.orDie)
  // Gaps are fine: a snapshot event is only deleted once a newer one for the same message or part
  // exists, and that one is among these changes. Events written since `seq` was read are not.
  if (changes.at(-1)?.seq !== seq) return undefined
  const messageIDs = new Set<string>()
  const partIDs = new Set<string>()
  for (const change of changes) {
    if (changeTypes.reload.has(change.type)) return undefined
    if (change.type === changeTypes.message) {
      const id = change.snapshotKey ?? change.messageID
      if (!id) return undefined
      messageIDs.add(id)
    }
    if (change.type === changeTypes.part) {
      const id = change.snapshotKey ?? change.partID
      if (!id) return undefined
      partIDs.add(id)
    }
  }
  const messages = cached.messages.slice()
  const indexOf = (id: string) => messages.findIndex((item) => item.info.id === id)
  for (const id of messageIDs) {
    const row = yield* statements(db).message.get({ id }).pipe(Effect.orDie)
    if (!row) return undefined
    const index = indexOf(id)
    if (index >= 0) {
      messages[index] = { info: info(row), parts: messages[index]!.parts }
      continue
    }
    // Newest first by (time_created, id), as page() orders them.
    const position = messages.findIndex((item) => newer(row, item.info))
    if (position === -1 && !cached.complete) return undefined
    messages.splice(position === -1 ? messages.length : position, 0, { info: info(row), parts: [] })
  }
  for (const id of partIDs) {
    const row = yield* statements(db).part.get({ id }).pipe(Effect.orDie)
    if (!row) return undefined
    const index = indexOf(row.message_id)
    if (index < 0) return undefined
    const next = part(row)
    const parts = messages[index]!.parts.filter((item) => item.id !== next.id)
    const at = parts.findIndex((item) => item.id > next.id)
    parts.splice(at === -1 ? parts.length : at, 0, next)
    messages[index] = { info: messages[index]!.info, parts }
  }
  return { seq, messages, complete: cached.complete } satisfies CachedHistory
})

function newer(row: typeof MessageTable.$inferSelect, than: Info) {
  if (row.time_created !== than.time.created) return row.time_created > than.time.created
  return row.id > than.id
}

// filterCompacted reorders messages for model consumption
// ([compaction-user, summary, ...retained tail..., continue-user]), so array
// position is not chronological. IDs are only a deterministic tie-breaker
// because imported messages do not necessarily have monotonic IDs.
export function latest(msgs: WithParts[]) {
  let user: User | undefined
  let assistant: Assistant | undefined
  let finished: Assistant | undefined
  for (const msg of msgs) {
    const info = msg.info
    if (info.role === "user" && isAfter(info, user)) user = info
    if (info.role === "assistant" && isAfter(info, assistant)) assistant = info
    if (info.role === "assistant" && info.finish && isAfter(info, finished)) finished = info
  }
  const tasks = msgs.flatMap((m) =>
    finished && !isAfter(m.info, finished)
      ? []
      : m.parts.filter((p): p is CompactionPart | SubtaskPart => p.type === "compaction" || p.type === "subtask"),
  )
  return { user, assistant, finished, tasks }
}

function isAfter(info: Info, other?: Info) {
  if (!other) return true
  if (info.time.created !== other.time.created) return info.time.created > other.time.created
  return info.id > other.id
}

export function fromError(
  e: unknown,
  ctx: { providerID: ProviderV2.ID; aborted?: boolean },
): NonNullable<Assistant["error"]> {
  switch (true) {
    case e instanceof DOMException && e.name === "AbortError":
      return new AbortedError(
        { message: e.message },
        {
          cause: e,
        },
      ).toObject()
    case OutputLengthError.isInstance(e):
      return e
    case LoadAPIKeyError.isInstance(e):
      return new AuthError(
        {
          providerID: ctx.providerID,
          message: e.message,
        },
        { cause: e },
      ).toObject()
    case (e as SystemError)?.code === "ECONNRESET":
      return new APIError(
        {
          message: "Connection reset by server",
          isRetryable: true,
          metadata: {
            code: (e as SystemError).code ?? "",
            syscall: (e as SystemError).syscall ?? "",
            message: (e as SystemError).message ?? "",
          },
        },
        { cause: e },
      ).toObject()
    case e instanceof Error && (e as FetchDecompressionError).code === "ZlibError":
      if (ctx.aborted) {
        return new AbortedError({ message: e.message }, { cause: e }).toObject()
      }
      return new APIError(
        {
          message: "Response decompression failed",
          isRetryable: true,
          metadata: {
            code: (e as FetchDecompressionError).code,
            message: e.message,
          },
        },
        { cause: e },
      ).toObject()
    case e instanceof ProviderError.HeaderTimeoutError:
      return new APIError(
        {
          message: e.message,
          isRetryable: true,
          metadata: {
            code: e.name,
            timeoutMs: String(e.ms),
          },
        },
        { cause: e },
      ).toObject()
    case e instanceof ProviderError.ResponseStreamError:
      return new APIError(
        {
          message: e.message,
          isRetryable: true,
          metadata: {
            code: e.name,
          },
        },
        { cause: e },
      ).toObject()
    case APICallError.isInstance(e):
      const parsed = ProviderError.parseAPICallError({
        providerID: ctx.providerID,
        error: e,
      })
      if (parsed.type === "context_overflow") {
        return new ContextOverflowError(
          {
            message: parsed.message,
            responseBody: parsed.responseBody,
          },
          { cause: e },
        ).toObject()
      }

      return new APIError(
        {
          message: parsed.message,
          statusCode: parsed.statusCode,
          isRetryable: parsed.isRetryable,
          responseHeaders: parsed.responseHeaders,
          responseBody: parsed.responseBody,
          metadata: parsed.metadata,
        },
        { cause: e },
      ).toObject()
    case e instanceof Error:
      return new NamedError.Unknown({ message: errorMessage(e) }, { cause: e }).toObject()
    default:
      try {
        const parsed = ProviderError.parseStreamError(e)
        if (parsed) {
          if (parsed.type === "context_overflow") {
            return new ContextOverflowError(
              {
                message: parsed.message,
                responseBody: parsed.responseBody,
              },
              { cause: e },
            ).toObject()
          }
          return new APIError(
            {
              message: parsed.message,
              isRetryable: parsed.isRetryable,
              responseBody: parsed.responseBody,
            },
            {
              cause: e,
            },
          ).toObject()
        }
      } catch {}
      return new NamedError.Unknown({ message: JSON.stringify(e) }, { cause: e }).toObject()
  }
}

export * as MessageV2 from "./message-v2"
export const node = LayerNode.group([Database.node])
