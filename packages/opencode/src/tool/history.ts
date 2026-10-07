import { Database } from "@opencode-ai/core/database/database"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { and, asc, eq, inArray, or, sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
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
  scope: Schema.optional(Schema.Literals(["session", "project"])).annotate({
    description:
      'Where to search: "session" (default) is this session and the sessions that spawned it; "project" is every session of this project, newest first',
  }),
})

const SNIPPET = 160
const MESSAGE_LIMIT = 50_000
// Parts are scanned newest first in rowid ranges, one statement per range, with the event loop released
// in between, so a search never holds the single SQLite connection for long. Without a literal every
// part in range is decoded, so those ranges are smaller.
const CHUNK = 5_000
const CHUNK_UNFILTERED = 2_000
// Project scope without a usable literal decodes every part it visits, so it stops after this many.
const SCAN_CAP = 20_000
const MAX_LITERALS = 16

type Part = typeof PartTable.$inferSelect
type SessionRowID = typeof SessionTable.$inferSelect.id

// The part data shape this tool reads; stored parts carry more fields.
type Stored = {
  type?: string
  text?: string
  tool?: string
  state?: { status?: string; input?: unknown; output?: string; error?: string }
}

// The transcript the model sees ends at the last compaction and has old tool outputs cleared, but
// every message and output stays in the database. This tool lets the model search and reread that
// full record (including the parent sessions of a subagent, and optionally the whole project)
// instead of relying on the summary.
export const HistoryTool = Tool.define(
  "history",
  Effect.gen(function* () {
    const database = yield* Database.Service
    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const db = database.db
          const project = params.scope === "project"
          const chain = yield* db
            .all<{ id: SessionRowID; project_id: typeof SessionTable.$inferSelect.project_id }>(
              sql`WITH RECURSIVE chain(id, parent_id, project_id) AS (
                SELECT id, parent_id, project_id FROM session WHERE id = ${ctx.sessionID}
                UNION
                SELECT s.id, s.parent_id, s.project_id FROM session s JOIN chain c ON s.id = c.parent_id
              ) SELECT id, project_id FROM chain`,
            )
            .pipe(Effect.orDie)
          if (!chain[0]) throw new Error(`Session not found: ${ctx.sessionID}`)
          const projectID = chain[0].project_id
          const sessions = project
            ? sql`SELECT id FROM session WHERE project_id = ${projectID}`
            : sql.join(
                chain.map((row) => sql`${row.id}`),
                sql`, `,
              )
          // A lineage is a few sessions, so each range is an index range on (session_id, rowid). A project can
          // have many, so the unary plus keeps SQLite from walking the session index once per range there.
          const sessionFilter = project
            ? sql`+${PartTable.session_id} IN (${sessions})`
            : sql`${PartTable.session_id} IN (${sessions})`

          if (params.messageID) {
            const message = yield* db
              .select()
              .from(MessageTable)
              .where(
                and(
                  eq(MessageTable.id, params.messageID as typeof MessageTable.$inferSelect.id),
                  inArray(
                    MessageTable.session_id,
                    project
                      ? db
                          .select({ id: SessionTable.id })
                          .from(SessionTable)
                          .where(eq(SessionTable.project_id, projectID))
                      : chain.map((row) => row.id),
                  ),
                ),
              )
              .get()
              .pipe(Effect.orDie)
            if (!message)
              return {
                title: params.messageID,
                metadata: { matches: 0 },
                output: `No message ${params.messageID} in this ${project ? "project" : "session"}`,
              }
            const parts = yield* db
              .select()
              .from(PartTable)
              .where(eq(PartTable.message_id, message.id))
              .orderBy(asc(PartTable.id))
              .all()
              .pipe(Effect.orDie)
            const body = parts.flatMap((part) =>
              entries(part.data as Stored).map((entry) => `--- ${entry.label}\n${entry.text}`),
            )
            return {
              title: params.messageID,
              metadata: { matches: 1 },
              output: [`[${message.id}] ${role(message.data)}`, ...body].join("\n").slice(0, MESSAGE_LIMIT),
            }
          }

          if (!params.query) throw new Error("Provide a query to search or a messageID to read")
          const pattern = new RegExp(params.query, "i")
          const limit = params.limit ?? 20
          const literals = requiredLiterals(params.query)
          const prefilter = literals
            ? or(...literals.map((literal) => sql`${PartTable.data} LIKE ${like(literal)} ESCAPE '\\'`))
            : undefined
          const cap = project && !literals ? SCAN_CAP : Infinity
          // Tests set a small range to exercise range boundaries.
          const chunk =
            typeof ctx.extra?.historyChunk === "number" && ctx.extra.historyChunk > 0
              ? ctx.extra.historyChunk
              : literals
                ? CHUNK
                : CHUNK_UNFILTERED

          const found = [] as { part: Part; label: string; text: string; match: RegExpExecArray }[]
          // Walk only the rowid span the scope's parts occupy, not the whole shared table (part_session_idx
          // holds the rowid, so this is an index lookup).
          const bounds = (yield* db
            .all<{
              low: number
              high: number
            }>(sql`SELECT coalesce(min(rowid), 0) AS low, coalesce(max(rowid), 0) AS high FROM part WHERE session_id IN (${sessions})`)
            .pipe(Effect.orDie))[0]
          let high = bounds.high
          let scanned = 0
          while (found.length < limit && high >= bounds.low && high > 0 && scanned < cap) {
            const rows = yield* db
              .select()
              .from(PartTable)
              .where(and(sql`"part"."rowid" > ${high - chunk} AND "part"."rowid" <= ${high}`, sessionFilter, prefilter))
              .orderBy(sql`"part"."rowid" DESC`)
              .all()
              .pipe(Effect.orDie)
            for (const part of rows)
              for (const entry of entries(part.data as Stored)) {
                const match = pattern.exec(entry.text)
                if (match && found.length < limit) found.push({ part, label: entry.label, text: entry.text, match })
              }
            scanned += rows.length
            high -= chunk
            // Let timers, I/O and other writers on the shared connection run between ranges.
            if (found.length < limit && high >= bounds.low) yield* Effect.yieldNow
          }
          const incomplete = found.length < limit && high >= bounds.low && high > 0

          // Only hit rows need their message role (and, for project scope, their session title).
          const messages = found.length
            ? yield* db
                .select({ id: MessageTable.id, data: MessageTable.data })
                .from(MessageTable)
                .where(inArray(MessageTable.id, [...new Set(found.map((hit) => hit.part.message_id))]))
                .all()
                .pipe(Effect.orDie)
            : []
          const titles =
            project && found.length
              ? yield* db
                  .select({ id: SessionTable.id, title: SessionTable.title })
                  .from(SessionTable)
                  .where(inArray(SessionTable.id, [...new Set(found.map((hit) => hit.part.session_id))]))
                  .all()
                  .pipe(Effect.orDie)
              : []
          const hits = found.map((hit) => {
            const start = Math.max(0, hit.match.index - SNIPPET)
            const end = Math.min(hit.text.length, hit.match.index + hit.match[0].length + SNIPPET)
            const owner = project
              ? ` [session ${hit.part.session_id} "${titles.find((item) => item.id === hit.part.session_id)?.title ?? ""}"]`
              : ""
            return `[${hit.part.message_id}] ${role(messages.find((item) => item.id === hit.part.message_id)?.data)} ${hit.label}${owner}: ${start > 0 ? "…" : ""}${hit.text.slice(start, end)}${end < hit.text.length ? "…" : ""}`
          })
          return {
            title: params.query,
            metadata: { matches: hits.length },
            output:
              (hits.length ? hits.join("\n\n") : "No matches") +
              (incomplete
                ? `\n\nNote: this query has no literal text to narrow the search, so only the ${SCAN_CAP} newest parts of the project were scanned and results may be incomplete. Add a literal word or phrase of 3+ characters to search all sessions.`
                : ""),
          }
        }),
    } satisfies Tool.DefWithoutID<typeof Parameters>
  }),
)

function role(data: unknown) {
  return (data as { role?: string } | undefined)?.role ?? "unknown"
}

function entries(part: Stored) {
  if (part.type === "text" || part.type === "reasoning") return [{ label: part.type, text: part.text ?? "" }]
  if (part.type !== "tool" || !part.state) return []
  const input = { label: `${part.tool} input`, text: JSON.stringify(part.state.input) }
  if (part.state.status === "completed") return [input, { label: `${part.tool} output`, text: part.state.output ?? "" }]
  if (part.state.status === "error") return [input, { label: `${part.tool} error`, text: part.state.error ?? "" }]
  return [input]
}

function like(literal: string) {
  return `%${literal.replace(/[\\%_]/g, "\\$&")}%`
}

const safe = (char: string) => char >= " " && char <= "~" && char !== '"' && char !== "\\"

/**
 * Literal substrings of which at least one must appear in any text the pattern matches, or undefined
 * when that cannot be established with certainty. Each top-level alternative contributes its longest
 * required run of at least 3 characters that survive JSON encoding unchanged and that SQLite folds
 * case the same way JS does (printable ASCII except `"` and `\`). Anything unfamiliar (lookarounds,
 * backreferences, unicode or hex escapes, unterminated constructs) disables the prefilter.
 */
export function requiredLiterals(source: string) {
  if (/\(\?<?[=!]|\\[1-9k]/.test(source)) return undefined
  const branches = new Set<string>()
  let best = ""
  let run = ""
  // Whether the last character of `run` is a single atom a following quantifier would apply to.
  let atom = false
  const flush = () => {
    if (run.length > best.length) best = run
    run = ""
    atom = false
  }
  for (let i = 0; i < source.length; i++) {
    const char = source[i]
    if (char === "|") {
      flush()
      if (best.length < 3) return undefined
      branches.add(best)
      best = ""
      continue
    }
    if (char === "(" || char === "[") {
      const end = closing(source, i)
      if (end < 0) return undefined
      flush()
      i = end
      continue
    }
    if (char === "\\") {
      const next = source[++i]
      if (next === undefined) return undefined
      if (/[dDwWsSbBtnrfv]/.test(next)) flush()
      else if (/[A-Za-z0-9]/.test(next)) return undefined
      else if (safe(next)) {
        run += next
        atom = true
      } else flush()
      continue
    }
    if (char === "{") {
      // Only a well-formed `{n}`, `{n,}` or `{n,m}` is a quantifier; anything else is a literal brace.
      const quantifier = /^\{\d+(,\d*)?\}/.exec(source.slice(i))
      if (quantifier) {
        if (atom) run = run.slice(0, -1)
        flush()
        i += quantifier[0].length - 1
        continue
      }
    }
    if (char === "*" || char === "?") {
      if (atom) run = run.slice(0, -1)
      flush()
      continue
    }
    if (char === "+" || char === "." || char === "^" || char === "$") {
      flush()
      continue
    }
    if (safe(char)) {
      run += char
      atom = true
      continue
    }
    flush()
  }
  flush()
  if (best.length < 3) return undefined
  branches.add(best)
  return branches.size > MAX_LITERALS ? undefined : [...branches]
}

// Index of the `)` or `]` that closes the group or class opening at `start`, or -1.
function closing(source: string, start: number) {
  if (source[start] === "[") {
    for (let i = start + 1; i < source.length; i++) {
      if (source[i] === "\\") i++
      else if (source[i] === "]") return i
    }
    return -1
  }
  let depth = 0
  for (let i = start; i < source.length; i++) {
    if (source[i] === "\\") i++
    else if (source[i] === "[") {
      i = closing(source, i)
      if (i < 0) return -1
    } else if (source[i] === "(") depth++
    else if (source[i] === ")" && --depth === 0) return i
  }
  return -1
}
