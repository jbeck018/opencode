import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import path from "path"
import { Context, Effect, Layer, Schema } from "effect"
import { InstanceState } from "@/effect/instance-state"
import { RuntimeFlags } from "@/effect/runtime-flags"
import type { Agent } from "@/agent/agent"
import type { SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import PROMPT from "./prompt.txt"

export const MAX_LINES = 200
export const MAX_BYTES = 25 * 1024
// Warn the model once the index passes this share of either limit.
const NEAR = 0.8

export const Type = Schema.Literals(["user", "feedback", "project", "reference"])
export type Type = Schema.Schema.Type<typeof Type>

export class InvalidNameError extends Schema.TaggedError<InvalidNameError>()("Memory.InvalidNameError", {
  name: Schema.String,
}) {
  override get message() {
    return `Invalid memory name "${this.name}": use lowercase letters, digits, "-", "_" or "." (no slashes, no "..", not "MEMORY")`
  }
}

export class NotFoundError extends Schema.TaggedError<NotFoundError>()("Memory.NotFoundError", {
  name: Schema.String,
  available: Schema.Array(Schema.String),
}) {
  override get message() {
    return `No memory named "${this.name}". Existing memories: ${this.available.join(", ") || "(none)"}`
  }
}

export class UnavailableError extends Schema.TaggedError<UnavailableError>()("Memory.UnavailableError", {}) {
  override get message() {
    return "Memory requires a git project: this folder is not inside a git repository, so there is no stable project to attach memory to."
  }
}

export class ContentRequiredError extends Schema.TaggedError<ContentRequiredError>()("Memory.ContentRequiredError", {
  name: Schema.String,
}) {
  override get message() {
    return `Memory "${this.name}" does not exist yet, so content is required to create it.`
  }
}

export type Usage = { lines: number; bytes: number; over: boolean; near: boolean }

export type Saved = { file: string; usage: Usage; warning?: string }

export interface Input {
  name: string
  type: Type
  description: string
  /** Optional when the memory already exists: the current body is kept. */
  content?: string
}

export interface Interface {
  readonly dir: () => Effect.Effect<string>
  /** First MEMORY.md lines/bytes that fit the load limits, or undefined when there is no index. */
  readonly index: () => Effect.Effect<{ content: string; truncated: boolean } | undefined>
  /** System prompt section for a primary agent; snapshotted on first use per session so the prompt stays cache-stable. */
  readonly system: (agent: Agent.Info, sessionID: SessionID) => Effect.Effect<string | undefined>
  /** Full index with no name, otherwise one topic file. */
  readonly view: (name?: string) => Effect.Effect<string, Failure>
  readonly save: (input: Input) => Effect.Effect<Saved, Failure>
  readonly remove: (name: string) => Effect.Effect<Saved, Failure>
}

export type Failure = InvalidNameError | NotFoundError | UnavailableError | ContentRequiredError

export class Service extends Context.Service<Service, Interface>()("@opencode/Memory") {}

// The limits count lines and bytes of the index without the newline after the last line.
function split(content: string) {
  const lines = content.split(/\r?\n/)
  return lines.at(-1) === "" ? lines.slice(0, -1) : lines
}

export function usage(content: string): Usage {
  const lines = split(content)
  const bytes = Buffer.byteLength(lines.join("\n"))
  return {
    lines: lines.length,
    bytes,
    over: lines.length > MAX_LINES || bytes > MAX_BYTES,
    near: lines.length >= MAX_LINES * NEAR || bytes >= MAX_BYTES * NEAR,
  }
}

/** Keeps whole lines until either limit would be exceeded. */
export function truncate(content: string) {
  const all = split(content)
  const kept = all.slice(0, MAX_LINES).reduce(
    (acc, line) => {
      const bytes = acc.bytes + (acc.lines.length ? 1 : 0) + Buffer.byteLength(line)
      if (acc.done || bytes > MAX_BYTES) return { ...acc, done: true }
      return { lines: [...acc.lines, line], bytes, done: false }
    },
    { lines: [] as string[], bytes: 0, done: false },
  ).lines
  return { content: kept.join("\n"), truncated: kept.length < all.length }
}

const SNAPSHOT_CAP = 200

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const flags = yield* RuntimeFlags.Service
    const global = yield* Global.Service
    const fs = yield* FSUtil.Service
    const flock = yield* EffectFlock.Service
    const events = yield* EventV2Bridge.Service

    // Every worktree and subdirectory of a repo shares one project ID, hence one memory directory.
    const dir = Effect.fn("Memory.dir")(function* () {
      const ctx = yield* InstanceState.context
      return path.join(global.data, "memory", ctx.project.id)
    })

    // Non-git folders, and git repos with no commits yet, all resolve to the shared "global" project,
    // so memory is off there.
    const requireGit = Effect.fnUntraced(function* () {
      const ctx = yield* InstanceState.context
      if (!available(ctx.project)) return yield* new UnavailableError()
    })

    const snapshots = yield* InstanceState.make(
      Effect.fn("Memory.state")(function* (ctx) {
        const map = new Map<string, string>()
        const unsubscribe = yield* events.listen((event) => {
          if (event.type !== Session.Event.Deleted.type || event.location?.directory !== ctx.directory)
            return Effect.void
          map.delete((event.data as { sessionID: string }).sessionID)
          return Effect.void
        })
        yield* Effect.addFinalizer(() => unsubscribe)
        return map
      }),
    )

    const read = Effect.fnUntraced(function* (file: string) {
      return yield* fs.readFileStringSafe(file).pipe(Effect.orDie)
    })

    // Index and topic files are replaced atomically so unlocked readers never see a partial write.
    const put = Effect.fnUntraced(function* (file: string, content: string) {
      const tmp = path.join(path.dirname(file), `.${path.basename(file)}.${process.pid}.tmp`)
      yield* fs.writeWithDirs(tmp, content).pipe(Effect.orDie)
      yield* fs.rename(tmp, file).pipe(Effect.orDie)
    })

    const topics = Effect.fnUntraced(function* (root: string) {
      const entries = yield* fs.readDirectoryEntries(root).pipe(Effect.orElseSucceed(() => []))
      return entries
        .filter((item) => item.type === "file" && item.name.endsWith(".md") && item.name !== "MEMORY.md")
        .map((item) => item.name.slice(0, -3))
        .toSorted()
    })

    const index: Interface["index"] = Effect.fn("Memory.index")(function* () {
      const root = yield* dir()
      const content = yield* read(path.join(root, "MEMORY.md"))
      if (!content?.trim()) return
      return truncate(content)
    })

    // The whole read-modify-write runs under a cross-process lock: several TUIs can share one server and one project.
    const locked = Effect.fnUntraced(function* <A, E>(root: string, body: Effect.Effect<A, E>) {
      return yield* flock
        .withLock(body, `memory:${root}`)
        .pipe(
          Effect.catchIf(
            (error): error is EffectFlock.LockError =>
              error instanceof EffectFlock.LockTimeoutError || error instanceof EffectFlock.LockCompromisedError,
            Effect.die,
          ),
        )
    })

    // Hand-edited indexes keep their blank lines, other lines and line-ending style.
    const readIndex = Effect.fnUntraced(function* (root: string) {
      const raw = (yield* read(path.join(root, "MEMORY.md"))) ?? ""
      return { lines: split(raw), eol: raw.includes("\r\n") ? "\r\n" : "\n" }
    })

    const writeIndex = Effect.fnUntraced(function* (root: string, lines: string[], eol: string) {
      const content = lines.length ? lines.join(eol) + eol : ""
      yield* put(path.join(root, "MEMORY.md"), content)
      return content
    })

    const system: Interface["system"] = Effect.fn("Memory.system")(function* (agent, sessionID) {
      if (!flags.experimentalMemory || agent.mode === "subagent") return
      if (!available((yield* InstanceState.context).project)) return
      const state = yield* InstanceState.get(snapshots)
      const cached = state.get(sessionID)
      if (cached) return cached
      const root = yield* dir()
      const loaded = yield* index()
      const section = [
        PROMPT.replace("${dir}", () => root)
          .replace("${lines}", String(MAX_LINES))
          .replace("${kb}", String(MAX_BYTES / 1024))
          .trimEnd(),
        "",
        "## MEMORY.md",
        "",
        loaded
          ? loaded.content +
            (loaded.truncated
              ? `\n\n[Truncated: only the first ${MAX_LINES} lines / ${MAX_BYTES / 1024} KB of MEMORY.md are loaded. Use the memory tool to view the full index, then shorten it.]`
              : "")
          : "Your memory is currently empty. Save memories here as you learn things worth keeping.",
      ].join("\n")
      // Bounded: evict the oldest snapshot so a long-running shared server cannot grow without limit.
      if (state.size >= SNAPSHOT_CAP) state.delete(state.keys().next().value!)
      state.set(sessionID, section)
      return section
    })

    const view: Interface["view"] = Effect.fn("Memory.view")(function* (name) {
      yield* requireGit()
      const root = yield* dir()
      if (name === undefined) {
        const content = yield* read(path.join(root, "MEMORY.md"))
        return content?.trim() ? content : `Memory is empty (directory: ${root})`
      }
      const file = filename(name)
      if (!file) return yield* new InvalidNameError({ name })
      const content = yield* read(path.join(root, file))
      if (content === undefined) return yield* new NotFoundError({ name, available: yield* topics(root) })
      return content
    })

    const save: Interface["save"] = Effect.fn("Memory.save")(function* (input) {
      yield* requireGit()
      const file = filename(input.name)
      if (!file) return yield* new InvalidNameError({ name: input.name })
      const root = yield* dir()
      const line = `- [${input.name}](${file}) — ${oneLine(input.description)}`
      return yield* locked(
        root,
        Effect.gen(function* () {
          const body = input.content ?? existingBody(yield* read(path.join(root, file)))
          if (body === undefined) return yield* new ContentRequiredError({ name: input.name })
          yield* put(path.join(root, file), topic(input, body))
          const current = yield* readIndex(root)
          const entry = entryPattern(file)
          const first = current.lines.findIndex((item) => entry.test(item))
          const next =
            first === -1
              ? [...current.lines, line]
              : current.lines.flatMap((item, i) => (i === first ? [line] : entry.test(item) ? [] : [item]))
          return saved(path.join(root, file), yield* writeIndex(root, next, current.eol))
        }),
      )
    })

    const remove: Interface["remove"] = Effect.fn("Memory.remove")(function* (name) {
      yield* requireGit()
      const file = filename(name)
      if (!file) return yield* new InvalidNameError({ name })
      const root = yield* dir()
      const result = yield* locked(
        root,
        Effect.gen(function* () {
          const target = path.join(root, file)
          const current = yield* readIndex(root)
          const entry = entryPattern(file)
          const kept = current.lines.filter((item) => !entry.test(item))
          const existed = yield* fs.existsSafe(target)
          if (!existed && kept.length === current.lines.length) return undefined
          yield* fs.remove(target, { force: true }).pipe(Effect.orDie)
          return saved(target, yield* writeIndex(root, kept, current.eol))
        }),
      )
      if (!result) return yield* new NotFoundError({ name, available: yield* topics(root) })
      return result
    })

    return Service.of({ dir, index, system, view, save, remove })
  }),
)

function saved(file: string, content: string): Saved {
  const measured = usage(content)
  const size = `${measured.lines} lines / ${(measured.bytes / 1024).toFixed(1)} KB`
  const limit = `${MAX_LINES} line / ${MAX_BYTES / 1024} KB`
  const fix =
    "re-save entries with shorter one-line descriptions (omit content to keep the body), merge related memories into one and delete the rest, or delete stale memories"
  return {
    file,
    usage: measured,
    warning: measured.over
      ? `Error: MEMORY.md is ${size}, over the ${limit} load limit. The write succeeded, but content past the limit is dropped the next time memory is loaded. Fix it now: ${fix}. The user can also edit MEMORY.md by hand.`
      : measured.near
        ? `Warning: MEMORY.md is ${size}, near the ${limit} load limit. Shorten it: ${fix}.`
        : undefined,
  }
}

// Frontmatter is stripped so a save without content keeps the existing body.
function existingBody(existing: string | undefined) {
  return existing?.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n?/, "").trim()
}

// Anchored at the line start so a line that merely links to another memory is never matched.
function entryPattern(file: string) {
  return new RegExp(`^- \\[[^\\]]*\\]\\(${file.replaceAll(".", "\\.")}\\)`)
}

// Reject instead of rewrite so the model never silently writes somewhere other than it asked for.
function filename(name: string) {
  const base = name.endsWith(".md") ? name.slice(0, -3) : name
  if (!/^[a-z0-9][a-z0-9_.-]*$/.test(base) || base.includes("..") || base === "memory") return
  return `${base}.md`
}

function oneLine(value: string) {
  return value.replace(/\s+/g, " ").trim()
}

function available(project: { id: string; vcs?: string }) {
  return project.vcs === "git" && project.id !== "global"
}

function topic(input: Input, body: string) {
  return [
    "---",
    `name: ${yaml(input.name)}`,
    `description: ${yaml(oneLine(input.description))}`,
    `type: ${input.type}`,
    "---",
    "",
    body.trim(),
    "",
  ].join("\n")
}

function yaml(value: string) {
  return /^[\s\-?:,\[\]{}#&*!|>'"%@`]|[:#]\s|\s$/.test(value) ? JSON.stringify(value) : value
}

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [RuntimeFlags.node, Global.node, FSUtil.node, EffectFlock.node, EventV2Bridge.node],
})

export * as Memory from "."
