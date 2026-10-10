export * as ToolOutputStore from "./tool-output-store"

import path from "path"
import { Context, Duration, Effect, Layer, Option, Schedule, Schema } from "effect"
import { Config } from "./config"
import { FSUtil } from "./fs-util"
import { Global } from "./global"
import { makeGlobalNode, makeLocationNode } from "./effect/app-node"
import { SessionSchema } from "./session/schema"
import { Identifier } from "./util/identifier"
import type { ToolOutput } from "@opencode-ai/llm"

export const MAX_LINES = 2_000
export const MAX_BYTES = 50 * 1024
export const RETENTION = Duration.days(3)
export const MAX_TOTAL_BYTES = 100 * 1024 * 1024
// A "full output saved to <path>" hint must stay readable, so recent and newest files are never evicted.
export const PROTECTED_AGE = Duration.minutes(30)
export const PROTECTED_COUNT = 20

export const MANAGED_DIRECTORY = "tool-output"

export interface ManagedFile {
  readonly file: string
  readonly size: number
  /** Epoch millis. Files without an mtime are kept and counted as just modified. */
  readonly modified?: number
}

/** Returns the managed files to delete: expired, or over the size cap unless recent or among the newest. */
export function evictable(files: ReadonlyArray<ManagedFile>, now: number) {
  const cutoff = now - Duration.toMillis(RETENTION)
  const protectedAfter = now - Duration.toMillis(PROTECTED_AGE)
  return files
    .map((item) => ({ ...item, modified: item.modified ?? now }))
    .toSorted((a, b) => b.modified - a.modified)
    .reduce(
      (acc, item, index) => {
        const total = acc.total + item.size
        const keep =
          item.modified >= cutoff &&
          (index < PROTECTED_COUNT || item.modified >= protectedAfter || total <= MAX_TOTAL_BYTES)
        return { total, evict: keep ? acc.evict : [...acc.evict, item.file] }
      },
      { total: 0, evict: [] as string[] },
    ).evict
}

export interface BoundInput {
  readonly sessionID: SessionSchema.ID
  readonly toolCallID: string
  readonly output: ToolOutput
}

export interface BoundResult {
  readonly output: ToolOutput
  readonly outputPaths: ReadonlyArray<string>
}

export class StorageError extends Schema.TaggedError<StorageError>()("ToolOutputStore.StorageError", {
  operation: Schema.Literals(["encode", "write"]),
  cause: Schema.Defect(),
}) {
  override get message() {
    const detail = this.cause instanceof Error ? this.cause.message : String(this.cause)
    return `Failed to ${this.operation} tool output${detail ? `: ${detail}` : ""}`
  }
}

export type Error = StorageError

export interface Interface {
  readonly limits: () => Effect.Effect<{ readonly maxLines: number; readonly maxBytes: number }>
  readonly bound: (input: BoundInput) => Effect.Effect<BoundResult, Error>
  readonly cleanup: () => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/ToolOutputStore") {}

const takePrefix = (input: string, maximumBytes: number) => {
  let bytes = 0
  let content = ""
  for (const char of input) {
    const size = Buffer.byteLength(char, "utf-8")
    if (bytes + size > maximumBytes) break
    content += char
    bytes += size
  }
  return content
}

const takeSuffix = (input: string, maximumBytes: number) => {
  let bytes = 0
  const content: string[] = []
  for (const char of Array.from(input).toReversed()) {
    const size = Buffer.byteLength(char, "utf-8")
    if (bytes + size > maximumBytes) break
    content.unshift(char)
    bytes += size
  }
  return content.join("")
}

const preview = (text: string, maxLines: number, maxBytes: number) => {
  const lines = text.split("\n")
  const headLines = Math.ceil(maxLines / 2)
  const tailLines = Math.floor(maxLines / 2)
  const sampled =
    lines.length <= maxLines
      ? text
      : [
          lines.slice(0, headLines).join("\n"),
          ...(tailLines > 0 ? [lines.slice(lines.length - tailLines).join("\n")] : []),
        ].join("\n")
  if (Buffer.byteLength(sampled, "utf-8") <= maxBytes) {
    return lines.length <= maxLines
      ? { head: sampled, tail: "" }
      : {
          head: lines.slice(0, headLines).join("\n"),
          tail: tailLines > 0 ? lines.slice(lines.length - tailLines).join("\n") : "",
        }
  }
  const headBytes = Math.ceil(maxBytes / 2)
  const tailBytes = Math.floor(maxBytes / 2)
  return { head: takePrefix(sampled, headBytes), tail: takeSuffix(sampled, tailBytes) }
}

const boundedPreview = (text: string, marker: string, maxLines: number, maxBytes: number) => {
  const markerOnly = takePrefix(marker, maxBytes).split("\n").slice(0, maxLines).join("\n")
  const markerBytes = Buffer.byteLength(marker, "utf-8")
  if (maxLines <= 4 || maxBytes <= markerBytes + 4) return markerOnly
  const bounded = preview(text, maxLines - 4, maxBytes - markerBytes - 4)
  return bounded.tail ? `${bounded.head}\n\n${marker}\n\n${bounded.tail}` : `${bounded.head}\n\n${marker}`
}

const lineCount = (text: string) => {
  let count = 1
  for (const char of text) if (char === "\n") count++
  return count
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const global = yield* Global.Service
    const config = yield* Effect.serviceOption(Config.Service)
    const directory = path.join(global.data, MANAGED_DIRECTORY)
    const limits = Effect.fn("ToolOutputStore.limits")(function* () {
      if (Option.isNone(config)) return { maxLines: MAX_LINES, maxBytes: MAX_BYTES }
      const entries = yield* config.value.entries().pipe(Effect.catch(() => Effect.succeed([] as Config.Entry[])))
      const configured = Object.assign(
        {},
        ...entries.flatMap((entry) => (entry.type === "document" ? [entry.info.tool_output ?? {}] : [])),
      )
      return { maxLines: configured.max_lines ?? MAX_LINES, maxBytes: configured.max_bytes ?? MAX_BYTES }
    })

    const write = Effect.fn("ToolOutputStore.write")(function* (content: string) {
      const file = path.join(directory, `tool_${Identifier.ascending()}`)
      yield* fs.ensureDir(directory).pipe(Effect.mapError((cause) => new StorageError({ operation: "write", cause })))
      yield* fs
        .writeFileString(file, content, { flag: "wx" })
        .pipe(Effect.mapError((cause) => new StorageError({ operation: "write", cause })))
      return file
    })

    const bound = Effect.fn("ToolOutputStore.bound")(function* (input: BoundInput) {
      const outputLimits = yield* limits()
      const media = input.output.content.filter((item) => item.type === "file")
      const text = input.output.content.filter((item) => item.type === "text")
      const contextual =
        input.output.content.length === 0
          ? yield* Effect.try({
              try: () => JSON.stringify(input.output.structured, null, 2) ?? String(input.output.structured),
              catch: (cause) => new StorageError({ operation: "encode", cause }),
            })
          : text.map((item) => item.text).join("")
      if (
        lineCount(contextual) <= outputLimits.maxLines &&
        Buffer.byteLength(contextual, "utf-8") <= outputLimits.maxBytes
      )
        return {
          output: input.output,
          outputPaths: [],
        }

      const outputPath = yield* write(contextual)
      const marker = `... output truncated; full content saved to ${outputPath} ...`

      return {
        output: {
          structured: input.output.structured,
          content: [
            {
              type: "text" as const,
              text: boundedPreview(contextual, marker, outputLimits.maxLines, outputLimits.maxBytes),
            },
            ...media,
          ],
        },
        outputPaths: [outputPath],
      }
    })

    const cleanup = Effect.fn("ToolOutputStore.cleanup")(function* () {
      const entries = yield* fs.readDirectory(directory).pipe(Effect.catch(() => Effect.succeed([])))
      const files = yield* Effect.forEach(
        entries.filter((entry) => entry.startsWith("tool_")),
        (entry) => {
          const file = path.join(directory, entry)
          return fs.stat(file).pipe(
            Effect.map((info) => [
              { file, size: Number(info.size), modified: Option.getOrUndefined(info.mtime)?.getTime() },
            ]),
            Effect.catch(() => Effect.succeed([])),
          )
        },
      )
      yield* Effect.forEach(
        evictable(files.flat(), Date.now()),
        (file) => fs.remove(file).pipe(Effect.catch(() => Effect.void)),
        { discard: true },
      )
    })

    return Service.of({ limits, bound, cleanup })
  }),
)

export const node = makeLocationNode({ service: Service, layer, deps: [FSUtil.node, Global.node, Config.node] })

export const nodeWithoutConfig = makeLocationNode({ service: Service, layer, deps: [FSUtil.node, Global.node] })

/** Runs retention scanning once globally rather than once per active Location. */
export const cleanupLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const store = yield* Service
    yield* store.cleanup().pipe(Effect.repeat(Schedule.spaced(Duration.hours(1))), Effect.forkScoped)
  }),
)

export const cleanupNode = makeGlobalNode({
  name: "tool-output-cleanup",
  layer: Layer.merge(layer, cleanupLayer.pipe(Layer.provide(layer))),
  deps: [FSUtil.node, Global.node],
})
