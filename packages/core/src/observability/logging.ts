import { Effect, Formatter, Logger, Schedule, type Duration, type LogLevel } from "effect"
import fs from "fs"
import path from "path"
import { Global } from "../global"
import { runID } from "./shared"

function formatter(id: string = runID) {
  return Logger.map(Logger.formatStructured, (output) => {
    const messages = Array.isArray(output.message) ? output.message : [output.message]
    return [
      ["timestamp", output.timestamp],
      ["level", output.level],
      ["run", id],
      ...messages.flatMap((value) => (plain(value) ? flatten(value) : [["message", value] as const])),
      ...(output.cause === undefined ? [] : [["cause", output.cause] as const]),
      ...flatten(output.spans),
      ...flatten(output.annotations),
    ]
      .map(([key, value]) => `${key}=${format(value)}`)
      .join(" ")
  })
}

function flatten(
  input: Record<string, unknown>,
  prefix = "",
  seen = new WeakSet<object>(),
): Array<readonly [string, unknown]> {
  if (seen.has(input)) return [[prefix, "[Circular]"]]
  seen.add(input)
  const entries = Object.entries(input)
  if (entries.length === 0 && prefix) return [[prefix, input]]
  return entries.flatMap(([key, value]) => {
    const path = prefix ? `${prefix}.${key}` : key
    return plain(value) ? flatten(value, path, seen) : [[path, value] as const]
  })
}

function plain(input: unknown): input is Record<string, unknown> {
  if (input === null || typeof input !== "object" || Array.isArray(input)) return false
  const prototype = Object.getPrototypeOf(input)
  return prototype === Object.prototype || prototype === null
}

function format(input: unknown) {
  const value = typeof input === "string" ? input : Formatter.format(input)
  return /^[^\s="\\]+$/.test(value) ? value : JSON.stringify(value)
}

const MAX_BYTES = 50 * 1024 * 1024
const KEEP_ROTATED = 3
const KEEP_LEGACY = 10
const STALE_ROTATING_MS = 5 * 60 * 1000
const legacy = /^\d{4}-\d{2}-\d{2}T\d{6}\.log$/

export function fileLogger(
  file = path.join(Global.Path.log, "opencode.log"),
  id: string = runID,
  options: { maxBytes?: number; checkInterval?: Duration.Input } = {},
) {
  const max = options.maxBytes ?? MAX_BYTES
  return Effect.gen(function* () {
    rotate(file, max)
    prune(file)
    const sink = { fd: attempt(() => fs.openSync(file, "a")) }
    yield* Effect.addFinalizer(() => Effect.sync(() => close(sink.fd)))
    // Rotation at startup alone never bounds a long-running process, and another process may rotate the file
    // away from under this one, so periodically rotate if needed and reopen whatever file is now at the path.
    yield* Effect.sync(() => {
      sink.fd = reopen(file, sink.fd, max)
    }).pipe(Effect.repeat(Schedule.spaced(options.checkInterval ?? "1 minute")), Effect.forkScoped)
    // Do not set the batch window to 0; it causes high idle CPU usage.
    return yield* Logger.batched(formatter(id), {
      window: 1000,
      flush: (output) =>
        Effect.sync(() => {
          const fd = sink.fd
          if (fd !== undefined) attempt(() => fs.appendFileSync(fd, output.join("\n") + "\n"))
        }),
    })
  })
}

// Rotation is best-effort: other processes may hold the file open or rotate it at the same time, and the directory may
// be read-only, so no failure may escape into logger construction.
export function rotate(file: string, max = MAX_BYTES) {
  attempt(() => {
    if ((fs.statSync(file, { throwIfNoEntry: false })?.size ?? 0) <= max) return
    // Claiming the file first means a concurrent rotator finds nothing to rename instead of shifting the chain twice.
    const claimed = `${file}.rotating-${process.pid}`
    if (!renameIfExists(file, claimed)) return
    // Renaming keeps the old mtime, so refresh it to keep a concurrent prune from treating the claim as stale.
    attempt(() => fs.utimesSync(claimed, new Date(), new Date()))
    attempt(() => fs.rmSync(`${file}.${KEEP_ROTATED}`, { force: true }))
    Array.from({ length: KEEP_ROTATED - 1 }, (_, index) => KEEP_ROTATED - 1 - index).forEach((n) =>
      attempt(() => renameIfExists(`${file}.${n}`, `${file}.${n + 1}`)),
    )
    renameIfExists(claimed, `${file}.1`)
  })
}

function reopen(file: string, fd: number | undefined, max: number) {
  rotate(file, max)
  const current = attempt(() => fs.statSync(file, { bigint: true, throwIfNoEntry: false })?.ino)
  if (fd !== undefined && current !== undefined && current === attempt(() => fs.fstatSync(fd, { bigint: true }).ino))
    return fd
  // Keep writing to the old file when the path cannot be opened rather than dropping every later line.
  const next = attempt(() => fs.openSync(file, "a"))
  if (next === undefined) return fd
  close(fd)
  return next
}

function close(fd: number | undefined) {
  if (fd !== undefined) attempt(() => fs.closeSync(fd))
}

function renameIfExists(from: string, to: string) {
  try {
    fs.renameSync(from, to)
    return true
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error
    return false
  }
}

// Removes claims left by a process that died mid-rotation, and legacy timestamped logs beyond the newest few.
function prune(file: string) {
  const dir = path.dirname(file)
  const names = attempt(() => fs.readdirSync(dir)) ?? []
  const claim = `${path.basename(file)}.rotating-`
  names
    .filter((name) => name.startsWith(claim))
    .forEach((name) =>
      attempt(() => {
        const target = path.join(dir, name)
        if (Date.now() - fs.statSync(target).mtimeMs > STALE_ROTATING_MS) fs.rmSync(target, { force: true })
      }),
    )
  names
    .filter((name) => legacy.test(name))
    .sort()
    .slice(0, -KEEP_LEGACY)
    .forEach((name) => attempt(() => fs.rmSync(path.join(dir, name), { force: true })))
}

function attempt<A>(fn: () => A) {
  try {
    return fn()
  } catch {
    return undefined
  }
}

const stderrLogger = Logger.make((options) => process.stderr.write(formatter().log(options) + "\n"))

export function minimumLogLevel() {
  const value = process.env.OPENCODE_LOG_LEVEL?.toUpperCase()
  const levels = {
    DEBUG: "Debug",
    INFO: "Info",
    WARN: "Warn",
    ERROR: "Error",
  } as const satisfies Record<string, LogLevel.LogLevel>
  return value && value in levels ? levels[value as keyof typeof levels] : levels.INFO
}

export function loggers() {
  return process.env.OPENCODE_PRINT_LOGS === "1" ? [fileLogger(), stderrLogger] : [fileLogger()]
}

export * as Logging from "./logging"
