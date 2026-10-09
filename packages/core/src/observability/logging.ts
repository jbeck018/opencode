import { Formatter, Logger, type LogLevel } from "effect"
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
const legacy = /^\d{4}-\d{2}-\d{2}T\d{6}\.log$/

// Other processes may hold the file open or rotate it at the same time, so every step tolerates a missing file.
export function rotate(file: string, max = MAX_BYTES) {
  if ((fs.statSync(file, { throwIfNoEntry: false })?.size ?? 0) <= max) return
  // Claiming the file first means a concurrent rotator finds nothing to rename instead of shifting the chain twice.
  const claimed = `${file}.rotating-${process.pid}`
  if (!renameIfExists(file, claimed)) return
  fs.rmSync(`${file}.${KEEP_ROTATED}`, { force: true })
  Array.from({ length: KEEP_ROTATED - 1 }, (_, index) => KEEP_ROTATED - 1 - index).forEach((n) =>
    renameIfExists(`${file}.${n}`, `${file}.${n + 1}`),
  )
  renameIfExists(claimed, `${file}.1`)
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

function removeLegacy(dir: string) {
  const files = fs.existsSync(dir) ? fs.readdirSync(dir).filter((name) => legacy.test(name)).sort() : []
  files.slice(0, -KEEP_LEGACY).forEach((name) => fs.rmSync(path.join(dir, name), { force: true }))
}

export function fileLogger(file = path.join(Global.Path.log, "opencode.log"), id: string = runID) {
  rotate(file)
  removeLegacy(path.dirname(file))
  // Do not set batchWindow to 0; it causes high idle CPU usage.
  return Logger.toFile(formatter(id), file, { flag: "a" })
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
