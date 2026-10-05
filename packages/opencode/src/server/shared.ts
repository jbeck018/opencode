export * as SharedServer from "./shared"

import os from "os"
import path from "path"
import { spawn } from "child_process"
import { randomBytes } from "crypto"
import { mkdir, rm, writeFile } from "fs/promises"
import { Flag } from "@opencode-ai/core/flag/flag"
import { Global } from "@opencode-ai/core/global"
import { InstallationVersion } from "@opencode-ai/core/installation/version"
import { Flock } from "@opencode-ai/core/util/flock"
import { Hash } from "@opencode-ai/core/util/hash"
import { ServerAuth } from "./auth"

// A shared server is one background `opencode serve --shared <key>` process per
// (version, environment). Terminals attach to it instead of booting a private
// server in a worker, so N terminals cost one server plus N thin TUIs.

export type Info = {
  url: string
  pid: number
  version: string
  password: string
}

const READY_TIMEOUT_MS = 20_000
const DEFAULT_IDLE_MS = 5 * 60_000

// Variables that differ per terminal tab, pane or SSH connection without changing
// how tools behave, plus the constant markers the CLI sets on itself. Everything else (PATH, credentials, direnv/mise exports,
// OPENCODE_CONFIG, ...) is part of the key, so a shell tool never runs with a
// different terminal's environment. An unknown per-tab variable only costs
// sharing, never correctness.
const VOLATILE =
  /^(PWD|OLDPWD|SHLVL|_|COLUMNS|LINES|AGENT|OPENCODE|OPENCODE_PID|WINDOWID|WINDOW|STY|TMUX|TMUX_PANE|GPG_TTY|SSH_TTY|SSH_CLIENT|SSH_CONNECTION|SECURITYSESSIONID|TERM_SESSION_ID|ITERM_SESSION_ID|WT_SESSION|KITTY_WINDOW_ID|KITTY_PID|WEZTERM_PANE|ALACRITTY_WINDOW_ID|ZELLIJ_PANE_ID|ZELLIJ_SESSION_NAME|KONSOLE_DBUS_WINDOW|KONSOLE_DBUS_SESSION|GNOME_TERMINAL_SCREEN|TERMINATOR_UUID|GHOSTTY_.*|VSCODE_.*)$/

export function key(env: Record<string, string | undefined> = process.env) {
  const stable = Object.entries(env)
    .filter((entry): entry is [string, string] => entry[1] !== undefined && !VOLATILE.test(entry[0]))
    .toSorted((a, b) => (a[0] < b[0] ? -1 : 1))
  return Hash.fast(JSON.stringify([InstallationVersion, stable])).slice(0, 16)
}

export function file(serverKey: string) {
  return path.join(Global.Path.state, "shared-server", serverKey + ".json")
}

export function disabled() {
  return Flag.OPENCODE_DISABLE_SHARED_SERVER
}

// Returns a healthy shared server for this environment, starting one if needed.
// `ready` resolves undefined when no server could be reached so callers fall back
// to a private in-process server. `started` resolves as soon as nothing is left
// for this process to kick off (a server was found or spawned, or another process
// owns the spawn), so the CLI entry can wait on it without waiting for the boot.
//
// Memoized per key: the CLI entry starts connecting before the TUI loads, and the
// TUI command then joins that same in-flight attempt instead of starting another.
export type Attempt = { ready: Promise<Info | undefined>; started: Promise<void> }

const attempts = new Map<string, Attempt>()

export function connect(): Attempt {
  const serverKey = key()
  const existing = attempts.get(serverKey)
  if (existing) return existing
  const started = Promise.withResolvers<void>()
  const ready = discover(serverKey, started.resolve)
    .catch(() => undefined)
    .finally(started.resolve)
  const attempt = { ready, started: started.promise }
  attempts.set(serverKey, attempt)
  return attempt
}

async function discover(serverKey: string, started: () => void) {
  const running = await probe(serverKey)
  if (running.status === "ready") return running.info
  // Serialize discovery + spawn so terminals launched together share one server.
  return Flock.withLock(
    `shared-server:${serverKey}`,
    async () => {
      const current = await settle(serverKey, started)
      if (current.status === "ready") return current.info
      // Alive but unresponsive: starting another would duplicate it, so run privately instead.
      if (current.status === "busy") return undefined
      return start(serverKey, started)
    },
    // Another launcher holds the lock and is starting the server already.
    { timeoutMs: READY_TIMEOUT_MS + 5_000, onWait: started },
  )
}

export type Probe = { status: "ready"; info: Info } | { status: "busy" } | { status: "absent" }

// "busy" means the server accepted the connection but did not answer in time, e.g.
// while it boots several projects at once. Only "absent" justifies starting a server.
export async function probe(serverKey: string, timeoutMs = 1_000): Promise<Probe> {
  const info: Info | undefined = await Bun.file(file(serverKey))
    .json()
    .catch(() => undefined)
  if (!info) return { status: "absent" }
  const health = await fetch(new URL("/global/health", info.url), {
    headers: ServerAuth.headers({ password: info.password }),
    signal: AbortSignal.timeout(timeoutMs),
  })
    .then(async (res) => (res.ok ? await res.json() : undefined))
    .catch((error: unknown) => (error instanceof Error && error.name === "TimeoutError" ? "busy" : undefined))
  if (health === "busy") return { status: "busy" }
  if (health?.healthy !== true || health.version !== InstallationVersion) return { status: "absent" }
  return { status: "ready", info }
}

async function settle(serverKey: string, busy?: () => void) {
  const deadline = Date.now() + READY_TIMEOUT_MS
  while (Date.now() < deadline) {
    const result = await probe(serverKey, 5_000)
    if (result.status !== "busy") return result
    busy?.()
  }
  return { status: "busy" } as const
}

async function start(serverKey: string, started: () => void) {
  // Any file left here belongs to a server that failed the probe above.
  await rm(file(serverKey), { force: true })
  const command = self()
  const child = spawn(command[0], [...command.slice(1), "serve", "--shared", serverKey], {
    cwd: os.homedir(),
    detached: true,
    stdio: "ignore",
    env: { ...process.env, OPENCODE_SERVER_PASSWORD: randomBytes(24).toString("base64url") },
  })
  child.unref()
  started()
  const state = { exited: false }
  child.once("exit", () => (state.exited = true))
  child.once("error", () => (state.exited = true))
  const deadline = Date.now() + READY_TIMEOUT_MS
  while (!state.exited && Date.now() < deadline) {
    if (await Bun.file(file(serverKey)).exists()) {
      const result = await settle(serverKey)
      return result.status === "ready" ? result.info : undefined
    }
    await Bun.sleep(25)
  }
  return undefined
}

// A compiled binary embeds its entrypoint; a source checkout needs the script path.
function self() {
  if (Bun.main.startsWith("/$bunfs/") || Bun.main.startsWith("B:/~BUN/")) return [process.execPath]
  return [process.execPath, Bun.main]
}

// Server side. Event streams call open/close so the server can exit once no
// client has been attached for the idle window.
const clients = { count: 0, idleSince: Date.now() }

export function open() {
  clients.count++
}

export function close() {
  clients.count--
  if (clients.count === 0) clients.idleSince = Date.now()
}

export async function register(input: { key: string; url: URL; shutdown: () => Promise<void> }) {
  const target = file(input.key)
  const info: Info = {
    url: input.url.toString(),
    pid: process.pid,
    version: InstallationVersion,
    password: Flag.OPENCODE_SERVER_PASSWORD ?? "",
  }
  await mkdir(path.dirname(target), { recursive: true, mode: 0o700 })
  // The file carries the server password, so only the owner may read it.
  await writeFile(target, JSON.stringify(info), { mode: 0o600 })

  const idle = Number(Flag.OPENCODE_SHARED_SERVER_IDLE_MS) || DEFAULT_IDLE_MS
  const exit = async () => {
    clearInterval(timer)
    const current: Info | undefined = await Bun.file(target)
      .json()
      .catch(() => undefined)
    // A newer server may have replaced this one's discovery file.
    if (current?.pid === process.pid) await rm(target, { force: true })
    await input.shutdown().catch(() => {})
    process.exit(0)
  }
  const timer = setInterval(
    () => {
      if (clients.count > 0 || Date.now() - clients.idleSince < idle) return
      void exit()
    },
    Math.min(idle, 5_000),
  )
  process.once("SIGTERM", () => void exit())
  process.once("SIGINT", () => void exit())
  process.once("SIGHUP", () => void exit())
}
