import { describe, expect, test } from "bun:test"
import path from "path"
import { stat } from "fs/promises"
import { SharedServer } from "../../src/server/shared"
import { ServerAuth } from "../../src/server/auth"
import { tmpdir } from "../fixture/fixture"

const entry = path.join(import.meta.dir, "../../src/index.ts")

describe("SharedServer.key", () => {
  test("ignores per-terminal variables", () => {
    const base = { PATH: "/usr/bin", HOME: "/home/u" }
    expect(SharedServer.key({ ...base, TERM_SESSION_ID: "a", PWD: "/a", TMUX_PANE: "%1", OPENCODE_PID: "1" })).toBe(
      SharedServer.key({ ...base, TERM_SESSION_ID: "b", PWD: "/b", TMUX_PANE: "%2", OPENCODE_PID: "2" }),
    )
  })

  test("splits on variables that change how tools run", () => {
    const base = { PATH: "/usr/bin", HOME: "/home/u" }
    expect(SharedServer.key(base)).not.toBe(SharedServer.key({ ...base, PATH: "/opt/node/bin:/usr/bin" }))
    expect(SharedServer.key(base)).not.toBe(SharedServer.key({ ...base, AWS_PROFILE: "prod" }))
    expect(SharedServer.key(base)).not.toBe(SharedServer.key({ ...base, OPENCODE_CONFIG: "/x.json" }))
  })
})

describe("opencode serve --shared", () => {
  test("registers, accepts attaching clients, and exits once idle", async () => {
    const key = SharedServer.key()
    const password = "test-" + Math.random().toString(36).slice(2)
    const child = Bun.spawn(["bun", "run", entry, "serve", "--shared", key], {
      cwd: path.dirname(entry),
      env: { ...process.env, OPENCODE_SERVER_PASSWORD: password, OPENCODE_SHARED_SERVER_IDLE_MS: "3000" },
      stdout: "ignore",
      stderr: "ignore",
    })
    try {
      const info = await waitFor(async () => {
        const result = await SharedServer.probe(key)
        return result.status === "ready" ? result.info : undefined
      }, 45_000)
      expect(info.pid).toBe(child.pid)
      expect(info.password).toBe(password)
      expect((await stat(SharedServer.file(key))).mode & 0o777).toBe(0o600)

      // A launcher in the same environment reuses the running server instead of spawning one.
      expect((await SharedServer.connect().ready)?.pid).toBe(child.pid)

      expect((await fetch(new URL("/global/health", info.url))).status).toBe(401)

      // A TUI is a separate process; its exit is what closes the event stream.
      const client = attach(info)
      await waitFor(async () => (await Bun.file(client.ready).exists()) || undefined, 15_000)
      // Held open well past the idle window: an attached client keeps the server alive.
      await Bun.sleep(5_000)
      expect(child.exitCode).toBeNull()

      client.process.kill()
      expect(await Promise.race([child.exited, Bun.sleep(15_000).then(() => "timeout")])).toBe(0)
      expect(await Bun.file(SharedServer.file(key)).exists()).toBe(false)
    } finally {
      child.kill()
    }
  }, 90_000)
})

describe("global event stream on a shared server", () => {
  test("a directory-scoped subscriber only receives its own project's events and no sync copies", async () => {
    await using a = await tmpdir({ git: true })
    await using b = await tmpdir({ git: true })
    const key = "filter-" + Math.random().toString(36).slice(2)
    const password = "test-" + Math.random().toString(36).slice(2)
    const child = Bun.spawn(["bun", "run", entry, "serve", "--shared", key], {
      cwd: path.dirname(entry),
      env: { ...process.env, OPENCODE_SERVER_PASSWORD: password },
      stdout: "ignore",
      stderr: "ignore",
    })
    try {
      const info = await waitFor(async () => {
        const result = await SharedServer.probe(key)
        return result.status === "ready" ? result.info : undefined
      }, 45_000)
      const headers = ServerAuth.headers({ password })!
      const scoped = collect(new URL(`/global/event?directory=${encodeURIComponent(a.path)}`, info.url), {
        ...headers,
        "x-opencode-event-filter": "project,no-sync",
      })
      // SDK clients created with a directory add it to every GET; without the opt-in header
      // they keep receiving every project's events.
      const unfiltered = collect(new URL(`/global/event?directory=${encodeURIComponent(a.path)}`, info.url), headers)
      const everything = collect(new URL("/global/event", info.url), headers)
      await Promise.all([scoped.connected, everything.connected, unfiltered.connected])

      const create = async (directory: string) => {
        const res = await fetch(new URL("/session", info.url), {
          method: "POST",
          headers: { ...headers, "content-type": "application/json", "x-opencode-directory": directory },
          body: "{}",
        })
        return ((await res.json()) as { id: string }).id
      }
      const ours = await create(a.path)
      const theirs = await create(b.path)
      const mentions = (events: unknown[], id: string) => events.some((event) => JSON.stringify(event).includes(id))
      await waitFor(async () => mentions(everything.events, theirs) || undefined, 15_000)
      await waitFor(async () => mentions(unfiltered.events, theirs) || undefined, 15_000)
      await waitFor(async () => mentions(scoped.events, ours) || undefined, 15_000)

      // Unfiltered subscribers keep today's behaviour: every project, sync copies included.
      expect(mentions(everything.events, ours)).toBe(true)
      expect(everything.events.some((event) => event.payload?.type === "sync")).toBe(true)
      expect(mentions(scoped.events, theirs)).toBe(false)
      expect(scoped.events.some((event) => event.payload?.type === "sync")).toBe(false)
      expect(unfiltered.events.some((event) => event.payload?.type === "sync")).toBe(true)
      scoped.close()
      everything.close()
      unfiltered.close()
    } finally {
      child.kill()
    }
  }, 90_000)
})

function collect(url: URL, headers: Record<string, string>) {
  const ctrl = new AbortController()
  const events: { payload?: { type?: string } }[] = []
  const connected = Promise.withResolvers<void>()
  void fetch(url, { headers, signal: ctrl.signal })
    .then(async (res) => {
      const reader = res.body!.getReader()
      const state = { buffer: "" }
      while (true) {
        const chunk = await reader.read()
        if (chunk.done) return
        state.buffer += new TextDecoder().decode(chunk.value)
        const frames = state.buffer.split("\n\n")
        state.buffer = frames.pop() ?? ""
        for (const frame of frames) {
          const data = frame.split("\n").find((line) => line.startsWith("data: "))
          if (!data) continue
          events.push(JSON.parse(data.slice(6)))
          connected.resolve()
        }
      }
    })
    .catch(() => {})
  return { events, connected: connected.promise, close: () => ctrl.abort() }
}

function attach(info: SharedServer.Info) {
  const ready = path.join(process.env["XDG_STATE_HOME"]!, `attach-${Math.random().toString(36).slice(2)}`)
  const script = `const res = await fetch(${JSON.stringify(new URL("/global/event", info.url).toString())}, { headers: ${JSON.stringify(ServerAuth.headers({ password: info.password }))} })
await res.body.getReader().read()
await Bun.write(${JSON.stringify(ready)}, "")
await Bun.sleep(600_000)`
  return { ready, process: Bun.spawn(["bun", "-e", script], { stdout: "ignore", stderr: "ignore" }) }
}

async function waitFor<T>(fn: () => Promise<T | undefined>, timeout: number): Promise<T> {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    const value = await fn()
    if (value) return value
    await Bun.sleep(100)
  }
  throw new Error("timed out")
}
