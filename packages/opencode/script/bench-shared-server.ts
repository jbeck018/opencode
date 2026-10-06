#!/usr/bin/env bun
// Load test for the shared background server: N repos each run agent turns that
// stream text and a noisy bash tool against a fake OpenAI-compatible model, while
// one event-stream client per repo (what a TUI does) consumes /global/event.
//
//   bun run script/bench-shared-server.ts --binary dist/opencode-linux-x64/bin/opencode --mode shared
//   bun run script/bench-shared-server.ts --binary ... --mode standalone   # one server per repo
//
// Reports server CPU, server responsiveness (health latency sampled during load),
// token-to-client latency, per-client event traffic, database growth and memory.
import os from "os"
import path from "path"
import { mkdir, rm, stat } from "fs/promises"
import { readFileSync } from "fs"
import { parseArgs } from "util"

const { values: args } = parseArgs({
  options: {
    binary: { type: "string" },
    mode: { type: "string", default: "shared" },
    sessions: { type: "string", default: "5" },
    rounds: { type: "string", default: "2" },
    lines: { type: "string", default: "400" },
    llm: { type: "boolean", default: false },
    // Prompt one repo at a time and report server CPU per prompt instead of running them concurrently.
    sequential: { type: "boolean", default: false },
  },
})

if (args.llm) await fakeModel()
else await bench()

async function bench() {
  if (!args.binary) throw new Error("--binary is required")
  const binary = path.resolve(args.binary)
  const sessions = Number(args.sessions)
  const rounds = Number(args.rounds)
  const home = path.join(os.tmpdir(), `oc-bench-${process.pid}`)
  await rm(home, { recursive: true, force: true })
  const repos = await Promise.all(
    Array.from({ length: sessions }, async (_, i) => {
      const dir = path.join(home, "repos", `r${i}`)
      await mkdir(dir, { recursive: true })
      await Bun.write(path.join(dir, "README.md"), `repo ${i}\n`)
      // A root commit gives each repo its own project; without one they all share "global".
      await Bun.$`git init -q ${dir} && git -C ${dir} add . && git -C ${dir} -c user.email=bench@example.com -c user.name=bench commit -qm init`.quiet()
      return dir
    }),
  )

  const llm = Bun.spawn(["bun", import.meta.path, "--llm", "--lines", args.lines ?? "400"], {
    stdout: "pipe",
    stderr: "inherit",
  })
  const llmUrl = (await firstLine(llm.stdout)).trim()

  const password = "bench"
  const env: Record<string, string> = {
    ...Object.fromEntries(Object.entries(process.env).filter((e): e is [string, string] => e[1] !== undefined)),
    HOME: home,
    XDG_CONFIG_HOME: path.join(home, "config"),
    XDG_DATA_HOME: path.join(home, "data"),
    XDG_STATE_HOME: path.join(home, "state"),
    XDG_CACHE_HOME: path.join(home, "cache"),
    OPENCODE_SERVER_PASSWORD: password,
    OPENCODE_DB: "bench.db",
    OPENCODE_DISABLE_AUTOUPDATE: "1",
    OPENCODE_DISABLE_MODELS_FETCH: "1",
    OPENCODE_SHARED_SERVER_IDLE_MS: "600000",
    OPENCODE_CONFIG_CONTENT: JSON.stringify({
      formatter: false,
      lsp: false,
      model: "fake/model",
      small_model: "fake/model",
      provider: {
        fake: {
          name: "Fake",
          npm: "@ai-sdk/openai-compatible",
          env: [],
          options: { apiKey: "x", baseURL: llmUrl },
          models: {
            model: {
              id: "model",
              name: "Model",
              tool_call: true,
              attachment: false,
              reasoning: false,
              temperature: false,
              release_date: "2025-01-01",
              limit: { context: 200_000, output: 10_000 },
              cost: { input: 0, output: 0 },
              options: {},
            },
          },
        },
      },
    }),
  }
  delete env["BUN_OPTIONS"]
  const auth = { Authorization: `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}` }

  const servers =
    args.mode === "shared"
      ? [await startShared(binary, env, home)]
      : await Promise.all(repos.map(() => startStandalone(binary, env)))
  const serverFor = (i: number) => servers[args.mode === "shared" ? 0 : i]

  // Warm every repo (instance boot) before measuring the load itself.
  const sessionIDs = await Promise.all(
    repos.map(async (dir, i) => {
      const res = await fetch(new URL("/session", serverFor(i).url), {
        method: "POST",
        headers: { ...auth, "content-type": "application/json", "x-opencode-directory": dir },
        body: JSON.stringify({ permission: [{ permission: "*", pattern: "*", action: "allow" }] }),
      })
      return ((await res.json()) as { id: string }).id
    }),
  )

  const clients = repos.map((dir, i) => subscribe(serverFor(i).url, auth, dir))
  const probes = servers.map((server) => probe(server.url, auth))
  const cpuBefore = servers.map((server) => cpu(server.pid))
  const childCpuBefore = servers.map((server) => cpu(server.pid, true))
  const dbBefore = await dbSize(env.XDG_DATA_HOME)
  const started = performance.now()
  const posts = repos.map((): number[] => [])

  const promptCpu = repos.map((): number[] => [])
  const prompt = async (dir: string, i: number, n: number) => {
    const before = cpu(serverFor(i).pid)
    posts[i].push(performance.timeOrigin + performance.now())
    const res = await fetch(new URL(`/session/${sessionIDs[i]}/message`, serverFor(i).url), {
      method: "POST",
      headers: { ...auth, "content-type": "application/json", "x-opencode-directory": dir },
      body: JSON.stringify({
        agent: "build",
        parts: [{ type: "text", text: `round ${n}: run the noisy job` }],
      }),
    })
    if (!res.ok) throw new Error(`prompt failed: ${res.status} ${await res.text()}`)
    await res.arrayBuffer()
    promptCpu[i].push(round(cpu(serverFor(i).pid) - before))
  }
  if (args.sequential) {
    for (const [i, dir] of repos.entries()) for (let n = 0; n < rounds; n++) await prompt(dir, i, n)
  } else {
    await Promise.all(
      repos.map(async (dir, i) => {
        for (let n = 0; n < rounds; n++) await prompt(dir, i, n)
      }),
    )
  }

  const wall = (performance.now() - started) / 1000
  const serverCpu = servers.reduce((sum, server, i) => sum + cpu(server.pid) - cpuBefore[i], 0)
  const childCpu = servers.reduce((sum, server, i) => sum + cpu(server.pid, true) - childCpuBefore[i], 0)
  const latencies = probes.flatMap((p) => p.stop()).toSorted((a, b) => a - b)
  const traffic = clients.map((c) => c.stop())
  const dbGrowth = (await dbSize(env.XDG_DATA_HOME)) - dbBefore
  const pss = servers.reduce((sum, server) => sum + memory(server.pid), 0)

  const tokens = traffic.flatMap((t) => t.latencies).toSorted((a, b) => a - b)
  // Prompt submitted to the first agent token reaching that repo's client.
  const first = posts
    .flatMap((times, i) =>
      times.map((posted) => (traffic[i].arrivals.find((arrival) => arrival >= posted) ?? posted) - posted),
    )
    .toSorted((a, b) => a - b)
  const pct = (q: number, list = latencies) => list[Math.min(list.length - 1, Math.floor(list.length * q))] ?? 0
  console.log(
    JSON.stringify(
      {
        mode: args.mode,
        sessions,
        rounds,
        wall_s: round(wall),
        server_cpu_s: round(serverCpu),
        ...(args.sequential ? { prompt_cpu_s: promptCpu } : {}),
        // git, shell commands and other processes the server spawned and reaped
        child_cpu_s: round(childCpu),
        server_busy_pct: round((100 * serverCpu) / wall / servers.length),
        health_ms: { p50: round(pct(0.5)), p99: round(pct(0.99)), max: round(pct(1)) },
        // Time from the model sending a text token to a subscribed client receiving it.
        first_token_ms: { p50: round(pct(0.5, first)), max: round(pct(1, first)) },
        token_ms: {
          count: tokens.length,
          p50: round(pct(0.5, tokens)),
          p99: round(pct(0.99, tokens)),
          max: round(pct(1, tokens)),
        },
        per_client: {
          events: Math.round(avg(traffic.map((t) => t.events))),
          mb: round(avg(traffic.map((t) => t.bytes)) / 1e6),
        },
        db_growth_mb: round(dbGrowth / 1e6),
        server_pss_mb: Math.round(pss),
      },
      null,
      2,
    ),
  )

  for (const server of servers) server.process.kill()
  llm.kill()
  await Promise.all(servers.map((server) => server.process.exited))
  await rm(home, { recursive: true, force: true })
}

async function startShared(binary: string, env: Record<string, string>, home: string) {
  const key = "bench"
  const file = path.join(env.XDG_STATE_HOME, "opencode", "shared-server", `${key}.json`)
  const proc = Bun.spawn([binary, "serve", "--shared", key], { env, cwd: home, stdout: "ignore", stderr: "ignore" })
  const deadline = Date.now() + 60_000
  while (!(await Bun.file(file).exists())) {
    if (Date.now() > deadline || proc.exitCode !== null) throw new Error("shared server did not start")
    await Bun.sleep(20)
  }
  const info = (await Bun.file(file).json()) as { url: string; pid: number }
  return { url: info.url, pid: info.pid, process: proc }
}

async function startStandalone(binary: string, env: Record<string, string>) {
  const proc = Bun.spawn([binary, "serve", "--port", "0", "--hostname", "127.0.0.1"], {
    env,
    stdout: "pipe",
    stderr: "ignore",
  })
  const reader = proc.stdout.getReader()
  const decoder = new TextDecoder()
  const state = { text: "" }
  while (!/listening on (http:\/\/\S+)/.test(state.text)) {
    const chunk = await reader.read()
    if (chunk.done) throw new Error("standalone server did not start")
    state.text += decoder.decode(chunk.value)
  }
  const url = state.text.match(/listening on (http:\/\/\S+)/)![1]
  return { url, pid: proc.pid, process: proc }
}

// What a TUI does: hold /global/event open, scoped to its project, and consume it.
function subscribe(url: string, auth: Record<string, string>, directory: string) {
  const ctrl = new AbortController()
  const totals = { events: 0, bytes: 0 }
  const latencies: number[] = []
  const arrivals: number[] = []
  const seen = new Set<string>()
  const pending = { text: "" }
  const query = new URLSearchParams({ directory, sync: "false" })
  void fetch(new URL(`/global/event?${query}`, url), {
    headers: { ...auth, "x-opencode-directory": directory },
    signal: ctrl.signal,
  })
    .then(async (res) => {
      const reader = res.body!.getReader()
      while (true) {
        const chunk = await reader.read()
        if (chunk.done) return
        const now = performance.timeOrigin + performance.now()
        totals.bytes += chunk.value.byteLength
        const text = pending.text + Buffer.from(chunk.value).toString()
        const frames = text.split("\n\n")
        pending.text = frames.pop() ?? ""
        totals.events += frames.length
        // A token's first appearance is its delta; later full-part updates repeat it.
        for (const match of frames.join("\n").matchAll(/t@(\d+\.\d+)/g)) {
          if (seen.has(match[1])) continue
          seen.add(match[1])
          latencies.push(now - Number(match[1]))
          arrivals.push(now)
        }
      }
    })
    .catch(() => {})
  return {
    stop() {
      ctrl.abort()
      return { ...totals, latencies, arrivals }
    },
  }
}

// Health latency while under load: how long the server's only JS thread takes to answer.
function probe(url: string, auth: Record<string, string>) {
  const samples: number[] = []
  const state = { running: true }
  void (async () => {
    while (state.running) {
      const t = performance.now()
      await fetch(new URL("/global/health", url), { headers: auth }).catch(() => {})
      samples.push(performance.now() - t)
      await Bun.sleep(50)
    }
  })()
  return {
    stop() {
      state.running = false
      return samples
    },
  }
}

function cpu(pid: number, children = false) {
  const fields = readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" ")
  if (children) return (Number(fields[13]) + Number(fields[14])) / 100
  return (Number(fields[11]) + Number(fields[12])) / 100
}

function memory(pid: number) {
  const rollup = readFileSync(`/proc/${pid}/smaps_rollup`, "utf8")
  return Number(rollup.match(/^Pss:\s+(\d+)/m)?.[1] ?? 0) / 1024
}

async function dbSize(dataHome: string) {
  const base = path.join(dataHome, "opencode", "bench.db")
  const sizes = await Promise.all(
    ["", "-wal"].map((suffix) =>
      stat(base + suffix).then(
        (s) => s.size,
        () => 0,
      ),
    ),
  )
  return sizes[0] + sizes[1]
}

async function firstLine(stream: ReadableStream<Uint8Array>) {
  const reader = stream.getReader()
  const chunk = await reader.read()
  reader.releaseLock()
  return new TextDecoder().decode(chunk.value)
}

function avg(values: number[]) {
  return values.reduce((a, b) => a + b, 0) / Math.max(1, values.length)
}

function round(value: number) {
  return Math.round(value * 100) / 100
}

// Fake OpenAI-compatible chat model. A turn streams ~100 text tokens and then calls
// bash with a command printing `--lines` lines over ~2s; after the tool result it
// streams a short answer and stops.
async function fakeModel() {
  const lines = Number(args.lines)
  const command = `for i in $(seq 1 ${lines}); do echo "line $i: compiling packages/core/src/module-$i.ts ... ok (12ms)"; sleep 0.005; done`
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req) {
      const body = (await req.json()) as { messages: { role: string }[]; tools?: unknown[] }
      const afterTool = body.messages.at(-1)?.role === "tool"
      // Only agent turns carry tools; side requests such as title generation never stream to clients.
      const agent = (body.tools?.length ?? 0) > 0
      const encoder = new TextEncoder()
      const send = (controller: ReadableStreamDefaultController, delta: object, finish?: string) =>
        controller.enqueue(
          encoder.encode(
            `data: ${JSON.stringify({ id: "x", object: "chat.completion.chunk", choices: [{ index: 0, delta, ...(finish ? { finish_reason: finish } : {}) }] })}\n\n`,
          ),
        )
      const stream = new ReadableStream({
        async start(controller) {
          send(controller, { role: "assistant" })
          for (let i = 0; i < (afterTool ? 30 : 100); i++) {
            // Stamped with the send time so clients can measure token-to-TUI latency.
            send(controller, {
              content: agent ? `t@${(performance.timeOrigin + performance.now()).toFixed(3)} ` : `token${i} `,
            })
            await Bun.sleep(10)
          }
          if (afterTool) {
            send(controller, {}, "stop")
          } else {
            send(controller, {
              tool_calls: [
                {
                  index: 0,
                  id: `call_${Math.random().toString(36).slice(2)}`,
                  type: "function",
                  function: { name: "bash", arguments: JSON.stringify({ command, description: "Run noisy job" }) },
                },
              ],
            })
            send(controller, {}, "tool_calls")
          }
          controller.enqueue(encoder.encode("data: [DONE]\n\n"))
          controller.close()
        },
      })
      return new Response(stream, { headers: { "content-type": "text/event-stream" } })
    },
  })
  console.log(`http://127.0.0.1:${server.port}/v1`)
}
