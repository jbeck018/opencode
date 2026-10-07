#!/usr/bin/env bun
// Hands-on test of the experimental history tool, recall subagent, memory tool and /refine against a
// real model. Drives opencode headlessly in a throwaway git worktree, forces a compaction, then checks
// whether the agent can recover facts that only survive in the database, and what memory it saves.
//
//   bun script/try-history-memory.ts --model anthropic/claude-sonnet-4-5
//
// Needs a provider API key in the environment (e.g. ANTHROPIC_API_KEY, OPENAI_API_KEY). By default all
// opencode data (database, memory, auth, config) goes to a temporary directory, so your real sessions
// and memory are untouched; pass --real-data to use your normal data dir and `opencode auth login`
// credentials instead. The agent runs with permissions auto-approved, but only inside the worktree.

import path from "path"
import os from "os"
import fs from "fs"
import { randomBytes } from "crypto"
import { parseArgs } from "util"

const args = parseArgs({
  options: {
    model: { type: "string" },
    "real-data": { type: "boolean", default: false },
    keep: { type: "boolean", default: false },
    port: { type: "string", default: "4517" },
    help: { type: "boolean", default: false },
  },
}).values

if (args.help || !args.model || !args.model.includes("/")) {
  console.log(`usage: bun script/try-history-memory.ts --model <provider>/<model> [--real-data] [--keep] [--port N]

  --model      model to drive opencode with, e.g. anthropic/claude-sonnet-4-5
  --real-data  use your real opencode data dir (sessions, memory, auth) instead of a temp one
  --keep       keep the worktree and temp data dir afterwards for inspection
  --port       port for the temporary opencode server (default 4517)`)
  process.exit(args.help ? 0 : 1)
}

const model = args.model
const [providerID, ...rest] = model.split("/")
const modelID = rest.join("/")
const pkg = path.resolve(import.meta.dir, "..")
const repo = path.resolve(pkg, "../..")
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-try-memory-"))
const worktree = path.join(scratch, "repo")
const password = randomBytes(18).toString("base64url")
const url = `http://127.0.0.1:${args.port}`
const env: Record<string, string> = {
  ...(process.env as Record<string, string>),
  OPENCODE_EXPERIMENTAL_HISTORY_TOOL: "1",
  OPENCODE_EXPERIMENTAL_MEMORY: "1",
  OPENCODE_SERVER_PASSWORD: password,
  ...(args["real-data"]
    ? {}
    : {
        XDG_DATA_HOME: path.join(scratch, "data"),
        XDG_CONFIG_HOME: path.join(scratch, "config"),
        XDG_STATE_HOME: path.join(scratch, "state"),
        XDG_CACHE_HOME: path.join(scratch, "cache"),
      }),
}
const results: { check: string; ok: boolean | "manual"; detail: string }[] = []

console.log(`scratch dir: ${scratch}`)
sh(["git", "-C", repo, "worktree", "add", "--detach", worktree, "HEAD"])
const facts = {
  subjects: sh(["git", "-C", worktree, "log", "-3", "--format=%s"]).trim().split("\n"),
  lines: ["packages/opencode/src/tool/history.ts", "packages/opencode/src/memory/index.ts"].map((file) =>
    String(fs.readFileSync(path.join(worktree, file), "utf8").split("\n").length - 1),
  ),
}

const server = Bun.spawn(
  [
    "bun",
    "run",
    "--cwd",
    pkg,
    "src/index.ts",
    "serve",
    "--port",
    args.port,
    "--hostname",
    "127.0.0.1",
    "--print-logs",
    "--log-level",
    "WARN",
  ],
  { env, stdout: fs.openSync(path.join(scratch, "server.log"), "w"), stderr: "inherit" },
)
process.on("exit", (code) => cleanup(code))
process.on("SIGINT", () => process.exit(130))

try {
  await waitForServer()
  await requireModel()

  step("1/6 Plant facts in tool output")
  const first = await run([
    "--title",
    "history-memory test",
    `Look around this repository for me. Run \`git log -3 --format=%s\` and \`wc -l packages/opencode/src/tool/history.ts packages/opencode/src/memory/index.ts\`, then read packages/opencode/src/tool/history.ts and packages/opencode/src/server/shared.ts in full and explain in two sentences what each does. Do not change any files.`,
  ])
  const sessionID = first.sessionID
  if (!sessionID) throw new Error(`no session ID in run output; see ${scratch}/server.log`)
  console.log(`session: ${sessionID}`)

  step("2/6 Grow the context")
  await run([
    "--session",
    sessionID,
    "Now read packages/opencode/src/session/prompt.ts and packages/core/src/session/projector.ts in full and list the five most important functions in each. Do not change any files.",
  ])

  step("3/6 Force a compaction")
  const compacted = await fetch(`${url}/session/${sessionID}/summarize?directory=${encodeURIComponent(worktree)}`, {
    method: "POST",
    headers: { authorization: `Basic ${btoa(`opencode:${password}`)}`, "content-type": "application/json" },
    body: JSON.stringify({ providerID, modelID }),
  })
  record("compaction ran", compacted.ok, `HTTP ${compacted.status}`)
  const compactedAt = Date.now()

  step("4/6 Ask for details that only survive in the database")
  const recall = (
    await run([
      "--session",
      sessionID,
      "Earlier in this session you ran `git log -3` and `wc -l` on two files. Without running any shell command or reading any file again, tell me the exact three commit subjects and the two line counts. If they are not in your context, find them some other way.",
    ])
  ).text
  record(
    "recalled the commit subjects",
    facts.subjects.every((subject) => includes(recall, subject.slice(0, 40))),
    facts.subjects.join(" | "),
  )
  record(
    "recalled the line counts",
    facts.lines.every((count) => recall.includes(count)),
    facts.lines.join(", "),
  )
  const used = tools(sessionID, compactedAt)
  record(
    "used history or recall instead of re-running commands",
    (used.history ?? 0) + (used.recall ?? 0) > 0 && !used.bash && !used.read,
    JSON.stringify(used),
  )

  step("5/6 Teach preferences, then /refine")
  await run([
    "--session",
    sessionID,
    "Some things about how I work, for future sessions: I maintain this fork and review every PR myself; I want PR descriptions as short bullet lists, never prose paragraphs; and whenever you quote a benchmark number, include how many runs it came from. Also, the default branch here is dev.",
  ])
  await run(["--session", sessionID, "--command", "refine"])
  const projectID = query(`SELECT project_id FROM session WHERE id = '${sessionID}'`).trim().split("\n")[1]
  const dataDir = path.dirname(sh(["bun", "run", "--cwd", pkg, "src/index.ts", "db", "path"], env).trim())
  const memoryDir = path.join(dataDir, "memory", projectID ?? "")
  const index = fs.existsSync(path.join(memoryDir, "MEMORY.md"))
    ? fs.readFileSync(path.join(memoryDir, "MEMORY.md"), "utf8")
    : ""
  record("/refine saved memories", index.includes("]("), memoryDir)
  record(
    "skipped what AGENTS.md already says (default branch)",
    !/default branch|\bdev\b branch|branch.*\bdev\b/i.test(index),
    "the default branch is in AGENTS.md, so it should not be saved",
  )

  step("6/6 New sessions: does memory carry over, and can project search reach the old session?")
  const fresh = (
    await run([
      "Draft a pull request description for a hypothetical change that made session search 3x faster (measured over 5 runs). Do not change any files.",
    ])
  ).text
  record(
    "new session follows the saved preferences",
    "manual",
    "check below: bullet list, not prose, and the run count quoted with the number",
  )
  const older = (
    await run([
      "In an earlier session in this project, someone ran `git log -3`. What were the three commit subjects? Do not run git or read files; find it another way.",
    ])
  ).text
  record(
    "found facts from an earlier session",
    facts.subjects.every((subject) => includes(older, subject.slice(0, 40))),
    facts.subjects.join(" | "),
  )

  console.log(`\n${"=".repeat(72)}\nRESULTS (${model})\n${"=".repeat(72)}`)
  for (const result of results)
    console.log(
      `${result.ok === "manual" ? "CHECK" : result.ok ? "PASS " : "FAIL "}  ${result.check}\n        ${result.detail}`,
    )
  console.log(`\n--- MEMORY.md (${memoryDir}) ---\n${index || "(empty)"}`)
  for (const file of fs.existsSync(memoryDir) ? fs.readdirSync(memoryDir) : [])
    if (file !== "MEMORY.md") console.log(`\n--- ${file} ---\n${fs.readFileSync(path.join(memoryDir, file), "utf8")}`)
  console.log(`\n--- New-session PR description ---\n${fresh.trim()}`)
  console.log(`\n--- Tool calls in the test session ---\n${JSON.stringify(tools(sessionID, 0))}`)
  console.log(
    `\nFull transcripts and server log: ${scratch}${args.keep ? "" : " (deleted on success; pass --keep to keep it)"}`,
  )
} finally {
  server.kill()
}

// Runs one prompt and returns the session ID and the agent's text. Any model or tool error stops the test, so
// a missing key or a refusing provider can't produce a misleading PASS/FAIL table.
async function run(extra: string[]) {
  const out = sh(
    [
      "bun",
      "run",
      "--cwd",
      pkg,
      "src/index.ts",
      "run",
      "--attach",
      url,
      "--password",
      password,
      "--dir",
      worktree,
      "--model",
      model,
      "--auto",
      "--format",
      "json",
      ...extra,
    ],
    env,
  )
  const events = out
    .split("\n")
    .filter((line) => line.startsWith("{"))
    .map((line) => JSON.parse(line) as { type: string; sessionID: string; part?: { text?: string }; error?: unknown })
  const error = events.find((event) => event.type === "error")
  if (error) throw new Error(`model call failed: ${JSON.stringify(error.error)}`)
  const text = events
    .filter((event) => event.type === "text")
    .map((event) => event.part?.text ?? "")
    .join("\n")
  fs.appendFileSync(path.join(scratch, "transcript.txt"), `\n\n>>> ${extra.join(" ")}\n${out}`)
  console.log(text.length > 1500 ? `${text.slice(0, 1500)}\n… (${text.length} chars, events in transcript.txt)` : text)
  return { sessionID: events[0]?.sessionID ?? "", text }
}

// Tool calls per tool in the session and its subagent sessions since `since`; a recall delegation counts as
// "recall".
function tools(sessionID: string, since: number) {
  const rows = query(
    `SELECT CASE WHEN json_extract(data, '$.tool') = 'task' AND json_extract(data, '$.state.input.subagent_type') = 'recall' THEN 'recall' ELSE json_extract(data, '$.tool') END AS tool, count(*) AS n
     FROM part WHERE json_extract(data, '$.type') = 'tool' AND time_created >= ${since}
       AND session_id IN (SELECT id FROM session WHERE id = '${sessionID}' OR parent_id = '${sessionID}')
     GROUP BY 1`,
  )
  return Object.fromEntries(
    rows
      .trim()
      .split("\n")
      .slice(1)
      .filter(Boolean)
      .map((line) => line.split("\t"))
      .map(([tool, count]) => [tool, Number(count)]),
  ) as Record<string, number>
}

function query(sql: string) {
  return sh(["bun", "run", "--cwd", pkg, "src/index.ts", "db", sql.replace(/\s+/g, " ")], env)
}

function sh(cmd: string[], environment: Record<string, string> = env) {
  const result = Bun.spawnSync(cmd, { env: environment, stdout: "pipe", stderr: "pipe" })
  if (result.exitCode !== 0)
    throw new Error(
      `${cmd.slice(0, 6).join(" ")} … failed (${result.exitCode}):\n${result.stdout.toString().slice(-2000)}\n${result.stderr.toString()}`,
    )
  return result.stdout.toString()
}

async function waitForServer() {
  for (let attempt = 0; attempt < 120; attempt++) {
    const ok = await fetch(`${url}/global/health`, {
      headers: { authorization: `Basic ${btoa(`opencode:${password}`)}` },
    })
      .then((response) => response.ok)
      .catch(() => false)
    if (ok) return
    await Bun.sleep(500)
  }
  throw new Error(`server did not start; see ${scratch}/server.log`)
}

async function requireModel() {
  const catalog = (await fetch(`${url}/provider?directory=${encodeURIComponent(worktree)}`, {
    headers: { authorization: `Basic ${btoa(`opencode:${password}`)}` },
  }).then((response) => response.json())) as {
    all: { id: string; models: Record<string, unknown> }[]
    connected: string[]
  }
  if (!catalog.connected.includes(providerID))
    throw new Error(
      `provider "${providerID}" has no credentials. Set its API key in the environment (e.g. ANTHROPIC_API_KEY)${args["real-data"] ? "" : ", or pass --real-data to use your `opencode auth login` credentials"}. Connected: ${catalog.connected.join(", ") || "none"}`,
    )
  const models = Object.keys(catalog.all.find((provider) => provider.id === providerID)?.models ?? {})
  if (!models.includes(modelID))
    throw new Error(`model "${modelID}" not found for ${providerID}. Some available: ${models.slice(0, 8).join(", ")}`)
}

function step(title: string) {
  console.log(`\n### ${title}`)
}

function record(check: string, ok: boolean | "manual", detail: string) {
  results.push({ check, ok, detail })
  console.log(`${ok === "manual" ? "CHECK" : ok ? "PASS" : "FAIL"}: ${check}`)
}

function includes(text: string, needle: string) {
  return text.toLowerCase().includes(needle.toLowerCase())
}

function cleanup(code: number) {
  server.kill()
  if (args.keep || code !== 0) {
    console.log(`\nKept ${scratch} for inspection. Remove it with: git -C ${repo} worktree remove --force ${worktree}`)
    return
  }
  Bun.spawnSync(["git", "-C", repo, "worktree", "remove", "--force", worktree])
  fs.rmSync(scratch, { recursive: true, force: true })
}
