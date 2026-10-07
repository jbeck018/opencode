import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Effect } from "effect"
import { Agent } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Config } from "@/config/config"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Session } from "@/session/session"
import { MessageID, PartID, type SessionID } from "../../src/session/schema"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { HistoryTool, requiredLiterals } from "../../src/tool/history"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"

afterEach(async () => {
  await disposeAllInstances()
})

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const layer = (flags: Partial<RuntimeFlags.Info> = {}) =>
  LayerNode.compile(
    LayerNode.group([
      Agent.node,
      BackgroundJob.node,
      EventV2Bridge.node,
      Config.node,
      CrossSpawnSpawner.node,
      Session.node,
      SessionProjector.node,
      SessionRunState.node,
      SessionStatus.node,
      Truncate.node,
      ToolRegistry.node,
      Database.node,
      RuntimeFlags.node,
      Ripgrep.node,
    ]),
    [[RuntimeFlags.node, RuntimeFlags.layer(flags)]],
  )

const enabled = testEffect(layer({ experimentalHistoryTool: true }))
const disabled = testEffect(layer())

const ctx = (sessionID: SessionID) => ({
  sessionID,
  messageID: MessageID.make("msg_test"),
  callID: "",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
})

// A parent session whose bash output was cleared by compaction pruning, and a subagent session
// spawned from it.
const seed = Effect.fn("HistoryToolTest.seed")(function* () {
  const session = yield* Session.Service
  const parent = yield* session.create({ title: "parent" })
  const user = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: parent.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: user.id,
    sessionID: parent.id,
    type: "text",
    text: "Deploy to the blue-7 cluster",
  })
  const assistant = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "assistant",
    parentID: user.id,
    sessionID: parent.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: assistant.id,
    sessionID: parent.id,
    type: "tool",
    tool: "bash",
    callID: "call-1",
    state: {
      status: "completed",
      input: { command: "cat .env" },
      output: "HOST=localhost\nSECRET_PORT=4821\nMODE=dev",
      title: "cat .env",
      metadata: {},
      time: { start: 1, end: 2, compacted: 3 },
    },
  })
  const child = yield* session.create({ title: "child", parentID: parent.id })
  return { parent, child, assistant }
})

describe("tool.history", () => {
  enabled.instance("searches cleared tool output from a subagent's parent session", () =>
    Effect.gen(function* () {
      const seeded = yield* seed()
      const info = yield* HistoryTool
      const tool = yield* info.init()

      const search = yield* tool.execute({ query: "secret_port=\\d+" }, ctx(seeded.child.id))
      expect(search.metadata.matches).toBe(1)
      expect(search.output).toContain(`[${seeded.assistant.id}] assistant bash output:`)
      expect(search.output).toContain("SECRET_PORT=4821")

      const read = yield* tool.execute({ messageID: seeded.assistant.id }, ctx(seeded.child.id))
      expect(read.output).toContain('--- bash input\n{"command":"cat .env"}')
      expect(read.output).toContain("HOST=localhost\nSECRET_PORT=4821\nMODE=dev")

      const user = yield* tool.execute({ query: "blue-\\d" }, ctx(seeded.parent.id))
      expect(user.output).toContain("user text: Deploy to the blue-7 cluster")

      expect((yield* tool.execute({ query: "no such thing" }, ctx(seeded.parent.id))).output).toBe("No matches")
    }),
  )

  enabled.instance("registers the tool and the recall subagent", () =>
    Effect.gen(function* () {
      const agents = yield* Agent.Service
      const build = yield* agents.get("build")
      const registry = yield* ToolRegistry.Service
      const tools = yield* registry.tools({ ...ref, agent: build })
      expect(tools.some((tool) => tool.id === "history")).toBe(true)
      expect((yield* agents.get("recall"))?.mode).toBe("subagent")
    }),
  )

  disabled.instance("stays off without the flag", () =>
    Effect.gen(function* () {
      const agents = yield* Agent.Service
      const build = yield* agents.get("build")
      const registry = yield* ToolRegistry.Service
      const tools = yield* registry.tools({ ...ref, agent: build })
      expect(tools.some((tool) => tool.id === "history")).toBe(false)
      expect(yield* agents.get("recall")).toBeUndefined()
    }),
  )
})

const assistantInfo = (sessionID: SessionID, parentID: MessageID) => ({
  id: MessageID.ascending(),
  role: "assistant" as const,
  parentID,
  sessionID,
  mode: "build",
  agent: "build",
  cost: 0,
  path: { cwd: "/tmp", root: "/tmp" },
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  modelID: ref.modelID,
  providerID: ref.providerID,
  time: { created: Date.now() },
})

// One user message with a text part, in the given session.
const say = Effect.fn("HistoryToolTest.say")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const user = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const part = yield* session.updatePart({
    id: PartID.ascending(),
    messageID: user.id,
    sessionID,
    type: "text",
    text,
  })
  return { user, part }
})

const search = Effect.fn("HistoryToolTest.search")(function* (
  sessionID: SessionID,
  params: { query?: string; messageID?: string; scope?: "session" | "project"; limit?: number },
  chunk?: number,
) {
  const info = yield* HistoryTool
  const tool = yield* info.init()
  return yield* tool.execute(params, chunk ? { ...ctx(sessionID), extra: { historyChunk: chunk } } : ctx(sessionID))
})

describe("tool.history requiredLiterals", () => {
  const cases: [string, string[] | undefined][] = [
    ["secret_port=\\d+", ["secret_port="]],
    ["foo_bar|secret", ["foo_bar", "secret"]],
    ["foo_bar|ab", undefined],
    ["\\d+", undefined],
    ["a.b", undefined],
    ["colou?r", ["colo"]],
    ["colour?ful", ["colou"]],
    ["(abc)+def", ["def"]],
    ["(?=foo)foobar", undefined],
    ["(foo)\\1bar", undefined],
    ["[abc]xyz", ["xyz"]],
    ["ab{2,3}cdef", ["cdef"]],
    ["abc\\.def", ["abc.def"]],
    ["abcd{x|yz}", ["abcd{x", "yz}"]],
    ["a{2}bcd", ["bcd"]],
    ["ab{,3}cd", ["ab{,3}cd"]],
    ["xyz{2,}|abc", undefined],
    ["a{1,2", ["a{1,2"]],
    ["caf\u00e9bar", ["caf"]],
    ['say "hi"', ["say "]],
    ["héllo wörld", ["llo w"]],
  ]
  for (const [pattern, expected] of cases) {
    test(`extracts from ${pattern}`, () => {
      expect(requiredLiterals(pattern)).toEqual(expected)
    })
  }

  test("every extracted literal is required by any match (randomized)", () => {
    const random = prng(7)
    const pick = <T>(items: T[]) => items[Math.floor(random() * items.length)]
    const atoms = [
      "abc",
      "xyz",
      "a-b",
      "_1%",
      "abc",
      "a",
      "b",
      "c",
      "x",
      "y",
      "_",
      "-",
      "1",
      "%",
      "\\.",
      "\\\\",
      ".",
      "[a-c]",
      "[^b]",
      "\\d",
      "\\w",
      "\\b",
      "^",
      "$",
      " ",
    ]
    const quantifiers = ["", "", "", "*", "+", "?", "{2}", "{1,3}", "*?", "+?", "{0,1}"]
    const piece = (depth: number): string => {
      if (depth < 2 && random() < 0.2) {
        const inner = Array.from({ length: 1 + Math.floor(random() * 3) }, () => piece(depth + 1)).join(pick(["", "|"]))
        return pick([`(${inner})`, `(?:${inner})`, `(?=${inner})`, `(?<!${inner})`]) + pick(quantifiers)
      }
      return pick(atoms) + pick(quantifiers)
    }
    const alphabet = [
      "{",
      "}",
      "|",
      "a",
      "b",
      "c",
      "x",
      "y",
      "_",
      "-",
      "1",
      "%",
      ".",
      "\\",
      " ",
      "A",
      "B",
      "é",
      "\n",
      '"',
    ]
    let covered = 0
    for (let i = 0; i < 30_000; i++) {
      const source = Array.from({ length: 1 + Math.floor(random() * 3) }, () =>
        Array.from({ length: 1 + Math.floor(random() * 6) }, () => piece(0)).join(""),
      ).join(random() < 0.3 ? "|" : "")
      const literals = requiredLiterals(source)
      if (!literals) continue
      const regex = (() => {
        try {
          return new RegExp(source, "i")
        } catch {
          return undefined
        }
      })()
      if (!regex) continue
      covered++
      for (let j = 0; j < 40; j++) {
        const text = Array.from({ length: Math.floor(random() * 14) }, () => pick(alphabet)).join("")
        if (!regex.test(text)) continue
        const folded = text.replace(/[A-Z]/g, (char) => char.toLowerCase())
        expect(
          literals.some((literal) => folded.includes(literal.replace(/[A-Z]/g, (char) => char.toLowerCase()))),
        ).toBe(true)
      }
    }
    expect(covered).toBeGreaterThan(100)
  })
})

function prng(seed: number) {
  let state = seed
  return () => {
    state = (state + 0x6d2b79f5) | 0
    let value = Math.imul(state ^ (state >>> 15), 1 | state)
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296
  }
}

describe("tool.history search", () => {
  const phrases = [
    "Foo_Bar baz",
    "SECRET_PORT=4821",
    "100% sure_thing",
    'she said "hello" and left',
    "C:\\Users\\dev\\file.txt",
    "line one\nline two\n\nline three",
    "héllo wörld ünï",
    "emoji 😀 party",
    "a.b(c)[d]{e}|f",
    "colour color colouur",
    "ERROR: failed to connect to db-7",
    "  indented\ttabbed  ",
    "xxx yyy zzz",
    "abcabcabc d",
  ]

  // Builds a transcript of random parts of every kind, newest last.
  const populate = Effect.fn("HistoryToolTest.populate")(function* (
    sessionID: SessionID,
    random: () => number,
    size: number,
  ) {
    const session = yield* Session.Service
    const pick = <T>(items: T[]) => items[Math.floor(random() * items.length)]
    const words = () => Array.from({ length: 1 + Math.floor(random() * 4) }, () => pick(phrases)).join(" ")
    const user = (yield* say(sessionID, words())).user
    const assistant = yield* session.updateMessage(assistantInfo(sessionID, user.id))
    for (let i = 0; i < size; i++) {
      const base = { id: PartID.ascending(), messageID: assistant.id, sessionID }
      const kind = Math.floor(random() * 5)
      if (kind === 0) yield* session.updatePart({ ...base, type: "text", text: words() })
      if (kind === 1) yield* session.updatePart({ ...base, type: "reasoning", text: words(), time: { start: 1 } })
      if (kind === 2)
        yield* session.updatePart({
          ...base,
          type: "tool",
          tool: pick(["bash", "read"]),
          callID: `c${i}`,
          state: {
            status: "completed",
            input: { command: words(), path: pick(phrases) },
            output: words(),
            title: "t",
            metadata: {},
            time: { start: 1, end: 2 },
          },
        })
      if (kind === 3)
        yield* session.updatePart({
          ...base,
          type: "tool",
          tool: "bash",
          callID: `c${i}`,
          state: { status: "error", input: { command: words() }, error: words(), time: { start: 1, end: 2 } },
        })
      if (kind === 4)
        yield* session.updatePart({
          ...base,
          type: "tool",
          tool: "bash",
          callID: `c${i}`,
          state: { status: "running", input: { command: words() }, time: { start: 1 } },
        })
    }
  })

  // The reference implementation: load every message and part of the sessions, newest part first,
  // and run the regex over each entry.
  const naive = Effect.fn("HistoryToolTest.naive")(function* (sessionIDs: SessionID[], query: string) {
    const session = yield* Session.Service
    const pattern = new RegExp(query, "i")
    const rows = (yield* Effect.forEach(sessionIDs, (sessionID) => session.messages({ sessionID })))
      .flat()
      .flatMap((message) =>
        message.parts.map((part) => ({ role: message.info.role, messageID: message.info.id, part })),
      )
    return rows
      .toSorted((a, b) => (a.part.id < b.part.id ? 1 : -1))
      .flatMap((row) => {
        const part: SessionV1.Part = row.part
        const items =
          part.type === "text" || part.type === "reasoning"
            ? [["" + part.type, part.text]]
            : part.type !== "tool"
              ? []
              : part.state.status === "completed"
                ? [
                    [`${part.tool} input`, JSON.stringify(part.state.input)],
                    [`${part.tool} output`, part.state.output],
                  ]
                : part.state.status === "error"
                  ? [
                      [`${part.tool} input`, JSON.stringify(part.state.input)],
                      [`${part.tool} error`, part.state.error],
                    ]
                  : [[`${part.tool} input`, JSON.stringify(part.state.input)]]
        return items.filter((item) => pattern.test(item[1])).map((item) => `${row.messageID} ${row.role} ${item[0]}`)
      })
  })

  const lines = (output: string) =>
    [...output.matchAll(/^\[(msg_[A-Za-z0-9]+)\] (\w+) ([^:\n]+?)(?: \[session [^\]]*\])?: /gm)].map(
      (match) => `${match[1]} ${match[2]} ${match[3]}`,
    )

  const patterns = (random: () => number) => {
    const fixed = [
      "foo_bar",
      "FOO_BAR",
      "secret_port=\\d+",
      "100%",
      "sure_thing",
      "100% sure",
      '"hello"',
      'said "hello"',
      "C:\\\\Users",
      "Users\\\\dev",
      "line two",
      "line one.line two",
      "one\\ntwo",
      "héllo",
      "HÉLLO",
      "wörld",
      "😀",
      "a\\.b\\(c\\)",
      "a.b",
      "\\[d\\]\\{e\\}",
      "colou?r",
      "colour|color",
      "colour+",
      "colou{2}r",
      "error: failed|db-7",
      "db-\\d",
      "zzz|xxx yyy",
      "xx|yy",
      "^line",
      "ndented",
      "\\ttabbed",
      "(abc)+ d",
      "abcabc",
      "(foo)_\\1",
      "(?=foo)foo_bar",
      "[a-z]+_bar",
      "baz$",
      "\\bbaz\\b",
      "sure_|%",
      "_",
      "%%%",
      "nothing-matches-this",
      "ab+c",
      "x*yyy",
      "zz?z",
      "ERROR: .* db",
      "he said|SECRET",
      "100%|xyz",
      "e|ll",
      "abc\\|",
      "f\\|",
      "{e}",
    ]
    const generated = Array.from({ length: 40 }, () => {
      const phrase = phrases[Math.floor(random() * phrases.length)]
      const start = Math.floor(random() * phrase.length)
      const piece = phrase.slice(start, start + 2 + Math.floor(random() * 7))
      const escaped = random() < 0.5 ? piece.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") : piece
      return random() < 0.2 ? `${escaped}|${phrases[Math.floor(random() * phrases.length)].slice(0, 4)}` : escaped
    })
    return [...fixed, ...generated].filter((query) => {
      try {
        new RegExp(query, "i")
        return true
      } catch {
        return false
      }
    })
  }

  enabled.instance(
    "prefiltered search matches a naive full scan for any pattern",
    () =>
      Effect.gen(function* () {
        const session = yield* Session.Service
        const parent = yield* session.create({ title: "parent" })
        const child = yield* session.create({ title: "child", parentID: parent.id })
        const random = prng(42)
        yield* populate(parent.id, random, 60)
        yield* populate(child.id, random, 60)
        let prefiltered = 0
        for (const query of patterns(random)) {
          if (requiredLiterals(query)) prefiltered++
          const expected = yield* naive([child.id, parent.id], query)
          const result = yield* search(child.id, { query, limit: 10_000 })
          expect({ query, hits: lines(result.output) }).toEqual({ query, hits: expected })
          const first = yield* search(child.id, { query, limit: 3 })
          expect({ query, hits: lines(first.output) }).toEqual({ query, hits: expected.slice(0, 3) })
        }
        expect(prefiltered).toBeGreaterThan(30)
      }),
    120_000,
  )

  // Project scope is the same exact scan as session scope over the project's sessions; ranges of any
  // size, including ones that split hits across boundaries, must give the same answer.
  for (const chunk of [undefined, 1, 7, 50])
    enabled.instance(
      `project scope matches a naive scan of the project (range size ${chunk ?? "default"})`,
      () =>
        Effect.gen(function* () {
          const session = yield* Session.Service
          const old = yield* session.create({ title: "old sibling" })
          const other = yield* session.create({ title: "other sibling" })
          const gone = yield* session.create({ title: "deleted" })
          const current = yield* session.create({ title: "current" })
          const random = prng(99)
          yield* populate(old.id, random, 40)
          yield* populate(other.id, random, 40)
          yield* populate(gone.id, random, 40)
          yield* populate(current.id, random, 40)
          yield* session.remove(gone.id)
          // Rewrite an old part in place: it keeps its position but its content changes.
          const rewritten = yield* session.messages({ sessionID: old.id })
          const target = rewritten.flatMap((message) => message.parts).find((part) => part.type === "text")!
          if (target.type === "text")
            yield* session.updatePart({ ...target, text: `${target.text} rewritten-zebra-11` })
          const projectSessions = [current.id, old.id, other.id]
          for (const query of [...patterns(random), "rewritten-zebra-\\d+"]) {
            const expected = yield* naive(projectSessions, query)
            const result = yield* search(current.id, { query, scope: "project", limit: 10_000 }, chunk)
            expect({ query, hits: lines(result.output) }).toEqual({ query, hits: expected })
            // Stopping partway through a range keeps the newest hits.
            const first = yield* search(current.id, { query, scope: "project", limit: 3 }, chunk)
            expect({ query, hits: lines(first.output) }).toEqual({ query, hits: expected.slice(0, 3) })
          }
        }),
      120_000,
    )

  enabled.instance("project scope finds older sessions that session scope cannot", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const old = yield* session.create({ title: "migration work" })
      const said = yield* say(old.id, "The staging database is pg-orbit-31")
      const current = yield* session.create({ title: "today" })
      yield* say(current.id, "unrelated chatter")

      expect((yield* search(current.id, { query: "pg-orbit-\\d+" })).output).toBe("No matches")
      const found = yield* search(current.id, { query: "pg-orbit-\\d+", scope: "project" })
      expect(found.metadata.matches).toBe(1)
      expect(found.output).toContain(
        `[${said.user.id}] user text [session ${old.id} "migration work"]: The staging database is pg-orbit-31`,
      )

      const read = yield* search(current.id, { messageID: said.user.id, scope: "project" })
      expect(read.output).toContain("--- text\nThe staging database is pg-orbit-31")
      expect((yield* search(current.id, { messageID: said.user.id })).output).toBe(
        `No message ${said.user.id} in this session`,
      )
    }),
  )

  enabled.instance("project scope finds parts that change after they were first seen", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const old = yield* session.create({ title: "old" })
      const said = yield* say(old.id, "the code word is tangerine")
      const current = yield* session.create({ title: "current" })
      expect((yield* search(current.id, { query: "tangerine", scope: "project" })).metadata.matches).toBe(1)

      yield* session.updatePart({ ...said.part, text: "the code word is persimmon" })
      expect((yield* search(current.id, { query: "tangerine", scope: "project" })).output).toBe("No matches")
      expect((yield* search(current.id, { query: "persimmon", scope: "project" })).metadata.matches).toBe(1)

      // A part that was still running when first seen is picked up once it completes.
      const assistant = yield* session.updateMessage(assistantInfo(old.id, said.user.id))
      const base = {
        id: PartID.ascending(),
        messageID: assistant.id,
        sessionID: old.id,
        type: "tool" as const,
        tool: "bash",
        callID: "late",
      }
      const running = yield* session.updatePart({
        ...base,
        state: { status: "running", input: { command: "make" }, time: { start: 1 } },
      })
      expect((yield* search(current.id, { query: "kumquat", scope: "project" })).output).toBe("No matches")
      yield* session.updatePart({
        ...running,
        state: {
          status: "completed",
          input: { command: "make" },
          output: "built kumquat",
          title: "make",
          metadata: {},
          time: { start: 1, end: 2 },
        },
      })
      expect((yield* search(current.id, { query: "kumquat", scope: "project" })).metadata.matches).toBe(1)
    }),
  )

  enabled.instance("project scope excludes deleted sessions", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const gone = yield* session.create({ title: "gone" })
      yield* say(gone.id, "ephemeral marker zebra-9")
      const kept = yield* session.create({ title: "kept" })
      yield* say(kept.id, "durable marker zebra-8")
      const current = yield* session.create({ title: "current" })
      expect((yield* search(current.id, { query: "zebra-\\d", scope: "project" })).metadata.matches).toBe(2)

      yield* session.remove(gone.id)
      const after = yield* search(current.id, { query: "zebra-\\d", scope: "project" })
      expect(after.metadata.matches).toBe(1)
      expect(after.output).toContain("zebra-8")
      expect(after.output).not.toContain("zebra-9")
    }),
  )

  enabled.instance("project scope searches without a literal, and says when it is incomplete", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const old = yield* session.create({ title: "old" })
      yield* say(old.id, "build 4417 finished")
      const current = yield* session.create({ title: "current" })
      const result = yield* search(current.id, { query: "\\d{4}", scope: "project" })
      expect(result.metadata.matches).toBe(1)
      expect(result.output).toContain("build 4417 finished")
      expect(result.output).not.toContain("incomplete")
      expect((yield* search(current.id, { query: "\\d{4}" })).output).toBe("No matches")
    }),
  )
})
