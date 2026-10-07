import { afterAll, afterEach, describe, expect } from "bun:test"
import { $ } from "bun"
import fs from "fs"
import os from "os"
import path from "path"
import { Effect, Exit, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Global } from "@opencode-ai/core/global"
import { Agent } from "@/agent/agent"
import { Command } from "@/command"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Memory } from "@/memory"
import { Permission } from "@/permission"
import { MessageID, SessionID } from "@/session/schema"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { ToolRegistry } from "@/tool/registry"
import { disposeAllInstances, provideInstance, testInstanceStoreLayer, TestInstance } from "../fixture/fixture"
import { TestConfig } from "../fixture/config"
import { testEffect } from "../lib/effect"

const data = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-memory-"))
const dataOff = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-memory-off-"))

afterAll(() => {
  fs.rmSync(data, { recursive: true, force: true })
  fs.rmSync(dataOff, { recursive: true, force: true })
})

afterEach(async () => {
  await disposeAllInstances()
})

const layer = (dir: string, flags: Partial<RuntimeFlags.Info>) =>
  AppNodeBuilder.build(Memory.node, [
    [Global.node, Global.layerWith({ data: dir, state: path.join(dir, "state") })],
    [RuntimeFlags.node, RuntimeFlags.layer(flags)],
  ])

const on = testEffect(layer(data, { experimentalMemory: true }))
const off = testEffect(layer(dataOff, {}))

// Non-git instances all share the "global" project, so give every test its own git repo (and project ID).
const inst = (name: string, value: Parameters<typeof on.instance>[1], timeout = 30_000) =>
  on.instance(name, value, { git: true }, timeout)

const build = {
  name: "build",
  mode: "primary",
  permission: Permission.fromConfig({ "*": "allow" }),
  options: {},
} as const
const explore = { ...build, name: "explore", mode: "subagent" } as const

const entry = (name: string) => ({
  name,
  type: "feedback" as const,
  description: `description of ${name}`,
  content: `body of ${name}`,
})

describe("memory", () => {
  inst("save, view and delete keep the index in sync", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const root = yield* memory.dir()

      const first = yield* memory.save({ ...entry("testing"), description: "run tests\nfrom package dirs" })
      yield* memory.save(entry("style"))
      expect(first.file).toBe(path.join(root, "testing.md"))
      expect(first.warning).toBeUndefined()

      expect(yield* memory.view()).toBe(
        "- [testing](testing.md) — run tests from package dirs\n- [style](style.md) — description of style\n",
      )
      const topic = yield* memory.view("testing")
      expect(topic).toStartWith("---\nname: testing\ndescription: run tests from package dirs\ntype: feedback\n---\n")
      expect(topic).toContain("body of testing")

      // replacing keeps one line
      yield* memory.save({ ...entry("testing"), description: "updated", content: "new body" })
      expect(yield* memory.view()).toBe(
        "- [testing](testing.md) — updated\n- [style](style.md) — description of style\n",
      )
      expect(yield* memory.view("testing")).toContain("new body")

      yield* memory.remove("testing")
      expect(yield* memory.view()).toBe("- [style](style.md) — description of style\n")
      expect(fs.existsSync(path.join(root, "testing.md"))).toBe(false)
      const missing = yield* memory.view("testing").pipe(Effect.flip)
      expect(missing._tag).toBe("Memory.NotFoundError")
      expect((yield* memory.remove("testing").pipe(Effect.flip))._tag).toBe("Memory.NotFoundError")

      yield* memory.remove("style")
      expect(yield* memory.view()).toStartWith("Memory is empty")
    }),
  )

  inst("rejects names that could escape the memory directory", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      for (const name of [
        "../evil",
        "a/b",
        "/etc/passwd",
        "..",
        "a..b",
        "",
        ".hidden",
        "MEMORY",
        "memory.md",
        "a\\b",
      ]) {
        const exit = yield* memory.save(entry(name)).pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        expect((yield* memory.view(name).pipe(Effect.flip))._tag).toBe("Memory.InvalidNameError")
        expect((yield* memory.remove(name).pipe(Effect.flip))._tag).toBe("Memory.InvalidNameError")
      }
      expect(fs.existsSync(path.join(path.dirname(yield* memory.dir()), "evil.md"))).toBe(false)
      yield* memory.save(entry("ok.md"))
      expect(yield* memory.view("ok")).toContain("body of ok")
    }),
  )

  inst("loads at most 200 lines of the index", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const root = yield* memory.dir()
      fs.mkdirSync(root, { recursive: true })
      fs.writeFileSync(
        path.join(root, "MEMORY.md"),
        Array.from({ length: 250 }, (_, i) => `- [m${i}](m${i}.md) — d`).join("\n") + "\n",
      )
      const index = yield* memory.index()
      expect(index?.truncated).toBe(true)
      expect(index?.content.split("\n")).toHaveLength(200)
      expect(index?.content).toContain("m199")
      expect(index?.content).not.toContain("m200]")

      const section = yield* memory.system(build, SessionID.make("ses_lines"))
      expect(section).toContain("m199")
      expect(section).not.toContain("m200]")
      expect(section).toContain("Truncated")
    }),
  )

  inst("loads at most 25 KB of the index", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const root = yield* memory.dir()
      fs.mkdirSync(root, { recursive: true })
      const line = "x".repeat(1023)
      fs.writeFileSync(path.join(root, "MEMORY.md"), Array.from({ length: 40 }, () => line).join("\n") + "\n")
      const index = yield* memory.index()
      expect(index?.truncated).toBe(true)
      expect(Buffer.byteLength(index?.content ?? "")).toBeLessThanOrEqual(Memory.MAX_BYTES)
      expect(index?.content.split("\n")).toHaveLength(25)
    }),
  )

  inst("an index within the limits is not truncated", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      expect(yield* memory.index()).toBeUndefined()
      yield* memory.save(entry("one"))
      const index = yield* memory.index()
      expect(index).toEqual({ content: "- [one](one.md) — description of one", truncated: false })
    }),
  )

  inst("warns near a limit and errors over it while still writing", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const root = yield* memory.dir()
      fs.mkdirSync(root, { recursive: true })
      const seed = (count: number) =>
        fs.writeFileSync(
          path.join(root, "MEMORY.md"),
          Array.from({ length: count }, (_, i) => `- [m${i}](m${i}.md) — d`).join("\n") + "\n",
        )

      seed(10)
      expect((yield* memory.save(entry("a"))).warning).toBeUndefined()

      seed(165)
      const near = yield* memory.save(entry("b"))
      expect(near.usage.near).toBe(true)
      expect(near.usage.over).toBe(false)
      expect(near.warning).toStartWith("Warning:")
      expect(near.warning).toContain("Shorten")

      seed(200)
      const over = yield* memory.save(entry("c"))
      expect(over.usage.over).toBe(true)
      expect(over.warning).toStartWith("Error:")
      expect(over.warning).toContain("dropped")
      // the write still succeeded
      expect(yield* memory.view("c")).toContain("body of c")
      expect(yield* memory.view()).toContain("[c](c.md)")

      // size limit applies to bytes as well
      yield* memory.remove("c")
      yield* memory.remove("b")
      yield* memory.remove("a")
      const long = yield* memory.save({ ...entry("d"), description: "y".repeat(Memory.MAX_BYTES) })
      expect(long.warning).toStartWith("Error:")
    }),
  )

  inst("concurrent saves do not lose index lines", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const names = Array.from({ length: 12 }, (_, i) => `entry-${i}`)
      yield* Effect.forEach(names, (name) => memory.save(entry(name)), { concurrency: "unbounded" })
      const lines = (yield* memory.view()).trim().split("\n")
      expect(lines.toSorted()).toEqual(
        names.map((name) => `- [${name}](${name}.md) — description of ${name}`).toSorted(),
      )
    }),
  )

  inst("worktrees of one repo share a memory directory", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const main = yield* memory.dir()
      const dir = (yield* TestInstance).directory
      const worktree = path.join(os.tmpdir(), `opencode-memory-wt-${process.pid}-${Date.now()}`)
      yield* Effect.promise(() => $`git worktree add ${worktree} -b memory-wt`.cwd(dir).quiet())
      yield* memory.save(entry("shared"))
      const other = yield* Effect.gen(function* () {
        expect(yield* memory.dir()).toBe(main)
        return yield* memory.view("shared")
      }).pipe(provideInstance(worktree), Effect.provide(testInstanceStoreLayer))
      expect(other).toContain("body of shared")
      fs.rmSync(worktree, { recursive: true, force: true })
    }),
  )

  inst("system section is shown to primary agents only, snapshotted per session", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const root = yield* memory.dir()
      const sessionID = SessionID.make("ses_primary")

      const empty = yield* memory.system(build, sessionID)
      expect(empty).toContain(root)
      expect(empty).toContain("currently empty")
      expect(empty).toContain("feedback")
      expect(empty).toContain("MEMORY.md")

      yield* memory.save(entry("later"))
      // same session keeps the snapshot so the system prompt stays stable for caching
      expect(yield* memory.system(build, sessionID)).toBe(empty)
      const fresh = yield* memory.system(build, SessionID.make("ses_other"))
      expect(fresh).toContain("- [later](later.md) — description of later")
      expect(fresh).not.toContain("currently empty")

      expect(yield* memory.system(explore, SessionID.make("ses_sub"))).toBeUndefined()
    }),
  )

  inst("hand-edited indexes keep blank lines, other lines and CRLF; duplicates collapse", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const root = yield* memory.dir()
      fs.mkdirSync(root, { recursive: true })
      const indexFile = path.join(root, "MEMORY.md")
      fs.writeFileSync(
        indexFile,
        [
          "# My notes",
          "",
          "- [a](a.md) — old",
          "free text",
          "- [a](a.md) — duplicate",
          "",
          "- [b](b.md) — supersedes [a](a.md)",
        ].join("\r\n") + "\r\n",
      )
      yield* memory.save({ ...entry("a"), description: "new" })
      expect(fs.readFileSync(indexFile, "utf8")).toBe(
        ["# My notes", "", "- [a](a.md) — new", "free text", "", "- [b](b.md) — supersedes [a](a.md)"].join("\r\n") +
          "\r\n",
      )
      yield* memory.remove("a")
      // the line that merely links to a is untouched
      expect(fs.readFileSync(indexFile, "utf8")).toBe(
        ["# My notes", "", "free text", "", "- [b](b.md) — supersedes [a](a.md)"].join("\r\n") + "\r\n",
      )
    }),
  )

  inst("an entry that links to another memory is not replaced when the other is saved or removed", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      yield* memory.save({ ...entry("b"), description: "supersedes [a](a.md)" })
      yield* memory.save(entry("a"))
      yield* memory.save({ ...entry("a"), description: "again" })
      expect(yield* memory.view()).toBe("- [b](b.md) — supersedes [a](a.md)\n- [a](a.md) — again\n")
      yield* memory.remove("a")
      expect(yield* memory.view()).toBe("- [b](b.md) — supersedes [a](a.md)\n")
    }),
  )

  inst("names are lowercase so case-insensitive filesystems cannot alias two memories", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      expect((yield* memory.save(entry("Foo")).pipe(Effect.flip))._tag).toBe("Memory.InvalidNameError")
      expect((yield* memory.save(entry("memory")).pipe(Effect.flip))._tag).toBe("Memory.InvalidNameError")
      yield* memory.save(entry("foo"))
      expect((yield* memory.view("FOO").pipe(Effect.flip))._tag).toBe("Memory.InvalidNameError")
    }),
  )

  inst("save without content keeps the body of an existing memory", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      expect((yield* memory.save({ ...entry("x"), content: undefined }).pipe(Effect.flip))._tag).toBe(
        "Memory.ContentRequiredError",
      )
      yield* memory.save({ ...entry("x"), content: "keep me\n\nsecond paragraph" })
      yield* memory.save({ name: "x", type: "project", description: "short" })
      const topic = yield* memory.view("x")
      expect(topic).toContain("type: project")
      expect(topic).toContain("description: short")
      expect(topic).toEndWith("---\n\nkeep me\n\nsecond paragraph\n")
      expect(yield* memory.view()).toBe("- [x](x.md) — short\n")
    }),
  )

  inst("limit boundaries are exact, with or without a trailing newline, and count bytes", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const root = yield* memory.dir()
      fs.mkdirSync(root, { recursive: true })
      const write = (content: string) => fs.writeFileSync(path.join(root, "MEMORY.md"), content)
      const exact = Array.from({ length: 25 }, (_, i) => "x".repeat(i === 24 ? 1023 - 24 + 24 : 1023)).join("\n")
      expect(Buffer.byteLength(exact)).toBeLessThan(Memory.MAX_BYTES)
      // pad the last line so the index is exactly 25,600 bytes without a trailing newline
      const full = exact + "x".repeat(Memory.MAX_BYTES - Buffer.byteLength(exact))
      expect(Buffer.byteLength(full)).toBe(Memory.MAX_BYTES)

      for (const content of [full, full + "\n"]) {
        write(content)
        expect(Memory.usage(content).over).toBe(false)
        expect((yield* memory.index())?.truncated).toBe(false)
        expect(Buffer.byteLength((yield* memory.index())?.content ?? "")).toBe(Memory.MAX_BYTES)
      }
      for (const content of [full + "x", full + "x\n"]) {
        write(content)
        expect(Memory.usage(content).over).toBe(true)
        const index = yield* memory.index()
        expect(index?.truncated).toBe(true)
        expect(index?.content.split("\n")).toHaveLength(24)
      }

      // exactly 200 lines is fine, with or without a trailing newline; 201 is truncated
      const lines = (n: number) => Array.from({ length: n }, (_, i) => `l${i}`).join("\n")
      for (const content of [lines(200), lines(200) + "\n"]) {
        write(content)
        expect(Memory.usage(content).over).toBe(false)
        expect((yield* memory.index())?.truncated).toBe(false)
      }
      write(lines(201))
      expect((yield* memory.index())?.truncated).toBe(true)

      // multi-byte characters count as bytes, not characters
      const wide = Array.from({ length: 10 }, () => "é".repeat(1500)).join("\n")
      write(wide)
      expect(wide.length).toBeLessThan(Memory.MAX_BYTES)
      expect(Memory.usage(wide).over).toBe(true)
      const index = yield* memory.index()
      expect(index?.truncated).toBe(true)
      expect(Buffer.byteLength(index?.content ?? "")).toBeLessThanOrEqual(Memory.MAX_BYTES)
      expect(index?.content.split("\n")).toHaveLength(8)
    }),
  )

  inst("paths with replacement patterns are inserted literally", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const section = yield* memory.system(build, SessionID.make("ses_dollar"))
      expect(section).toContain(yield* memory.dir())
      expect(section).not.toContain("${")
    }),
  )

  inst("the snapshot map is bounded", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      const ids = Array.from({ length: 230 }, (_, i) => SessionID.make(`ses_cap${i}`))
      const first = yield* memory.system(build, ids[0])
      yield* Effect.forEach(ids.slice(1), (id) => memory.system(build, id))
      yield* memory.save(entry("late"))
      // the oldest snapshot was evicted, so it is rebuilt with the new entry; recent ones are kept
      expect(first).not.toContain("late.md")
      expect(yield* memory.system(build, ids[0])).toContain("late.md")
      expect(yield* memory.system(build, ids[229])).not.toContain("late.md")
    }),
  )

  on.instance("non-git folders have no memory", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      expect(yield* memory.system(build, SessionID.make("ses_nogit"))).toBeUndefined()
      expect((yield* memory.save(entry("x")).pipe(Effect.flip))._tag).toBe("Memory.UnavailableError")
      expect((yield* memory.view().pipe(Effect.flip))._tag).toBe("Memory.UnavailableError")
      expect((yield* memory.remove("x").pipe(Effect.flip))._tag).toBe("Memory.UnavailableError")
      expect(fs.existsSync(path.join(data, "memory", "global"))).toBe(false)
    }),
  )

  off.instance("flag off: no section and no directory", () =>
    Effect.gen(function* () {
      const memory = yield* Memory.Service
      expect(yield* memory.system(build, SessionID.make("ses_off"))).toBeUndefined()
      expect(fs.existsSync(path.join(dataOff, "memory"))).toBe(false)
    }),
  )
})

const configLayer = TestConfig.layer({})
const compile = (flags: Partial<RuntimeFlags.Info>) =>
  testEffect(
    LayerNode.compile(LayerNode.group([ToolRegistry.node, Agent.node, Command.node]), [
      [Config.node, configLayer],
      [RuntimeFlags.node, RuntimeFlags.layer(flags)],
      [Global.node, Global.layerWith({ data: dataOff, state: path.join(dataOff, "state") })],
    ]),
  )

const tool = Effect.gen(function* () {
  const registry = yield* ToolRegistry.Service
  const found = (yield* registry.all()).find((item) => item.id === "memory")
  if (!found) return yield* Effect.die(new Error("memory tool missing"))
  return found
})

const toolCtx = (asked: unknown[]) => ({
  sessionID: SessionID.make("ses_tool"),
  messageID: MessageID.make("msg_tool"),
  callID: "",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: (input: unknown) => Effect.sync(() => void asked.push(input)),
})

describe("memory integration", () => {
  const enabled = compile({ experimentalMemory: true })
  const disabled = compile({})

  enabled.instance(
    "flag on: memory tool and /refine command exist",
    () =>
      Effect.gen(function* () {
        const registry = yield* ToolRegistry.Service
        const commands = yield* Command.Service
        expect(yield* registry.ids()).toContain("memory")
        const refine = yield* commands.get("refine")
        expect(refine?.hints).toContain("$ARGUMENTS")
        expect(yield* Effect.promise(async () => refine?.template)).toContain("memory")
      }),
    { git: true },
  )

  enabled.instance("/refine says memory needs a git project outside git", () =>
    Effect.gen(function* () {
      const commands = yield* Command.Service
      const refine = yield* commands.get("refine")
      expect(yield* Effect.promise(async () => refine?.template)).toContain("requires a git project")
    }),
  )

  disabled.instance(
    "flag off: no memory tool, no /refine command, no files",
    () =>
      Effect.gen(function* () {
        const registry = yield* ToolRegistry.Service
        const commands = yield* Command.Service
        expect(yield* registry.ids()).not.toContain("memory")
        expect(yield* commands.get("refine")).toBeUndefined()
        expect(fs.existsSync(path.join(dataOff, "memory"))).toBe(false)
      }),
    { git: true },
  )

  enabled.instance(
    "subagents are not offered the memory tool",
    () =>
      Effect.gen(function* () {
        const registry = yield* ToolRegistry.Service
        const agents = yield* Agent.Service
        const input = { providerID: ProviderV2.ID.opencode, modelID: ModelV2.ID.make("test") }
        const primary = yield* registry.tools({ ...input, agent: yield* agents.defaultInfo() })
        const subagent = yield* registry.tools({
          ...input,
          agent: { ...(yield* agents.defaultInfo()), mode: "subagent" },
        })
        expect(primary.map((item) => item.id)).toContain("memory")
        expect(subagent.map((item) => item.id)).not.toContain("memory")
      }),
    { git: true },
  )

  enabled.instance(
    "tool execute: save, view and delete, asking the memory permission",
    () =>
      Effect.gen(function* () {
        const memory = yield* tool
        const asked: unknown[] = []
        const ctx = toolCtx(asked)

        const saved = yield* memory.execute(
          { action: "save", name: "tool-test", type: "user", description: "d", content: "c" },
          ctx,
        )
        expect(saved.output).toContain('Saved memory "tool-test"')
        expect((yield* memory.execute({ action: "view" }, ctx)).output).toBe("- [tool-test](tool-test.md) — d\n")
        expect((yield* memory.execute({ action: "view", name: "tool-test" }, ctx)).output).toContain("c")
        const kept = yield* memory.execute({ action: "save", name: "tool-test", type: "user", description: "d2" }, ctx)
        expect(kept.output).not.toContain("Error")
        expect((yield* memory.execute({ action: "view" }, ctx)).output).toBe("- [tool-test](tool-test.md) — d2\n")
        expect((yield* memory.execute({ action: "delete", name: "tool-test" }, ctx)).output).toContain("Deleted")
        expect(asked).toHaveLength(6)
        expect(asked[0]).toMatchObject({ permission: "memory" })

        // invalid calls surface as failures
        const failures = yield* Effect.forEach(
          [
            { action: "save", name: "x", type: "user", description: "d" },
            { action: "save", name: "x", description: "d", content: "c" },
            { action: "save", type: "user", description: "d", content: "c" },
            { action: "delete" },
            { action: "delete", name: "missing" },
            { action: "view", name: "../evil" },
            { action: "bogus" },
          ],
          (args) => memory.execute(args as never, ctx).pipe(Effect.exit),
        )
        expect(failures.every(Exit.isFailure)).toBe(true)
      }),
    { git: true },
  )
})
