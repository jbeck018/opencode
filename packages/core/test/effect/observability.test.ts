import { afterEach, describe, expect, test } from "bun:test"
import { NodeFileSystem } from "@effect/platform-node"
import { Effect, Layer, Logger } from "effect"
import fs from "fs/promises"
import os from "os"
import path from "path"
import { fileLogger, rotate } from "../../src/observability/logging"
import { resource } from "../../src/observability/otlp"

const otelResourceAttributes = process.env.OTEL_RESOURCE_ATTRIBUTES
const opencodeClient = process.env.OPENCODE_CLIENT

afterEach(() => {
  if (otelResourceAttributes === undefined) delete process.env.OTEL_RESOURCE_ATTRIBUTES
  else process.env.OTEL_RESOURCE_ATTRIBUTES = otelResourceAttributes

  if (opencodeClient === undefined) delete process.env.OPENCODE_CLIENT
  else process.env.OPENCODE_CLIENT = opencodeClient
})

describe("resource", () => {
  test("parses and decodes OTEL resource attributes", () => {
    process.env.OTEL_RESOURCE_ATTRIBUTES =
      "service.namespace=anomalyco,team=platform%2Cobservability,label=hello%3Dworld,key%2Fname=value%20here"

    expect(resource().attributes).toMatchObject({
      "service.namespace": "anomalyco",
      team: "platform,observability",
      label: "hello=world",
      "key/name": "value here",
    })
  })

  test("drops OTEL resource attributes when any entry is invalid", () => {
    process.env.OTEL_RESOURCE_ATTRIBUTES = "service.namespace=anomalyco,broken"

    expect(resource().attributes["service.namespace"]).toBeUndefined()
    expect(resource().attributes["opencode.client"]).toBeDefined()
  })

  test("keeps built-in attributes when env values conflict", () => {
    process.env.OPENCODE_CLIENT = "cli"
    process.env.OTEL_RESOURCE_ATTRIBUTES =
      "opencode.client=web,service.instance.id=override,service.namespace=anomalyco"

    expect(resource().attributes).toMatchObject({
      "opencode.client": "cli",
      "service.namespace": "anomalyco",
    })
    expect(resource().attributes["service.instance.id"]).not.toBe("override")
    expect(resource().attributes["opencode.run"]).toMatch(/^[0-9a-f]{8}$/)
  })
})

test("file logger appends concurrent runs with a run on every line", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-log-test-"))
  await using _ = {
    async [Symbol.asyncDispose]() {
      await fs.rm(dir, { recursive: true, force: true })
    },
  }
  const file = path.join(dir, "opencode.log")
  const write = (runID: string) =>
    Effect.forEach(
      Array.from({ length: 50 }, (_, index) => index),
      (index) => Effect.logInfo(`entry-${index}`),
    ).pipe(
      Effect.provide(Logger.layer([fileLogger(file, runID)]).pipe(Layer.provide(NodeFileSystem.layer), Layer.orDie)),
      Effect.scoped,
    )

  await Effect.runPromise(Effect.all([write("run-a"), write("run-b")], { concurrency: "unbounded" }))

  const lines = (await Bun.file(file).text()).trim().split("\n")
  expect(lines).toHaveLength(100)
  expect(lines.filter((line) => line.includes("run=run-a"))).toHaveLength(50)
  expect(lines.filter((line) => line.includes("run=run-b"))).toHaveLength(50)
  expect(lines.every((line) => line.startsWith("timestamp=") && line.includes(" level=INFO "))).toBe(true)
  expect(lines.every((line) => !line.includes(" fiber="))).toBe(true)
  expect(lines.every((line) => !line.startsWith("{"))).toBe(true)
})

test("file logger flattens nested objects", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-log-test-"))
  await using _ = {
    async [Symbol.asyncDispose]() {
      await fs.rm(dir, { recursive: true, force: true })
    },
  }
  const file = path.join(dir, "opencode.log")

  await Effect.logInfo("request complete", {
    request: { method: "GET", timing: { duration: 42 } },
    tags: ["api", "test"],
  }).pipe(
    Effect.annotateLogs({ session: { id: "session-1" } }),
    Effect.provide(Logger.layer([fileLogger(file, "run-a")]).pipe(Layer.provide(NodeFileSystem.layer), Layer.orDie)),
    Effect.scoped,
    Effect.runPromise,
  )

  const line = (await Bun.file(file).text()).trim()
  expect(line).toContain('message="request complete"')
  expect(line).toContain("request.method=GET")
  expect(line).toContain("request.timing.duration=42")
  expect(line).toContain('tags="[\\\"api\\\",\\\"test\\\"]"')
  expect(line).toContain("session.id=session-1")
  expect(line).not.toContain("request={")
})

describe("log rotation", () => {
  const setup = () => fs.mkdtemp(path.join(os.tmpdir(), "opencode-rotate-"))

  test("rotates an oversized file and shifts existing rotations", async () => {
    const dir = await setup()
    const file = path.join(dir, "opencode.log")
    await Bun.write(file, "current-".repeat(20))
    await Bun.write(`${file}.1`, "one")
    await Bun.write(`${file}.2`, "two")

    rotate(file, 100)

    expect(await Bun.file(`${file}.1`).text()).toBe("current-".repeat(20))
    expect(await Bun.file(`${file}.2`).text()).toBe("one")
    expect(await Bun.file(`${file}.3`).text()).toBe("two")
    expect((await fs.readdir(dir)).sort()).toEqual(["opencode.log.1", "opencode.log.2", "opencode.log.3"])
  })

  test("caps rotated files at three", async () => {
    const dir = await setup()
    const file = path.join(dir, "opencode.log")
    await Promise.all([1, 2, 3].map((n) => Bun.write(`${file}.${n}`, `old-${n}`)))
    await Bun.write(file, "x".repeat(200))

    rotate(file, 100)

    expect(await Bun.file(`${file}.2`).text()).toBe("old-1")
    expect(await Bun.file(`${file}.3`).text()).toBe("old-2")
    expect((await fs.readdir(dir)).sort()).toEqual(["opencode.log.1", "opencode.log.2", "opencode.log.3"])
  })

  test("never replaces another in-flight rotation claim from the same process", async () => {
    const dir = await setup()
    const file = path.join(dir, "opencode.log")
    // Another logger in this process claimed a segment and has not finished shifting it into place.
    const other = `${file}.rotating-${process.pid}`
    await Bun.write(other, "other-segment")
    await Bun.write(file, "x".repeat(200))

    rotate(file, 100)

    expect(await Bun.file(other).text()).toBe("other-segment")
    expect(await Bun.file(`${file}.1`).text()).toBe("x".repeat(200))
    expect((await fs.readdir(dir)).sort()).toEqual(["opencode.log.1", `opencode.log.rotating-${process.pid}`])
  })

  test("leaves a small or missing file untouched", async () => {
    const dir = await setup()
    const file = path.join(dir, "opencode.log")
    rotate(file, 100)
    expect(await fs.readdir(dir)).toEqual([])

    await Bun.write(file, "small")
    rotate(file, 100)
    expect(await fs.readdir(dir)).toEqual(["opencode.log"])
  })

  test("fileLogger prunes legacy timestamped logs beyond the newest ten", async () => {
    const dir = await setup()
    const names = Array.from({ length: 13 }, (_, index) => `2025-01-${String(index + 1).padStart(2, "0")}T120000.log`)
    await Promise.all(names.map((name) => Bun.write(path.join(dir, name), "x")))
    await Bun.write(path.join(dir, "keep.log"), "x")

    await Effect.void.pipe(Effect.provide(Logger.layer([fileLogger(path.join(dir, "opencode.log"))])), Effect.runPromise)

    expect((await fs.readdir(dir)).sort()).toEqual([...names.slice(3), "keep.log", "opencode.log"])
  })

  test("fileLogger removes stale rotation claims and keeps fresh ones", async () => {
    const dir = await setup()
    const file = path.join(dir, "opencode.log")
    await Bun.write(`${file}.rotating-111`, "stale")
    await Bun.write(`${file}.rotating-222`, "fresh")
    const hourAgo = new Date(Date.now() - 60 * 60 * 1000)
    await fs.utimes(`${file}.rotating-111`, hourAgo, hourAgo)

    await Effect.void.pipe(Effect.provide(Logger.layer([fileLogger(file)])), Effect.runPromise)

    expect((await fs.readdir(dir)).sort()).toEqual(["opencode.log", "opencode.log.rotating-222"])
  })

  test("rotation and logging never throw when the log directory is read-only", async () => {
    const dir = await setup()
    const file = path.join(dir, "opencode.log")
    await Bun.write(file, "x".repeat(200))
    await Bun.write(`${file}.rotating-111`, "stale")
    const hourAgo = new Date(Date.now() - 60 * 60 * 1000)
    await fs.utimes(`${file}.rotating-111`, hourAgo, hourAgo)
    await fs.chmod(file, 0o400)
    await fs.chmod(dir, 0o500)
    await using _ = {
      async [Symbol.asyncDispose]() {
        await fs.chmod(dir, 0o700)
      },
    }

    expect(() => rotate(file, 100)).not.toThrow()
    await Effect.logInfo("dropped").pipe(
      Effect.provide(Logger.layer([fileLogger(file, "run-a", { maxBytes: 100, checkInterval: "10 millis" })])),
      Effect.andThen(Effect.sleep("50 millis")),
      Effect.runPromise,
    )

    expect((await fs.readdir(dir)).sort()).toEqual(["opencode.log", "opencode.log.rotating-111"])
  })

  test("a long-running logger reopens the path after another process rotates it", async () => {
    const dir = await setup()
    const file = path.join(dir, "opencode.log")

    await Effect.gen(function* () {
      yield* Effect.logInfo("before")
      // Let the batch flush, rotate the file the way another process would, then let the check reopen the path.
      yield* Effect.sleep("1200 millis")
      yield* Effect.promise(() => fs.rename(file, `${file}.1`))
      yield* Effect.sleep("100 millis")
      yield* Effect.logInfo("after")
    }).pipe(
      Effect.provide(Logger.layer([fileLogger(file, "run-a", { checkInterval: "10 millis" })])),
      Effect.scoped,
      Effect.runPromise,
    )

    expect(await Bun.file(`${file}.1`).text()).toContain("message=before")
    expect(await Bun.file(`${file}.1`).text()).not.toContain("message=after")
    expect(await Bun.file(file).text()).toContain("message=after")
    expect(await Bun.file(file).text()).not.toContain("message=before")
  })

  test("a long-running logger rotates its own file once it exceeds the limit", async () => {
    const dir = await setup()
    const file = path.join(dir, "opencode.log")

    await Effect.gen(function* () {
      yield* Effect.forEach(Array.from({ length: 5 }, (_, index) => index), (index) => Effect.logInfo(`before-${index}`))
      // Let the batch flush, then let the check rotate the oversized file and reopen the path.
      yield* Effect.sleep("1300 millis")
      yield* Effect.logInfo("after")
    }).pipe(
      Effect.provide(Logger.layer([fileLogger(file, "run-a", { maxBytes: 200, checkInterval: "10 millis" })])),
      Effect.scoped,
      Effect.runPromise,
    )

    expect(await Bun.file(`${file}.1`).text()).toContain("message=before-4")
    expect(await Bun.file(file).text()).toContain("message=after")
    expect(await Bun.file(file).text()).not.toContain("message=before")
    expect((await fs.readdir(dir)).sort()).toEqual(["opencode.log", "opencode.log.1"])
  })
})
