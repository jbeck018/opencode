import { describe, expect, test } from "bun:test"
import path from "path"
import { Duration, Effect } from "effect"
import { TestClock } from "effect/testing"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { tmpdir } from "./fixture/tmpdir"

const freePages = Effect.gen(function* () {
  const database = yield* Database.Service
  return (yield* database.db.get<{ freelist_count: number }>(sql`PRAGMA freelist_count`).pipe(Effect.orDie))
    ?.freelist_count
})

const reclaim = Effect.gen(function* () {
  const database = yield* Database.Service
  yield* Database.reclaimFreePages(database.db)
  return yield* freePages
})

// Leaves free pages behind by filling and dropping a table (about two pages per row), optionally after switching to
// INCREMENTAL.
const freeSomePages = (incremental: boolean, rows = 1500) =>
  Effect.gen(function* () {
    const database = yield* Database.Service
    if (incremental) {
      yield* database.db.run(sql`PRAGMA auto_vacuum = INCREMENTAL`)
      yield* database.db.run(sql`VACUUM`)
    }
    yield* database.db.run(sql`CREATE TABLE junk (data BLOB)`)
    yield* database.db.run(
      sql.raw(
        `WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < ${rows}) INSERT INTO junk SELECT randomblob(8000) FROM n`,
      ),
    )
    yield* database.db.run(sql`DROP TABLE junk`)
    return yield* freePages
  }).pipe(Effect.orDie)

const open = <A>(filename: string, effect: Effect.Effect<A, never, Database.Service>) =>
  Effect.runPromise(effect.pipe(Effect.provide(Database.layerFromPath(filename))))

describe("database free page reclaim", () => {
  test("startup does not run incremental_vacuum on the startup path", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "startup.sqlite")

    const freed = await open(filename, freeSomePages(true))
    expect(freed).toBeGreaterThan(2000)
    expect(await open(filename, freePages)).toBe(freed)
  })

  test("long-lived processes keep reclaiming free pages after the first run", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "repeat.sqlite")
    await open(filename, freeSomePages(true))

    // Advances the test clock in small steps so each vacuum batch's own spacing elapses too.
    const advance = (total: Duration.Input) =>
      Effect.gen(function* () {
        const step = Duration.seconds(1)
        const steps = Math.ceil(Duration.toMillis(total) / Duration.toMillis(step))
        for (const _ of Array.from({ length: steps })) yield* TestClock.adjust(step)
      })

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        yield* advance(Duration.seconds(65))
        const first = yield* freePages
        const freed = yield* freeSomePages(false)
        yield* advance(Duration.minutes(30))
        const midway = yield* freePages
        yield* advance(Duration.minutes(31))
        return { first, freed, midway, second: yield* freePages }
      }).pipe(Effect.provide(Database.layerFromPath(filename)), Effect.provide(TestClock.layer())),
    )
    expect(result.first).toBe(0)
    expect(result.freed).toBeGreaterThan(2000)
    expect(result.midway).toBe(result.freed)
    expect(result.second).toBe(0)
  })

  test("reclaims every free page in batches when auto_vacuum is INCREMENTAL", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "incremental.sqlite")

    expect(await open(filename, freeSomePages(true))).toBeGreaterThan(2000)
    expect(await open(filename, reclaim)).toBe(0)
  })

  test("leaves a small free list alone", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "small.sqlite")

    const freed = await open(filename, freeSomePages(true, 100))
    expect(freed).toBeGreaterThan(0)
    expect(await open(filename, reclaim)).toBe(freed)
  })

  // Electron/Node runs the node:sqlite adapter, where a statement that steps once would free a single page.
  test.skipIf(!Bun.which("node"))("frees whole batches through the node:sqlite adapter", async () => {
    await using tmp = await tmpdir()
    const build = await Bun.build({
      entrypoints: [path.join(import.meta.dir, "fixture/database-vacuum-node.ts")],
      target: "node",
      conditions: ["node"],
      outdir: tmp.path,
    })
    expect(build.success).toBe(true)
    const proc = Bun.spawn(["node", build.outputs[0].path, path.join(tmp.path, "node.sqlite")], { stderr: "inherit" })
    expect(await proc.exited).toBe(0)
    const result = JSON.parse(await new Response(proc.stdout).text())
    expect(result.before).toBeGreaterThan(2000)
    expect(result.before - result.single).toBe(100)
    expect(result.after).toBe(0)
  })

  test("leaves free pages alone when auto_vacuum is NONE", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "none.sqlite")

    const freed = await open(filename, freeSomePages(false))
    expect(freed).toBeGreaterThan(2000)
    expect(await open(filename, reclaim)).toBe(freed)
  })
})
