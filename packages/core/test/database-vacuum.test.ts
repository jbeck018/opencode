import { describe, expect, test } from "bun:test"
import path from "path"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { tmpdir } from "./fixture/tmpdir"

const freePages = Effect.gen(function* () {
  const database = yield* Database.Service
  return (yield* database.db.get<{ freelist_count: number }>(sql`PRAGMA freelist_count`).pipe(Effect.orDie))
    ?.freelist_count
})

// Leaves free pages behind by filling and dropping a table, optionally after switching to INCREMENTAL.
const freeSomePages = (incremental: boolean) =>
  Effect.gen(function* () {
    const database = yield* Database.Service
    if (incremental) {
      yield* database.db.run(sql`PRAGMA auto_vacuum = INCREMENTAL`)
      yield* database.db.run(sql`VACUUM`)
    }
    yield* database.db.run(sql`CREATE TABLE junk (data BLOB)`)
    yield* database.db.run(
      sql`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 500) INSERT INTO junk SELECT randomblob(8000) FROM n`,
    )
    yield* database.db.run(sql`DROP TABLE junk`)
    return yield* freePages
  }).pipe(Effect.orDie)

const open = <A>(filename: string, effect: Effect.Effect<A, never, Database.Service>) =>
  Effect.runPromise(effect.pipe(Effect.provide(Database.layerFromPath(filename))))

describe("database free page reclaim", () => {
  test("startup runs incremental_vacuum when auto_vacuum is INCREMENTAL", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "incremental.sqlite")

    expect(await open(filename, freeSomePages(true))).toBeGreaterThan(500)
    expect(await open(filename, freePages)).toBe(0)
  })

  test("startup leaves free pages alone when auto_vacuum is NONE", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "none.sqlite")

    const freed = await open(filename, freeSomePages(false))
    expect(freed).toBeGreaterThan(500)
    expect(await open(filename, freePages)).toBe(freed)
  })
})
