// Bundled for Node by database-vacuum.test.ts so `#sqlite` resolves to the node:sqlite adapter.
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { Database } from "../../src/database/database"

const freePages = (db: Database.Interface["db"]) =>
  db.get<{ freelist_count: number }>(sql`PRAGMA freelist_count`).pipe(Effect.map((row) => row?.freelist_count))

const program = Effect.gen(function* () {
  const db = (yield* Database.Service).db
  yield* db.run(sql`PRAGMA auto_vacuum = INCREMENTAL`)
  yield* db.run(sql`VACUUM`)
  yield* db.run(sql`CREATE TABLE junk (data BLOB)`)
  yield* db.run(
    sql`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 1500) INSERT INTO junk SELECT randomblob(8000) FROM n`,
  )
  yield* db.run(sql`DROP TABLE junk`)
  const before = yield* freePages(db)
  // One statement on its own, to show whether the adapter steps it to completion.
  yield* db.run(sql`PRAGMA incremental_vacuum(100)`)
  const single = yield* freePages(db)
  yield* Database.reclaimFreePages(db)
  return { before, single, after: yield* freePages(db) }
})

console.log(
  JSON.stringify(
    await Effect.runPromise(program.pipe(Effect.provide(Database.layerFromPath(process.argv[2]!)), Effect.orDie)),
  ),
)
