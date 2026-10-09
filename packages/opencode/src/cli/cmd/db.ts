import type { Argv } from "yargs"
import { spawn } from "child_process"
import { Database } from "@opencode-ai/core/database/database"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { effectCmd } from "../effect-cmd"

const QueryCommand = effectCmd({
  command: "$0 [query]",
  describe: "open an interactive sqlite3 shell or run a query",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs
      .positional("query", {
        type: "string",
        describe: "SQL query to execute",
      })
      .option("format", {
        type: "string",
        choices: ["json", "tsv"],
        default: "tsv",
        describe: "Output format",
      })
  },
  handler: Effect.fn("Cli.db.query")(function* (args: { query?: string; format: string }) {
    const query = args.query as string | undefined
    if (query) {
      const { db } = yield* Database.Service
      const result = yield* db.all<Record<string, unknown>>(sql.raw(query)).pipe(Effect.orDie)
      if (args.format === "json") console.log(JSON.stringify(result, null, 2))
      else if (result.length > 0) {
        const keys = Object.keys(result[0])
        console.log(keys.join("\t"))
        for (const row of result) console.log(keys.map((key) => row[key]).join("\t"))
      }
      return
    }
    const child = spawn("sqlite3", [Database.path()], {
      stdio: "inherit",
    })
    yield* Effect.promise(() => new Promise((resolve) => child.on("close", resolve)))
  }),
})

const PathCommand = effectCmd({
  command: "path",
  describe: "print the database path",
  instance: false,
  handler: Effect.fn("Cli.db.path")(function* () {
    console.log(Database.path())
  }),
})

const VacuumCommand = effectCmd({
  command: "vacuum",
  describe: "shrink the database file to its live data (needs free disk space about the size of the result)",
  instance: false,
  handler: Effect.fn("Cli.db.vacuum")(function* () {
    const { db } = yield* Database.Service
    const before = size()
    // VACUUM rewrites the whole file and needs every other connection idle.
    const vacuumed = yield* db.run(sql`VACUUM`).pipe(
      Effect.andThen(db.run(sql`PRAGMA wal_checkpoint(TRUNCATE)`)),
      Effect.as(true),
      Effect.catch((error) =>
        Effect.sync(() => {
          console.error(`vacuum failed: ${error}\nClose other opencode processes (including \`opencode serve\`) and retry.`)
          process.exitCode = 1
          return false
        }),
      ),
    )
    if (vacuumed) console.log(`${Database.path()}: ${mb(before)} -> ${mb(size())}`)
  }),
})

function size() {
  return Bun.file(Database.path()).size + Bun.file(`${Database.path()}-wal`).size
}

function mb(bytes: number) {
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`
}

export const DbCommand = effectCmd({
  command: "db",
  describe: "database tools",
  instance: false,
  builder: (yargs: Argv) => {
    return yargs.command(QueryCommand).command(PathCommand).command(VacuumCommand).demandCommand()
  },
  handler: Effect.fn("Cli.db")(function* () {}),
})
