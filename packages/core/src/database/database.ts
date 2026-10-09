export * as Database from "./database"

import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { Context, Duration, Effect, Layer, Schedule } from "effect"
import { sql } from "drizzle-orm"
import { Global } from "../global"
import { Flag } from "../flag/flag"
import { isAbsolute, join } from "path"
import { DatabaseMigration } from "./migration"
import { InstallationChannel } from "../installation/version"
import { makeGlobalNode } from "../effect/app-node"

const AUTO_VACUUM_INCREMENTAL = 2
const BUSY_TIMEOUT_MS = 5000
// Background free-page reclaim: small batches with a short lock wait so it never stalls another process's writers.
const RECLAIM_DELAY = Duration.minutes(1)
const RECLAIM_MIN_FREE_PAGES = 1_000
const RECLAIM_BATCH_PAGES = 500
const RECLAIM_BUSY_TIMEOUT_MS = 100

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
type DatabaseShape = Effect.Success<typeof makeDatabase>

export interface Interface {
  db: DatabaseShape
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/storage/Database") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = yield* makeDatabase

    yield* db.run("PRAGMA journal_mode = WAL")
    yield* db.run("PRAGMA synchronous = NORMAL")
    yield* db.run(sql.raw(`PRAGMA busy_timeout = ${BUSY_TIMEOUT_MS}`))
    yield* db.run("PRAGMA cache_size = -64000")
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")
    yield* DatabaseMigration.apply(db)
    // Off the startup path: short-lived CLI processes exit before the delay, so this mostly runs in long-lived ones.
    yield* reclaimFreePages(db).pipe(Effect.delay(RECLAIM_DELAY), Effect.forkScoped)

    return { db }
  }).pipe(Effect.orDie),
)

// Deleted rows only shrink the file when auto_vacuum is INCREMENTAL, which `opencode db vacuum` enables.
export const reclaimFreePages = Effect.fnUntraced(
  function* (db: DatabaseShape) {
    const mode = yield* db.get<{ auto_vacuum: number }>(sql`PRAGMA auto_vacuum`)
    const stats = yield* db.get<{ page_count: number; freelist_count: number; page_size: number }>(
      sql`SELECT page_count, freelist_count, page_size FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size()`,
    )
    if (!stats) return
    if (mode?.auto_vacuum === AUTO_VACUUM_INCREMENTAL) {
      if (stats.freelist_count < RECLAIM_MIN_FREE_PAGES) return
      // incremental_vacuum frees one page per result row, so a driver that steps once (node:sqlite `.run()`/`.get()`)
      // frees a single page. Both adapters use `.all()`, but loop on freelist_count rather than trust one statement,
      // bounded so a vacuum that stops making progress cannot spin forever.
      yield* vacuumBatch(db).pipe(
        Effect.andThen(db.get<{ freelist_count: number }>(sql`PRAGMA freelist_count`)),
        Effect.map((row) => row?.freelist_count ?? 0),
        Effect.retry({ times: 3, schedule: Schedule.spaced(Duration.seconds(1)) }),
        Effect.repeat({
          while: (free) => free > 0,
          times: Math.ceil(stats.freelist_count / RECLAIM_BATCH_PAGES) * 2,
          schedule: Schedule.spaced(Duration.millis(50)),
        }),
      )
      return
    }
    const freeBytes = stats.freelist_count * stats.page_size
    if (stats.freelist_count <= stats.page_count * 0.25 || freeBytes <= 100 * 1024 * 1024) return
    yield* Effect.logInfo("database has reclaimable free space; run `opencode db vacuum` to shrink it", {
      freeMB: Math.round(freeBytes / 1024 / 1024),
    })
  },
  // Reclaiming space is an optimization; never fail over it (e.g. another process holds the write lock).
  Effect.catch((error) => Effect.logWarning("failed to reclaim database free pages", { error })),
)

// One batch inside a transaction so the shortened busy timeout applies only to this statement on the shared connection.
function vacuumBatch(db: DatabaseShape) {
  return db.transaction((tx) =>
    tx
      .run(sql.raw(`PRAGMA busy_timeout = ${RECLAIM_BUSY_TIMEOUT_MS}`))
      .pipe(
        Effect.andThen(tx.run(sql.raw(`PRAGMA incremental_vacuum(${RECLAIM_BATCH_PAGES})`))),
        Effect.ensuring(tx.run(sql.raw(`PRAGMA busy_timeout = ${BUSY_TIMEOUT_MS}`)).pipe(Effect.ignore)),
      ),
  )
}

export function layerFromPath(filename: string) {
  return layer.pipe(Layer.provide(sqliteLayer({ filename })))
}

export function path() {
  if (Flag.OPENCODE_DB) {
    if (Flag.OPENCODE_DB === ":memory:" || isAbsolute(Flag.OPENCODE_DB)) return Flag.OPENCODE_DB
    return join(Global.Path.data, Flag.OPENCODE_DB)
  }
  if (
    ["latest", "beta", "prod"].includes(InstallationChannel) ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "1" ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "true"
  )
    return join(Global.Path.data, "opencode.db")
  return join(Global.Path.data, `opencode-${InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")}.db`)
}

export const node = makeGlobalNode({ service: Service, layer: layerFromPath(path()), deps: [] })
