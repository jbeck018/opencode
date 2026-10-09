export * as Database from "./database"

import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { Context, Effect, Layer } from "effect"
import { sql } from "drizzle-orm"
import { Global } from "../global"
import { Flag } from "../flag/flag"
import { isAbsolute, join } from "path"
import { DatabaseMigration } from "./migration"
import { InstallationChannel } from "../installation/version"
import { makeGlobalNode } from "../effect/app-node"

const AUTO_VACUUM_INCREMENTAL = 2
const INCREMENTAL_VACUUM_PAGES = 10_000

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
    yield* db.run("PRAGMA busy_timeout = 5000")
    yield* db.run("PRAGMA cache_size = -64000")
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")
    yield* DatabaseMigration.apply(db)
    yield* reclaimFreePages(db)

    return { db }
  }).pipe(Effect.orDie),
)

// Deleted rows only shrink the file when auto_vacuum is INCREMENTAL, which `opencode db vacuum` enables.
const reclaimFreePages = Effect.fnUntraced(
  function* (db: DatabaseShape) {
    const mode = yield* db.get<{ auto_vacuum: number }>(sql`PRAGMA auto_vacuum`)
    if (mode?.auto_vacuum === AUTO_VACUUM_INCREMENTAL) {
      yield* db.run(sql.raw(`PRAGMA incremental_vacuum(${INCREMENTAL_VACUUM_PAGES})`))
      return
    }
    const stats = yield* db.get<{ page_count: number; freelist_count: number; page_size: number }>(
      sql`SELECT page_count, freelist_count, page_size FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size()`,
    )
    if (!stats) return
    const freeBytes = stats.freelist_count * stats.page_size
    if (stats.freelist_count <= stats.page_count * 0.25 || freeBytes <= 100 * 1024 * 1024) return
    yield* Effect.logInfo("database has reclaimable free space; run `opencode db vacuum` to shrink it", {
      freeMB: Math.round(freeBytes / 1024 / 1024),
    })
  },
  // Reclaiming space is an optimization; never fail startup over it (e.g. another process holds the lock).
  Effect.catch((error) => Effect.logWarning("failed to reclaim database free pages", { error })),
)

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
