import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { Cause, Context, Duration, Effect, Layer, Schedule } from "effect"
import { and, gt, lt, sql } from "drizzle-orm"
import { Config } from "@/config/config"
import { NotFoundError } from "@/storage/storage"
import { Session } from "./session"

export interface Interface {
  /** Deletes sessions archived longer ago than `retention.archived_days` and returns how many were removed. */
  readonly run: () => Effect.Effect<number>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionRetention") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = yield* Config.Service
    const session = yield* Session.Service
    const database = yield* Database.Service

    const run = Effect.fn("SessionRetention.run")(function* () {
      const days = (yield* config.getGlobal()).retention?.archived_days
      if (!days) return 0
      // Unarchived sessions have a null time_archived; legacy clients could store 0 or negative values, which also
      // mean unarchived.
      const rows = yield* database.db
        .select({ id: SessionTable.id })
        .from(SessionTable)
        .where(
          and(
            gt(SessionTable.time_archived, 0),
            lt(SessionTable.time_archived, Date.now() - Duration.toMillis(Duration.days(days))),
          ),
        )
        .all()
        .pipe(Effect.orDie)
      const removed = yield* Effect.forEach(rows, (row) =>
        Effect.gen(function* () {
          // Removing a session also removes all of its descendants, so never remove one that has an unarchived one.
          const descendants = yield* database.db
            .get<{ total: number; unarchived: number }>(
              sql`WITH RECURSIVE tree(id) AS (
                SELECT id FROM ${SessionTable} WHERE parent_id = ${row.id}
                UNION ALL SELECT child.id FROM ${SessionTable} child JOIN tree ON child.parent_id = tree.id
              )
              SELECT count(*) AS total,
                coalesce(sum(time_archived IS NULL OR time_archived <= 0), 0) AS unarchived
              FROM ${SessionTable} WHERE id IN (SELECT id FROM tree)`,
            )
            .pipe(Effect.orDie)
          if (descendants && descendants.unarchived > 0) {
            yield* Effect.logInfo("kept archived session with unarchived child sessions", {
              sessionID: row.id,
              unarchived: descendants.unarchived,
            })
            return 0
          }
          // A descendant removed with an earlier parent is no longer found and counts nothing.
          return yield* session.remove(row.id).pipe(
            Effect.as(1 + (descendants?.total ?? 0)),
            Effect.catchIf(NotFoundError.isInstance, () => Effect.succeed(0)),
          )
        }),
      ).pipe(Effect.map((counts) => counts.reduce((sum, count) => sum + count, 0)))
      if (removed > 0) yield* Effect.logInfo("removed archived sessions", { count: removed, days })
      return removed
    })

    yield* run().pipe(
      Effect.catchCause((cause) => Effect.logError("session retention failed", { cause: Cause.pretty(cause) })),
      Effect.repeat(Schedule.spaced(Duration.days(1))),
      Effect.delay(Duration.minutes(1)),
      Effect.forkScoped,
    )

    return Service.of({ run })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [Config.node, Session.node, Database.node],
})

export * as SessionRetention from "./retention"
