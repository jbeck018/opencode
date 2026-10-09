import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { Cause, Context, Duration, Effect, Layer, Schedule } from "effect"
import { lt } from "drizzle-orm"
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
      // Unarchived sessions have a null time_archived, which never compares less than the cutoff.
      const rows = yield* database.db
        .select({ id: SessionTable.id })
        .from(SessionTable)
        .where(lt(SessionTable.time_archived, Date.now() - Duration.toMillis(Duration.days(days))))
        .all()
        .pipe(Effect.orDie)
      for (const row of rows) {
        // Removing a parent also removes its children, which may appear later in the list.
        yield* session.remove(row.id).pipe(Effect.catchIf(NotFoundError.isInstance, () => Effect.void))
      }
      if (rows.length > 0) yield* Effect.logInfo("removed archived sessions", { count: rows.length, days })
      return rows.length
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
