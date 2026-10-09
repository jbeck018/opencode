import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261008232918_event_snapshot_key",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`event\` ADD \`snapshot_key\` text;`)
      yield* tx.run(
        `CREATE INDEX \`event_aggregate_type_snapshot_idx\` ON \`event\` (\`aggregate_id\`,\`type\`,\`snapshot_key\`) WHERE "event"."snapshot_key" IS NOT NULL;`,
      )
      // Older builds kept every snapshot. Keep each entity's first one, which creates the entity before
      // events that reference it, and its newest; then key them so later writes supersede them.
      const key = `CASE type WHEN 'message.updated.1' THEN json_extract(data, '$.info.id') ELSE json_extract(data, '$.part.id') END`
      const snapshot = `type IN ('message.updated.1', 'message.part.updated.1')`
      yield* tx.run(`
        DELETE FROM \`event\` WHERE rowid IN (
          SELECT rowid FROM (
            SELECT rowid,
              row_number() OVER (PARTITION BY aggregate_id, type, ${key} ORDER BY seq) AS first,
              row_number() OVER (PARTITION BY aggregate_id, type, ${key} ORDER BY seq DESC) AS last
            FROM \`event\`
            WHERE ${snapshot} AND ${key} IS NOT NULL
          ) WHERE first > 1 AND last > 1
        );
      `)
      yield* tx.run(`UPDATE \`event\` SET snapshot_key = ${key} WHERE ${snapshot};`)
    })
  },
} satisfies DatabaseMigration.Migration
