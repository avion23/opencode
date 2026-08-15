import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260815174913_exotic_wilson_fisk",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`event_sequence\` ADD \`removed\` integer DEFAULT false NOT NULL;`)
    })
  },
} satisfies DatabaseMigration.Migration
