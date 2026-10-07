import { Effect, Schema } from "effect"
import { Memory } from "@/memory"
import DESCRIPTION from "./memory.txt"
import * as Tool from "./tool"

export const Parameters = Schema.Struct({
  action: Schema.Literals(["view", "save", "delete"]).annotate({
    description: "view reads the index or one memory, save creates or replaces a memory, delete removes one",
  }),
  name: Schema.optional(Schema.String).annotate({
    description:
      "Memory name, used as the topic file name (lowercase letters, digits, '-', '_', '.'). Required for save and delete",
  }),
  type: Schema.optional(Memory.Type).annotate({
    description: "Required for save: user, feedback, project or reference",
  }),
  description: Schema.optional(Schema.String).annotate({
    description: "Required for save: one-line summary used in the MEMORY.md index",
  }),
  content: Schema.optional(Schema.String).annotate({
    description:
      "The memory body in markdown. Required when creating a memory; omit when updating to keep the existing body",
  }),
})

// Permission: the tool only touches its own memory directory and is shown to the user like any other tool call,
// so it asks under the `memory` permission, which the default `*: allow` rule grants. Users can set
// `permission.memory` to "ask" or "deny". It deliberately skips the external_directory check.
export const MemoryTool = Tool.define(
  "memory",
  Effect.gen(function* () {
    const memory = yield* Memory.Service
    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          yield* ctx.ask({
            permission: "memory",
            patterns: [params.action === "view" ? (params.name ?? "MEMORY") : params.name || params.action],
            always: ["*"],
            metadata: { action: params.action, name: params.name },
          })

          if (params.action === "view") {
            const output = yield* memory.view(params.name)
            return { title: params.name ?? "MEMORY.md", metadata: { over: false }, output }
          }
          if (!params.name) throw new Error(`The name parameter is required for ${params.action}`)

          if (params.action === "delete") {
            const result = yield* memory.remove(params.name)
            return {
              title: `Deleted ${params.name}`,
              metadata: { over: result.usage.over },
              output: [`Deleted memory "${params.name}".`, result.warning].filter(Boolean).join("\n\n"),
            }
          }

          if (!params.type || !params.description) throw new Error("save requires type and description")
          const result = yield* memory.save({
            name: params.name,
            type: params.type,
            description: params.description,
            content: params.content,
          })
          return {
            title: `Saved ${params.name}`,
            metadata: { over: result.usage.over },
            output: [`Saved memory "${params.name}" to ${result.file}.`, result.warning].filter(Boolean).join("\n\n"),
          }
        }).pipe(
          Effect.catchTags({
            "Memory.InvalidNameError": (error) => Effect.die(new Error(error.message)),
            "Memory.NotFoundError": (error) => Effect.die(new Error(error.message)),
            "Memory.UnavailableError": (error) => Effect.die(new Error(error.message)),
            "Memory.ContentRequiredError": (error) => Effect.die(new Error(error.message)),
          }),
        ),
    } satisfies Tool.DefWithoutID<typeof Parameters>
  }),
)
