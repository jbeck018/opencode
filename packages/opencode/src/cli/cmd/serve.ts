import { Effect } from "effect"
import { effectCmd } from "../effect-cmd"
import { withNetworkOptions, resolveNetworkOptions } from "../network"
import { Flag } from "@opencode-ai/core/flag/flag"

export const ServeCommand = effectCmd({
  command: "serve",
  builder: (yargs) =>
    withNetworkOptions(yargs).option("shared", {
      type: "string",
      hidden: true,
      describe: "run as the background shared server for the given environment key",
    }),
  describe: "starts a headless opencode server",
  // Server loads instances per-request via x-opencode-directory header — no
  // need for an ambient project InstanceContext at startup.
  instance: false,
  handler: Effect.fn("Cli.serve")(function* (args) {
    const { Server } = yield* Effect.promise(() => import("../../server/server"))
    if (args.shared) {
      const key = args.shared
      const { SharedServer } = yield* Effect.promise(() => import("../../server/shared"))
      const { InstanceRuntime } = yield* Effect.promise(() => import("../../project/instance-runtime"))
      const { upgrade } = yield* Effect.promise(() => import("../upgrade"))
      const server = yield* Effect.promise(() =>
        Server.listen({ hostname: "127.0.0.1", port: 0, ephemeral: true, cors: [] }),
      )
      yield* Effect.promise(() =>
        SharedServer.register({
          key,
          url: server.url,
          shutdown: async () => {
            await InstanceRuntime.disposeAllInstances()
            await server.stop(true)
          },
        }),
      )
      // Attached TUIs have no private worker to run the update check, so the shared server does it.
      setTimeout(() => {
        upgrade().catch(() => {})
      }, 1000).unref?.()
      return yield* Effect.never
    }
    if (!Flag.OPENCODE_SERVER_PASSWORD) {
      console.log("Warning: OPENCODE_SERVER_PASSWORD is not set; server is unsecured.")
    }
    const opts = yield* resolveNetworkOptions(args)
    const server = yield* Effect.promise(() => Server.listen(opts))
    console.log(`opencode server listening on http://${server.hostname}:${server.port}`)

    return yield* Effect.never
  }),
})
