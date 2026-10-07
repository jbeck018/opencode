import { Config } from "@/config/config"
import { GlobalBus, type GlobalEvent as GlobalBusEvent } from "@/bus/global"
import { EffectBridge } from "@/effect/bridge"
import { EventV2 } from "@opencode-ai/core/event"
import { Installation } from "@/installation"
import { disposeAllInstancesAndEmitGlobalDisposed } from "@/server/global-lifecycle"
import { InstallationVersion } from "@opencode-ai/core/installation/version"
import { Effect } from "effect"
import { Readable } from "node:stream"
import { HttpServerRequest, HttpServerResponse } from "effect/http"
import { HttpApiBuilder } from "effect/http-api"
import { RootHttpApi } from "../api"
import { SharedServer } from "@/server/shared"
import { EVENT_FILTER_HEADER, GlobalUpgradeInput } from "../groups/global"
import { Project } from "@/project/project"

// TUIs hold this stream open for every streamed token, so events are written straight to the
// response instead of through an Effect stream: that pipeline cost two fiber handoffs per event
// per subscriber. The bytes are identical to Sse.encode for `message` events without an id.
function eventResponse(keep: (event: GlobalBusEvent) => boolean) {
  return Effect.gen(function* () {
    yield* Effect.logInfo("global event connected")
    const context = yield* Effect.context()
    const body = new Readable({ read() {} })
    const write = (event: unknown) => body.push(`data: ${JSON.stringify(event)}\n\n`)
    const handler = (event: GlobalBusEvent) => {
      if (keep(event)) write(event)
    }
    write({ payload: { id: EventV2.ID.create(), type: "server.connected", properties: {} } })
    GlobalBus.on("event", handler)
    SharedServer.open()
    const heartbeat = setInterval(
      () => write({ payload: { id: EventV2.ID.create(), type: "server.heartbeat", properties: {} } }),
      10_000,
    )
    body.once("close", () => {
      clearInterval(heartbeat)
      GlobalBus.off("event", handler)
      SharedServer.close()
      Effect.runForkWith(context)(Effect.logInfo("global event disconnected"))
    })
    return HttpServerResponse.raw(body, {
      contentType: "text/event-stream",
      headers: {
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
        "X-Content-Type-Options": "nosniff",
      },
    })
  })
}

export const globalHandlers = HttpApiBuilder.group(RootHttpApi, "global", (handlers) =>
  Effect.gen(function* () {
    const config = yield* Config.Service
    const installation = yield* Installation.Service
    const project = yield* Project.Service
    const bridge = yield* EffectBridge.make()

    const health = Effect.fn("GlobalHttpApi.health")(function* () {
      return { healthy: true as const, version: InstallationVersion }
    })

    // A shared server hosts many projects; a TUI only needs its own project's events and
    // never reads sync copies, so it can ask the server not to send the rest. The filter is an
    // opt-in header rather than a query parameter: SDK clients created with a directory add it
    // to every GET, and those subscribers have always received every project's events.
    const event = Effect.fn("GlobalHttpApi.event")(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest
      const filter = new Set((request.headers[EVENT_FILTER_HEADER] ?? "").split(",").map((item) => item.trim()))
      const directory = filter.has("project") ? requestDirectory(request) : undefined
      const projectID = directory ? (yield* project.fromDirectory(directory)).project.id : undefined
      return yield* eventResponse((item) => {
        if (filter.has("no-sync") && item.payload.type === "sync") return false
        if (!projectID || !item.project) return true
        return item.project === projectID
      })
    })

    const configGet = Effect.fn("GlobalHttpApi.configGet")(function* () {
      return yield* config.getGlobal()
    })

    const configUpdate = Effect.fn("GlobalHttpApi.configUpdate")(function* (ctx) {
      const result = yield* config.updateGlobal(ctx.payload)
      if (result.changed) bridge.fork(disposeAllInstancesAndEmitGlobalDisposed({ swallowErrors: true }))
      return result.info
    })

    const dispose = Effect.fn("GlobalHttpApi.dispose")(function* () {
      yield* disposeAllInstancesAndEmitGlobalDisposed()
      return true
    })

    const upgrade = Effect.fn("GlobalHttpApi.upgrade")(function* (ctx: { payload: typeof GlobalUpgradeInput.Type }) {
      const method = yield* installation.method()
      if (method === "unknown") {
        return HttpServerResponse.jsonUnsafe(
          { success: false as const, error: "Unknown installation method" },
          { status: 400 },
        )
      }
      const target = ctx.payload.target
      const result = yield* installation.upgrade(method, target).pipe(
        Effect.as({ success: true as const, version: target }),
        Effect.catch((err) =>
          Effect.succeed({
            success: false as const,
            error: err instanceof Error ? err.message : String(err),
          }),
        ),
      )
      if (!result.success) return HttpServerResponse.jsonUnsafe(result, { status: 500 })
      GlobalBus.emit("event", {
        directory: "global",
        payload: {
          type: Installation.Event.Updated.type,
          properties: { version: target },
        },
      })
      return HttpServerResponse.jsonUnsafe(result)
    })

    return handlers
      .handle("health", health)
      .handleRaw("event", event)
      .handle("configGet", configGet)
      .handle("configUpdate", configUpdate)
      .handle("dispose", dispose)
      .handle("upgrade", upgrade)
  }),
)

function requestDirectory(request: HttpServerRequest.HttpServerRequest) {
  const query = new URL(request.url, "http://localhost").searchParams.get("directory")
  if (query) return query
  const header = request.headers["x-opencode-directory"]
  if (!header) return undefined
  // SDK clients send the header URI-encoded.
  try {
    return decodeURIComponent(header)
  } catch {
    return header
  }
}
