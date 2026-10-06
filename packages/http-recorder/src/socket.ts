import { NodeFileSystem } from "@effect/platform-node"
import { Cause, Deferred, Effect, Exit, Layer, Ref, Scope, Semaphore } from "effect"
import { Socket } from "effect/socket"
import * as CassetteService from "./cassette.js"
import { canonicalizeJson, decodeJson, safeText } from "./matching.js"
import { makeReplayState, resolveAutoMode } from "./recorder.js"
import { make, type Redactor } from "./redactor.js"
import { webSocketInteractions } from "./schema.js"
import type {
  RecorderOptions,
  WebSocketEvent,
  WebSocketInteraction,
  WebSocketRecorderOptions,
  WebSocketRequest,
} from "./types.js"

interface ActiveReplay {
  readonly interaction: WebSocketInteraction
  readonly progress: Ref.Ref<{ readonly position: number; readonly changed: Deferred.Deferred<void> }>
  readonly writeLock: Semaphore.Semaphore
  readonly closed: Ref.Ref<boolean>
}

interface ActiveRecording {
  readonly events: Array<WebSocketEvent>
  readonly eventLock: Semaphore.Semaphore
  readonly accepting: Ref.Ref<boolean>
  opened: boolean
  valid: boolean
}

type Frame = string | Uint8Array

const encodeEvent = (direction: "client" | "server", message: Frame): WebSocketEvent =>
  typeof message === "string"
    ? { direction, kind: "text", body: message }
    : { direction, kind: "binary", body: Buffer.from(message).toString("base64"), bodyEncoding: "base64" }

const decodeEvent = (event: WebSocketEvent): Frame =>
  event.kind === "text" ? event.body : new Uint8Array(Buffer.from(event.body, "base64"))

const redactEvent = (event: WebSocketEvent, redactor: Redactor): WebSocketEvent => {
  if (event.kind === "binary") return event
  const body =
    event.direction === "client"
      ? redactor.request({ method: "WEBSOCKET", url: "", headers: {}, body: event.body }).body
      : redactor.response({ status: 101, headers: {}, body: event.body }).body
  return { ...event, body }
}

const comparable = (event: WebSocketEvent, asJson: boolean) => {
  if (!asJson || event.kind === "binary") return JSON.stringify(canonicalizeJson(event))
  const decoded = decodeJson(event.body)
  return JSON.stringify(
    canonicalizeJson({
      ...event,
      body: decoded._tag === "None" ? event.body : canonicalizeJson(decoded.value),
    }),
  )
}

const assertEvent = (actual: WebSocketEvent, expected: WebSocketEvent | undefined, index: number, asJson: boolean) =>
  Effect.sync(() => {
    if (expected && comparable(actual, asJson) === comparable(expected, asJson)) return
    throw new Error(`WebSocket event ${index + 1}: expected ${safeText(expected)}, received ${safeText(actual)}`)
  })

// Every socket termination surfaces as a SocketError, so a run ended by a peer close is still complete.
const completed = (exit: Exit.Exit<unknown, unknown>) =>
  Exit.isSuccess(exit) ||
  exit.cause.reasons.every(
    (reason) =>
      Cause.isFailReason(reason) &&
      Socket.isSocketError(reason.error) &&
      reason.error.reason._tag === "SocketCloseError",
  )

const unconsumed = (state: ActiveReplay, position: number) =>
  new Error(`WebSocket closed with unconsumed events: used ${position} of ${state.interaction.events.length}`)

// Server events are delivered one per pull; client events block the reader until the writer
// consumes them, which preserves the recorded causal order.
const pullReplay = (state: ActiveReplay): Effect.Effect<readonly [Frame], Socket.SocketError> =>
  Effect.gen(function* () {
    const current = yield* Ref.get(state.progress)
    const event = state.interaction.events[current.position]
    if (!event) return yield* new Socket.SocketError({ reason: new Socket.SocketCloseError({ code: 1000 }) })
    if (yield* Ref.get(state.closed)) return yield* Effect.die(unconsumed(state, current.position))
    if (event.direction === "server") {
      yield* Ref.set(state.progress, { position: current.position + 1, changed: yield* Deferred.make<void>() })
      return [decodeEvent(event)] as const
    }
    yield* Deferred.await(current.changed)
    return yield* pullReplay(state)
  })

const openSnapshot = (request: WebSocketRequest, redactor: Redactor) => {
  const snapshot = redactor.request({ method: "GET", url: request.url, headers: request.headers ?? {}, body: "" })
  return { url: snapshot.url, headers: snapshot.headers }
}

const makeRecordingSocket = (
  upstream: Socket.Socket,
  cassette: CassetteService.Interface,
  name: string,
  request: WebSocketRequest,
  options: WebSocketRecorderOptions,
  redactor: Redactor,
) =>
  Effect.gen(function* () {
    const active = yield* Ref.make<ActiveRecording | undefined>(undefined)
    const writeLock = yield* Semaphore.make(1)

    return Socket.make({
      reader: Effect.gen(function* () {
        const state: ActiveRecording = {
          events: [],
          eventLock: yield* Semaphore.make(1),
          accepting: yield* Ref.make(true),
          opened: false,
          valid: true,
        }
        const occupied = yield* Ref.modify(active, (current) => [current !== undefined, current ?? state])
        if (occupied) return yield* Effect.die("Concurrent runs of a recorded WebSocket are not supported")
        // Registered before the upstream reader so it runs after the upstream connection is released.
        yield* Effect.addFinalizer((exit) =>
          writeLock.withPermit(
            state.eventLock.withPermit(
              Effect.gen(function* () {
                yield* Ref.set(state.accepting, false)
                yield* Ref.set(active, undefined)
                if (!completed(exit) || !state.opened || !state.valid) return
                yield* cassette
                  .append(
                    name,
                    {
                      transport: "websocket",
                      open: openSnapshot(request, redactor),
                      events: [...state.events],
                    },
                    options.metadata,
                  )
                  .pipe(Effect.orDie)
              }),
            ),
          ),
        )
        const reader = yield* upstream.reader
        state.opened = true
        return {
          upgrade: reader.upgrade,
          pull: reader.pull.pipe(
            Effect.tap((messages) =>
              Effect.sync(() => {
                if (!Ref.getUnsafe(state.accepting)) throw new Error("WebSocket received a frame after closing")
                for (const message of messages) state.events.push(redactEvent(encodeEvent("server", message), redactor))
              }),
            ),
            // A close is the normal end of a run; any other failure leaves an incomplete recording.
            Effect.tapError((error) =>
              Effect.sync(() => {
                if (error.reason._tag !== "SocketCloseError") state.valid = false
              }),
            ),
          ),
        }
      }),
      writer: upstream.writer.pipe(
        Effect.map((upstreamWriter) => {
          const write = (message: Frame | Socket.CloseEvent) =>
            writeLock.withPermit(
              Effect.gen(function* () {
                if (Socket.isCloseEvent(message)) return yield* upstreamWriter.write(message)
                const state = yield* Ref.get(active)
                if (!state || !(yield* Ref.get(state.accepting)))
                  return yield* Effect.die("WebSocket writer used without an active socket run")
                const event = redactEvent(encodeEvent("client", message), redactor)
                yield* state.eventLock.withPermit(Effect.sync(() => state.events.push(event)))
                return yield* upstreamWriter
                  .write(message)
                  .pipe(Effect.onError(() => Effect.sync(() => (state.valid = false))))
              }),
            )
          return { write, writeAll: (messages) => Effect.forEach(messages, write, { discard: true }) }
        }),
      ),
    })
  })

const makeReplaySocket = (
  cassette: CassetteService.Interface,
  name: string,
  request: WebSocketRequest,
  options: WebSocketRecorderOptions,
  redactor: Redactor,
): Effect.Effect<Socket.Socket, never, Scope.Scope> =>
  Effect.gen(function* () {
    const replay = yield* makeReplayState(cassette, name, webSocketInteractions)
    const active = yield* Ref.make<ActiveReplay | undefined>(undefined)

    const write = (message: Frame | Socket.CloseEvent) =>
      Ref.get(active).pipe(
        Effect.flatMap((state) =>
          state
            ? state.writeLock.withPermit(
                Effect.gen(function* () {
                  const current = yield* Ref.get(state.progress)
                  if (Socket.isCloseEvent(message)) {
                    yield* Ref.set(state.closed, true)
                    yield* Deferred.succeed(current.changed, undefined)
                    if (current.position === state.interaction.events.length) return
                    return yield* Effect.die(unconsumed(state, current.position))
                  }
                  const actual = redactEvent(encodeEvent("client", message), redactor)
                  yield* assertEvent(
                    actual,
                    state.interaction.events[current.position],
                    current.position,
                    options.compareClientMessagesAsJson === true,
                  )
                  yield* Ref.set(state.progress, {
                    position: current.position + 1,
                    changed: yield* Deferred.make<void>(),
                  })
                  yield* Deferred.succeed(current.changed, undefined)
                }),
              )
            : Effect.die("WebSocket writer used without an active socket run"),
        ),
      )

    return Socket.make({
      reader: Effect.gen(function* () {
        const claimed = yield* replay
          .claim((interaction, index) =>
            Effect.sync(() => {
              const incoming = openSnapshot(request, redactor)
              if (
                interaction &&
                JSON.stringify(canonicalizeJson(incoming)) === JSON.stringify(canonicalizeJson(interaction.open))
              )
                return
              throw new Error(
                `WebSocket open ${index + 1}: expected ${safeText(interaction?.open)}, received ${safeText(incoming)}`,
              )
            }),
          )
          .pipe(Effect.orDie)
        const state: ActiveReplay = {
          interaction: claimed.interaction,
          progress: yield* Ref.make({ position: 0, changed: yield* Deferred.make<void>() }),
          writeLock: yield* Semaphore.make(1),
          closed: yield* Ref.make(false),
        }
        const occupied = yield* Ref.modify(active, (current) => [current !== undefined, current ?? state])
        if (occupied) return yield* Effect.die("Concurrent runs of a replayed WebSocket are not supported")
        yield* Effect.addFinalizer(() => Ref.set(active, undefined))
        return { pull: pullReplay(state), upgrade: Socket.SocketUpgradeError.unsupported }
      }),
      writer: Effect.succeed({ write, writeAll: (messages) => Effect.forEach(messages, write, { discard: true }) }),
    })
  })

const recordingLayer = (
  name: string,
  request: WebSocketRequest,
  options: WebSocketRecorderOptions,
  forcedMode?: "record" | "replay",
): Layer.Layer<Socket.Socket, never, Socket.Socket | CassetteService.Service> =>
  Layer.effect(
    Socket.Socket,
    Effect.gen(function* () {
      const upstream = yield* Socket.Socket
      const cassette = yield* CassetteService.Service
      const redactor = make(options.redact)
      if ((forcedMode ?? (yield* resolveAutoMode(cassette, name))) === "record")
        return yield* makeRecordingSocket(upstream, cassette, name, request, options, redactor)
      return yield* makeReplaySocket(cassette, name, request, options, redactor)
    }),
  )

/**
 * Wraps a provided `Socket.Socket` with cassette recording and replay.
 *
 * Supply the ordinary URL-bound Effect socket layer beneath this decorator.
 * The cassette name identifies the connection; recorder configuration does not
 * duplicate the transport URL.
 */
export const socket = (name: string, options: RecorderOptions = {}): Layer.Layer<Socket.Socket, never, Socket.Socket> =>
  provideCassette(recordingLayer(name, { url: "" }, { ...options, compareClientMessagesAsJson: true }), options)

/** @internal */
export const socketLayer = (
  name: string,
  request: WebSocketRequest,
  options: WebSocketRecorderOptions & { readonly mode: "record" | "replay" },
): Layer.Layer<Socket.Socket, never, Socket.Socket> =>
  provideCassette(recordingLayer(name, request, options, options.mode), options)

const provideCassette = (
  layer: Layer.Layer<Socket.Socket, never, Socket.Socket | CassetteService.Service>,
  options: WebSocketRecorderOptions,
) =>
  layer.pipe(
    Layer.provide(CassetteService.fileSystem({ directory: options.directory })),
    Layer.provide(NodeFileSystem.layer),
  )
