import { stat, utimes } from "fs/promises"
import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Effect } from "effect"
import path from "path"
import { Global } from "@opencode-ai/core/global"
import { Auth } from "../../src/auth"
import { testEffect } from "../lib/effect"

const it = testEffect(LayerNode.compile(Auth.node))

describe("Auth", () => {
  it.instance("set normalizes trailing slashes in keys", () =>
    Effect.gen(function* () {
      const auth = yield* Auth.Service
      yield* auth.set("https://example.com/", {
        type: "wellknown",
        key: "TOKEN",
        token: "abc",
      })
      const data = yield* auth.all()
      expect(data["https://example.com"]).toBeDefined()
      expect(data["https://example.com/"]).toBeUndefined()
    }),
  )

  it.instance("set cleans up pre-existing trailing-slash entry", () =>
    Effect.gen(function* () {
      const auth = yield* Auth.Service
      yield* auth.set("https://example.com/", {
        type: "wellknown",
        key: "TOKEN",
        token: "old",
      })
      yield* auth.set("https://example.com", {
        type: "wellknown",
        key: "TOKEN",
        token: "new",
      })
      const data = yield* auth.all()
      const keys = Object.keys(data).filter((key) => key.includes("example.com"))
      expect(keys).toEqual(["https://example.com"])
      const entry = data["https://example.com"]!
      expect(entry.type).toBe("wellknown")
      if (entry.type === "wellknown") expect(entry.token).toBe("new")
    }),
  )

  it.instance("remove deletes both trailing-slash and normalized keys", () =>
    Effect.gen(function* () {
      const auth = yield* Auth.Service
      yield* auth.set("https://example.com", {
        type: "wellknown",
        key: "TOKEN",
        token: "abc",
      })
      yield* auth.remove("https://example.com/")
      const data = yield* auth.all()
      expect(data["https://example.com"]).toBeUndefined()
      expect(data["https://example.com/"]).toBeUndefined()
    }),
  )

  it.instance("set and remove are no-ops on keys without trailing slashes", () =>
    Effect.gen(function* () {
      const auth = yield* Auth.Service
      yield* auth.set("anthropic", {
        type: "api",
        key: "sk-test",
      })
      const data = yield* auth.all()
      expect(data["anthropic"]).toBeDefined()
      yield* auth.remove("anthropic")
      const after = yield* auth.all()
      expect(after["anthropic"]).toBeUndefined()
    }),
  )

  it.instance("all picks up auth.json written by another process", () =>
    Effect.gen(function* () {
      const auth = yield* Auth.Service
      yield* auth.set("anthropic", { type: "api", key: "sk-old" })
      expect(yield* auth.all()).toMatchObject({ anthropic: { key: "sk-old" } })
      // Another opencode process rewrites the file (e.g. a login in another terminal).
      yield* Effect.promise(() =>
        Bun.write(
          path.join(Global.Path.data, "auth.json"),
          JSON.stringify({ anthropic: { type: "api", key: "sk-new-and-longer" } }),
        ),
      )
      expect(yield* auth.all()).toMatchObject({ anthropic: { key: "sk-new-and-longer" } })
    }),
  )

  it.instance("all picks up a same-length rewrite within the same timestamp", () =>
    Effect.gen(function* () {
      const auth = yield* Auth.Service
      const file = path.join(Global.Path.data, "auth.json")
      yield* auth.set("anthropic", { type: "api", key: "sk-aaaa" })
      expect(yield* auth.all()).toMatchObject({ anthropic: { key: "sk-aaaa" } })
      // Coarse filesystems (HFS+, FAT) can leave mtime unchanged across a quick same-size rewrite.
      const before = yield* Effect.promise(() => stat(file))
      yield* Effect.promise(() => Bun.write(file, JSON.stringify({ anthropic: { type: "api", key: "sk-bbbb" } })))
      yield* Effect.promise(() => utimes(file, before.atime, before.mtime))
      expect(yield* auth.all()).toMatchObject({ anthropic: { key: "sk-bbbb" } })
    }),
  )
})
