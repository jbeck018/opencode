import fs from "fs/promises"
import path from "path"
import { pathToFileURL } from "url"
import { describe, expect, test } from "bun:test"
import { Effect, Fiber, Option } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Npm } from "@opencode-ai/core/npm"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { which } from "@opencode-ai/core/util/which"
import { tmpdir } from "./fixture/tmpdir"

const win = process.platform === "win32"

const writePackage = (dir: string, pkg: Record<string, unknown>) =>
  Bun.write(
    path.join(dir, "package.json"),
    JSON.stringify({
      version: "1.0.0",
      ...pkg,
    }),
  )

const npmLayer = (cache: string) =>
  AppNodeBuilder.build(Npm.node, [[Global.node, Global.layerWith({ cache, state: path.join(cache, "state") })]])

const DAY_MS = 24 * 60 * 60 * 1000

const setAge = (file: string, days: number) => {
  const time = new Date(Date.now() - days * DAY_MS)
  return fs.utimes(file, time, time)
}

// Pre-seeds an install the way a previous reify would have left it.
const seedInstall = async (dir: string, name: string) => {
  await writePackage(path.join(dir, "node_modules", name), { name, main: "index.js" })
  await Bun.write(path.join(dir, "node_modules", name, "index.js"), "export const fixture = true\n")
  await writePackage(dir, { name: "cache", dependencies: { [name]: "1.0.0" } })
}

describe("Npm.sanitize", () => {
  test("keeps normal scoped package specs unchanged", () => {
    expect(Npm.sanitize("@opencode/acme")).toBe("@opencode/acme")
    expect(Npm.sanitize("@opencode/acme@1.0.0")).toBe("@opencode/acme@1.0.0")
    expect(Npm.sanitize("prettier")).toBe("prettier")
  })

  test("handles git https specs", () => {
    const spec = "acme@git+https://github.com/opencode/acme.git"
    const expected = win ? "acme@git+https_//github.com/opencode/acme.git" : spec
    expect(Npm.sanitize(spec)).toBe(expected)
  })
})

describe("Npm.add", () => {
  test("reifies when package cache directory exists without the package installed", async () => {
    await using tmp = await tmpdir()
    await fs.mkdir(path.join(tmp.path, "fixture-provider"))
    await writePackage(path.join(tmp.path, "fixture-provider"), {
      name: "fixture-provider",
      main: "index.js",
    })
    await Bun.write(path.join(tmp.path, "fixture-provider", "index.js"), "export const fixture = true\n")

    const spec = `fixture-provider@file:${path.join(tmp.path, "fixture-provider")}`
    await fs.mkdir(path.join(tmp.path, "cache", "packages", Npm.sanitize(spec)), { recursive: true })

    const entry = await Effect.gen(function* () {
      const npm = yield* Npm.Service
      return yield* npm.add(spec)
    }).pipe(Effect.scoped, Effect.provide(npmLayer(path.join(tmp.path, "cache"))), Effect.runPromise)

    expect(entry.entrypoint).toBeDefined()
  })

  test("shares one install directory between unversioned and @latest specs", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const dir = path.join(cache, "packages", "fixture-latest")
    await seedInstall(dir, "fixture-latest")
    await Bun.write(path.join(dir, ".opencode-refresh"), "")

    const entries = await Effect.gen(function* () {
      const npm = yield* Npm.Service
      return [yield* npm.add("fixture-latest"), yield* npm.add("fixture-latest@latest")]
    }).pipe(Effect.scoped, Effect.provide(npmLayer(cache)), Effect.runPromise)

    expect(entries.map((entry) => entry.directory)).toEqual([
      path.join(dir, "node_modules", "fixture-latest"),
      path.join(dir, "node_modules", "fixture-latest"),
    ])
    expect(entries[0].entrypoint).toBeDefined()
    await expect(fs.stat(path.join(cache, "packages", "fixture-latest@latest"))).rejects.toThrow()
  })

  test("keeps the existing install when a stale @latest refresh fails", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const dir = path.join(cache, "packages", "fixture-stale")
    await seedInstall(dir, "fixture-stale")
    // An unreachable registry makes the refresh fail without touching the network.
    await Bun.write(path.join(dir, ".npmrc"), "registry=http://127.0.0.1:9/\nfetch-retries=0\n")
    await Bun.write(path.join(dir, ".opencode-refresh"), "")
    await setAge(path.join(dir, ".opencode-refresh"), 2)

    const entry = await Effect.gen(function* () {
      const npm = yield* Npm.Service
      return yield* npm.add("fixture-stale@latest")
    }).pipe(Effect.scoped, Effect.provide(npmLayer(cache)), Effect.runPromise)

    expect(entry.directory).toBe(path.join(dir, "node_modules", "fixture-stale"))
    expect(entry.entrypoint).toBeDefined()
    // The attempt is recorded so the next call does not retry immediately.
    const refreshed = await fs.stat(path.join(dir, ".opencode-refresh"))
    expect(Date.now() - refreshed.mtimeMs).toBeLessThan(DAY_MS)
  }, 30_000)

  // The Desktop sidecar runs the server under Node, where import.meta.resolve cannot take a
  // parent URL. Exercise the real Node branch instead of the Bun one the test runner uses.
  test("resolves an importable file URL under Node", async () => {
    await using tmp = await tmpdir()
    const node = which("node")
    if (!node) throw new Error("Node is required for the Npm Node runtime test")

    const bundle = await Bun.build({
      entrypoints: [path.join(import.meta.dir, "../src/npm.ts")],
      target: "node",
      format: "esm",
    })
    expect(bundle.success).toBe(true)
    const entry = path.join(tmp.path, "npm.mjs")
    await Bun.write(entry, bundle.outputs[0])

    const dual = path.join(tmp.path, "dual-provider")
    await writePackage(dual, {
      name: "dual-provider",
      exports: { ".": { import: "./dist/index.mjs", require: "./dist/index.js" } },
    })
    await Bun.write(path.join(dual, "dist", "index.mjs"), "export const createDual = () => 'esm'\n")
    await Bun.write(path.join(dual, "dist", "index.js"), "exports.createDual = () => 'cjs'\n")

    const scoped = path.join(tmp.path, "scoped-provider")
    await writePackage(scoped, {
      name: "@fixture/scoped-provider",
      type: "module",
      exports: "./dist/index.js",
    })
    await Bun.write(path.join(scoped, "dist", "index.js"), "export const createScoped = () => 'scoped'\n")

    const proc = Bun.spawn(
      [
        node,
        "--input-type=module",
        "-e",
        `
        import assert from "node:assert/strict"
        import { Npm } from ${JSON.stringify(pathToFileURL(entry).href)}
        assert.equal(typeof Bun, "undefined")
        for (const [spec, name] of [
          [${JSON.stringify(`dual-provider@file:${dual}`)}, "createDual"],
          [${JSON.stringify(`@fixture/scoped-provider@file:${scoped}`)}, "createScoped"],
        ]) {
          const result = await Npm.add(spec)
          assert.ok(result.entrypoint?.startsWith("file://"), "entrypoint is a file URL: " + result.entrypoint)
          const mod = await import(result.entrypoint)
          assert.equal(typeof mod[name], "function", "module exports " + name)
        }
        process.exit(0)
      `,
      ],
      {
        env: { ...process.env, XDG_CACHE_HOME: path.join(tmp.path, "cache") },
        stdout: "pipe",
        stderr: "pipe",
      },
    )
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ])
    expect(stderr, stdout).toBe("")
    expect(code).toBe(0)
  }, 30_000)
})

describe("Npm.install", () => {
  test("respects omit from project .npmrc", async () => {
    await using tmp = await tmpdir()

    await writePackage(tmp.path, {
      name: "fixture",
      dependencies: {
        "prod-pkg": "file:./prod-pkg",
      },
      devDependencies: {
        "dev-pkg": "file:./dev-pkg",
      },
    })
    await Bun.write(path.join(tmp.path, ".npmrc"), "omit=dev\n")
    await fs.mkdir(path.join(tmp.path, "prod-pkg"))
    await fs.mkdir(path.join(tmp.path, "dev-pkg"))
    await writePackage(path.join(tmp.path, "prod-pkg"), { name: "prod-pkg" })
    await writePackage(path.join(tmp.path, "dev-pkg"), { name: "dev-pkg" })

    await Npm.install(tmp.path)

    await expect(fs.stat(path.join(tmp.path, "node_modules", "prod-pkg"))).resolves.toBeDefined()
    await expect(fs.stat(path.join(tmp.path, "node_modules", "dev-pkg"))).rejects.toThrow()
  })

  test("does not reinstall what a concurrent install finished while it waited for the lock", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const dir = path.join(tmp.path, "project")
    const lock = { packages: { "": { dependencies: { "fixture-dep": "file:./missing" } } } }
    await fs.mkdir(dir)
    const install = Effect.gen(function* () {
      const npm = yield* Npm.Service
      yield* npm.install(dir, { add: [{ name: "fixture-dep", version: "file:./missing" }] })
    }).pipe(Effect.scoped, Effect.provide(npmLayer(cache)))

    await Effect.gen(function* () {
      const flock = yield* EffectFlock.Service
      const fiber = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* flock.acquire(`npm-install:${dir}`)
          const fiber = yield* Effect.forkDetach(install)
          yield* Effect.sleep("200 millis")
          yield* Effect.promise(async () => {
            await fs.mkdir(path.join(dir, "node_modules"))
            await writePackage(dir, { name: "project", dependencies: { "fixture-dep": "file:./missing" } })
            await Bun.write(path.join(dir, "package-lock.json"), JSON.stringify(lock))
          })
          return fiber
        }),
      )
      yield* Fiber.join(fiber)
    }).pipe(
      Effect.provide(
        AppNodeBuilder.build(EffectFlock.node, [
          [Global.node, Global.layerWith({ cache, state: path.join(cache, "state") })],
        ]),
      ),
      Effect.runPromise,
    )

    // A reinstall would rewrite the lockfile and link the dependency.
    expect(await Bun.file(path.join(dir, "package-lock.json")).json()).toEqual(lock)
    await expect(fs.lstat(path.join(dir, "node_modules", "fixture-dep"))).rejects.toThrow()
  })
})

describe("Npm.sweep", () => {
  const sweepLayer = (cache: string) =>
    AppNodeBuilder.build(LayerNode.group([Global.node, FSUtil.node, EffectFlock.node]), [
      [Global.node, Global.layerWith({ cache, state: path.join(cache, "state") })],
    ])

  test("removes installs unused for 30 days and keeps recently used ones", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const packages = path.join(cache, "packages")
    const stale = path.join(packages, "stale-pkg")
    const scoped = path.join(packages, "@scope", "legacy-pkg")
    const fresh = path.join(packages, "fresh-pkg")
    const reused = path.join(packages, "reused-pkg")
    await seedInstall(stale, "stale-pkg")
    await Bun.write(path.join(stale, ".opencode-used"), "")
    await setAge(path.join(stale, ".opencode-used"), 31)
    // Installs from before the marker existed are aged by their directory mtime.
    await seedInstall(scoped, "@scope/legacy-pkg")
    await setAge(scoped, 31)
    await seedInstall(fresh, "fresh-pkg")
    await Bun.write(path.join(fresh, ".opencode-used"), "")
    // An old directory with a recent use marker is still in use.
    await seedInstall(reused, "reused-pkg")
    await Bun.write(path.join(reused, ".opencode-used"), "")
    await setAge(reused, 90)

    await Npm.sweep().pipe(Effect.provide(sweepLayer(cache)), Effect.runPromise)

    await expect(fs.stat(stale)).rejects.toThrow()
    await expect(fs.stat(scoped)).rejects.toThrow()
    await expect(fs.stat(fresh)).resolves.toBeDefined()
    await expect(fs.stat(reused)).resolves.toBeDefined()
  })

  test("waits for the install lock and re-checks before removing", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const dir = path.join(cache, "packages", "busy-pkg")
    await seedInstall(dir, "busy-pkg")
    await Bun.write(path.join(dir, ".opencode-used"), "")
    await setAge(path.join(dir, ".opencode-used"), 31)

    await Effect.gen(function* () {
      const flock = yield* EffectFlock.Service
      const fiber = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* flock.acquire(`npm-install:${dir}`)
          const fiber = yield* Effect.forkDetach(Npm.sweep())
          yield* Effect.sleep("200 millis")
          // The lock holder uses the install before releasing it.
          yield* Effect.promise(() => Bun.write(path.join(dir, ".opencode-used"), ""))
          return fiber
        }),
      )
      yield* Fiber.join(fiber)
    }).pipe(Effect.provide(sweepLayer(cache)), Effect.runPromise)

    await expect(fs.stat(dir)).resolves.toBeDefined()
  })
})
