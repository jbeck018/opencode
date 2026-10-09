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

const sweepLayer = (cache: string) =>
  AppNodeBuilder.build(LayerNode.group([Global.node, FSUtil.node, EffectFlock.node]), [
    [Global.node, Global.layerWith({ cache, state: path.join(cache, "state") })],
  ])

// A local registry whose `latest` is 2.0.0; `fail` makes it reject requests and `onTarball` runs
// while a reify is mid-download.
const serveRegistry = async (root: string, name: string) => {
  const source = path.join(root, "registry", "package")
  await writePackage(source, { name, version: "2.0.0", main: "index.js" })
  await Bun.write(path.join(source, "index.js"), 'export const version = "2.0.0"\n')
  const tarball = path.join(root, "registry", "package.tgz")
  expect(await Bun.spawn(["tar", "-czf", tarball, "-C", path.dirname(source), "package"]).exited).toBe(0)
  const state = {
    url: "",
    fail: false,
    requests: 0,
    onTarball: async () => {},
    server: undefined as ReturnType<typeof Bun.serve> | undefined,
    async [Symbol.asyncDispose]() {
      await state.server?.stop(true)
    },
  }
  state.server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch: async (request) => {
      const headers = { "cache-control": "no-store" }
      state.requests++
      if (state.fail) return new Response("unavailable", { status: 500, headers })
      if (new URL(request.url).pathname.endsWith(".tgz")) {
        await state.onTarball()
        return new Response(Bun.file(tarball), { headers })
      }
      return Response.json(
        {
          name,
          "dist-tags": { latest: "2.0.0" },
          versions: {
            "2.0.0": {
              name,
              version: "2.0.0",
              main: "index.js",
              dist: { tarball: `${state.url}/${name}/-/${name}-2.0.0.tgz` },
            },
          },
        },
        { headers },
      )
    },
  })
  state.url = `http://127.0.0.1:${state.server.port}`
  return state
}

// Background refreshes finish on their own schedule, so tests poll for their outcome.
const waitFor = async (check: () => Promise<boolean>) => {
  const deadline = Date.now() + 20_000
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("timed out waiting for condition")
    await Bun.sleep(50)
  }
}

const refreshedRecently = (dir: string) =>
  fs.stat(path.join(dir, ".opencode-refresh")).then(
    (info) => Date.now() - info.mtimeMs < DAY_MS,
    () => false,
  )

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
      const entry = yield* npm.add("fixture-stale@latest")
      // The attempt is recorded so the next call does not retry immediately.
      yield* Effect.promise(() => waitFor(() => refreshedRecently(dir)))
      return entry
    }).pipe(Effect.scoped, Effect.provide(npmLayer(cache)), Effect.runPromise)

    expect(entry.directory).toBe(path.join(dir, "node_modules", "fixture-stale"))
    expect(entry.entrypoint).toBeDefined()
    // The failed version directory is discarded and nothing was published.
    expect(
      (await fs.readdir(dir)).filter((name) => name.startsWith("v-") || name.startsWith(".opencode-current")),
    ).toEqual([])
  }, 30_000)

  test("refreshes @latest in the background, publishes atomically, keeps the old version through the grace period, and survives a failed refresh", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const dir = path.join(cache, "packages", "fixture-swap")
    const legacyIndex = path.join(dir, "node_modules", "fixture-swap", "index.js")
    await seedInstall(dir, "fixture-swap")
    await Bun.write(path.join(dir, ".opencode-refresh"), "")
    await setAge(path.join(dir, ".opencode-refresh"), 2)

    await using registry = await serveRegistry(tmp.path, "fixture-swap")
    await Bun.write(
      path.join(dir, ".npmrc"),
      `registry=${registry.url}/\nfetch-retries=0\ncache=${path.join(tmp.path, "npm-cache")}\n`,
    )

    // What a concurrent reader sees while the refresh is still downloading the new version. The
    // download stays blocked until the test releases it, so `add` can only return by not waiting.
    const midway: { pointer: boolean; legacy: string }[] = []
    const download = Promise.withResolvers<void>()
    registry.onTarball = async () => {
      midway.push({
        pointer: await Bun.file(path.join(dir, ".opencode-current")).exists(),
        legacy: await Bun.file(legacyIndex).text(),
      })
      await download.promise
    }

    const pointer = () => Bun.file(path.join(dir, ".opencode-current"))
    const result = await Effect.gen(function* () {
      const npm = yield* Npm.Service
      const add = () => npm.add("fixture-swap@latest")
      const wait = (check: () => Promise<boolean>) => Effect.promise(() => waitFor(check))

      // The stale install is served at once while the refresh runs behind it.
      const served = yield* add()
      yield* wait(async () => midway.length > 0)
      const during = yield* add()
      download.resolve()
      yield* wait(() => pointer().exists())
      const refreshed = yield* add()

      // A failed refresh leaves the published version in place.
      registry.fail = true
      yield* Effect.promise(() => setAge(path.join(dir, ".opencode-refresh"), 2))
      const kept = yield* add()
      yield* wait(() => refreshedRecently(dir))
      return { served, during, refreshed, kept }
    }).pipe(Effect.scoped, Effect.provide(npmLayer(cache)), Effect.runPromise)

    expect(result.served.directory).toBe(path.join(dir, "node_modules", "fixture-swap"))
    expect(result.during.directory).toBe(result.served.directory)
    expect(midway).toEqual([{ pointer: false, legacy: "export const fixture = true\n" }])
    const version = (await pointer().text()).trim()
    expect(version).toStartWith("v-")
    const refreshed = result.refreshed
    expect(refreshed.directory).toBe(path.join(dir, version, "node_modules", "fixture-swap"))
    expect((await import(refreshed.entrypoint!)).version).toBe("2.0.0")
    // The legacy layout is the previous version and stays readable for processes that resolved it.
    expect(await Bun.file(legacyIndex).text()).toBe("export const fixture = true\n")

    expect(result.kept.directory).toBe(refreshed.directory)
    expect((await fs.readdir(dir)).filter((name) => name.startsWith("v-"))).toEqual([version])
    // Pruning under the refresh lock leaves superseded versions alone inside the grace period.
    expect(await Bun.file(legacyIndex).exists()).toBe(true)

    const add = () =>
      Effect.gen(function* () {
        const npm = yield* Npm.Service
        return yield* npm.add("fixture-swap@latest")
      }).pipe(Effect.scoped, Effect.provide(npmLayer(cache)), Effect.runPromise)

    // Superseded versions and the migrated legacy files go once the pointer is older than the grace period.
    await fs.mkdir(path.join(dir, "v-0"))
    await setAge(path.join(dir, ".opencode-current"), 2)
    await Npm.sweep().pipe(Effect.provide(sweepLayer(cache)), Effect.runPromise)
    expect((await fs.readdir(dir)).sort()).toEqual(
      [".npmrc", ".opencode-current", ".opencode-refresh", ".opencode-used", version].sort(),
    )
    expect((await add()).directory).toBe(refreshed.directory)
  }, 60_000)

  test("never refreshes a bare package name once it is installed", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const dir = path.join(cache, "packages", "fixture-pinned")
    await seedInstall(dir, "fixture-pinned")
    await Bun.write(path.join(dir, ".opencode-refresh"), "")
    await setAge(path.join(dir, ".opencode-refresh"), 2)
    await using registry = await serveRegistry(tmp.path, "fixture-pinned")
    await Bun.write(path.join(dir, ".npmrc"), `registry=${registry.url}/\nfetch-retries=0\n`)

    const entries = await Effect.gen(function* () {
      const npm = yield* Npm.Service
      const entries = [yield* npm.add("fixture-pinned"), yield* npm.add("fixture-pinned")]
      // Leave room for a refresh fiber to reach the registry if one had been started.
      yield* Effect.sleep("500 millis")
      return entries
    }).pipe(Effect.scoped, Effect.provide(npmLayer(cache)), Effect.runPromise)

    expect(entries.map((entry) => entry.directory)).toEqual([
      path.join(dir, "node_modules", "fixture-pinned"),
      path.join(dir, "node_modules", "fixture-pinned"),
    ])
    expect(registry.requests).toBe(0)
    expect((await fs.readdir(dir)).filter((name) => name.startsWith("v-"))).toEqual([])
  })

  test("adopts a legacy @latest install directory instead of installing from the network", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const name = "opencode-fixture-legacy-adopt"
    const dir = path.join(cache, "packages", name)
    const legacy = path.join(cache, "packages", `${name}@latest`)
    await seedInstall(legacy, name)
    // A recent refresh keeps the background refresh away from the network; adoption itself never uses it.
    await Bun.write(path.join(dir, ".opencode-refresh"), "")

    const entry = await Effect.gen(function* () {
      const npm = yield* Npm.Service
      return yield* npm.add(`${name}@latest`)
    }).pipe(Effect.scoped, Effect.provide(npmLayer(cache)), Effect.runPromise)

    const version = (await Bun.file(path.join(dir, ".opencode-current")).text()).trim()
    expect(version).toStartWith("v-")
    expect(entry.directory).toBe(path.join(dir, version, "node_modules", name))
    expect((await import(entry.entrypoint!)).fixture).toBe(true)
    await expect(fs.stat(legacy)).rejects.toThrow()
  })

  test("resolves entrypoints and bins through the current version", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const dir = path.join(cache, "packages", "fixture-bin")
    await seedInstall(path.join(dir, "v-1"), "fixture-bin")
    await seedInstall(path.join(dir, "v-2"), "fixture-bin")
    await Bun.write(path.join(dir, "v-2", "node_modules", ".bin", "fixture-bin"), "#!/bin/sh\n")
    await Bun.write(path.join(dir, ".opencode-current"), "v-2")
    await Bun.write(path.join(dir, ".opencode-refresh"), "")

    const result = await Effect.gen(function* () {
      const npm = yield* Npm.Service
      return { entry: yield* npm.add("fixture-bin"), bin: yield* npm.which("fixture-bin") }
    }).pipe(Effect.scoped, Effect.provide(npmLayer(cache)), Effect.runPromise)

    expect(result.entry.directory).toBe(path.join(dir, "v-2", "node_modules", "fixture-bin"))
    expect(result.entry.entrypoint).toBeDefined()
    expect(result.bin).toBe(path.join(dir, "v-2", "node_modules", ".bin", "fixture-bin"))
  })

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
    // Installs from before the marker existed, however old, get a marker instead of being removed.
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
    expect(Date.now() - (await fs.stat(path.join(scoped, ".opencode-used"))).mtimeMs).toBeLessThan(DAY_MS)
    await expect(fs.stat(fresh)).resolves.toBeDefined()
    await expect(fs.stat(reused)).resolves.toBeDefined()

    // The written marker starts the unused window, so the install goes once that window passes.
    await setAge(path.join(scoped, ".opencode-used"), 31)
    await Npm.sweep().pipe(Effect.provide(sweepLayer(cache)), Effect.runPromise)
    await expect(fs.stat(scoped)).rejects.toThrow()
  })

  test("renews markers for and never removes what the calling process resolved", async () => {
    await using tmp = await tmpdir()
    const cache = path.join(tmp.path, "cache")
    const dir = path.join(cache, "packages", "loaded-pkg")
    await seedInstall(path.join(dir, "v-1"), "loaded-pkg")
    await seedInstall(path.join(dir, "v-2"), "loaded-pkg")
    await seedInstall(path.join(dir, "v-3"), "loaded-pkg")
    await Bun.write(path.join(dir, ".opencode-current"), "v-3")
    await setAge(path.join(dir, ".opencode-current"), 2)
    await Bun.write(path.join(dir, ".opencode-used"), "")
    await setAge(path.join(dir, ".opencode-used"), 31)

    // This process loaded v-1 at startup, long before v-3 superseded it.
    await Npm.sweep({ dirs: new Set([dir]), roots: new Set([path.join(dir, "v-1")]) }).pipe(
      Effect.provide(sweepLayer(cache)),
      Effect.runPromise,
    )

    expect(Date.now() - (await fs.stat(path.join(dir, ".opencode-used"))).mtimeMs).toBeLessThan(DAY_MS)
    expect((await fs.readdir(dir)).filter((name) => name.startsWith("v-")).sort()).toEqual(["v-1", "v-3"])
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
