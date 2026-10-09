export * as Npm from "./npm"

import path from "path"
import { createRequire } from "module"
import { pathToFileURL } from "url"
import npa from "npm-package-arg"
import { Cause, Duration, Effect, Schema, Context, Layer, Option, FileSystem, Schedule } from "effect"
import { NodeFileSystem } from "@effect/platform-node"
import { FSUtil } from "./fs-util"
import { Global } from "./global"
import { EffectFlock } from "./util/effect-flock"
import { makeGlobalNode } from "./effect/app-node"
import { filesystem } from "./effect/app-node-platform"
import { LayerNode } from "./effect/layer-node"
import { makeRuntime } from "./effect/runtime"
import { NpmConfig } from "./npm-config"

export class InstallFailedError extends Schema.TaggedError<InstallFailedError>()("NpmInstallFailedError", {
  add: Schema.Array(Schema.String).pipe(Schema.optional),
  dir: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

export interface EntryPoint {
  readonly directory: string
  readonly entrypoint?: string
}

export interface Interface {
  readonly add: (pkg: string) => Effect.Effect<EntryPoint, InstallFailedError | EffectFlock.LockError>
  readonly install: (
    dir: string,
    input?: {
      add: {
        name: string
        version?: string
      }[]
    },
  ) => Effect.Effect<void, EffectFlock.LockError | InstallFailedError>
  readonly which: (pkg: string, bin?: string) => Effect.Effect<string | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Npm") {}

const USED_MARKER = ".opencode-used"
const REFRESH_MARKER = ".opencode-refresh"
const REFRESH_AFTER = Duration.hours(24)
const UNUSED_AFTER = Duration.days(30)

const illegal = process.platform === "win32" ? new Set(["<", ">", ":", '"', "|", "?", "*"]) : undefined

export function sanitize(pkg: string) {
  if (!illegal) return pkg
  return Array.from(pkg, (char) => (illegal.has(char) || char.charCodeAt(0) < 32 ? "_" : char)).join("")
}

const parse = (pkg: string) => {
  try {
    const parsed = npa(pkg)
    const latest =
      (parsed.type === "tag" && parsed.fetchSpec === "latest") || (parsed.type === "range" && parsed.rawSpec === "*")
    return {
      name: parsed.name ?? pkg,
      // Dist-tags and unversioned specs move over time, so their installs are refreshed periodically.
      floating: latest || parsed.type === "tag",
      // `pkg` and `pkg@latest` resolve identically, so they share one install directory.
      key: latest && parsed.name ? parsed.name : pkg,
    }
  } catch {
    return { name: pkg, floating: false, key: pkg }
  }
}

const ageOf = (fs: FSUtil.Interface, file: string) =>
  fs.stat(file).pipe(
    Effect.map((info) =>
      Option.match(info.mtime, {
        onNone: () => Number.POSITIVE_INFINITY,
        onSome: (date) => Date.now() - date.getTime(),
      }),
    ),
    Effect.orElseSucceed(() => Number.POSITIVE_INFINITY),
  )

const resolveEntryPoint = (name: string, dir: string): EntryPoint => {
  let entrypoint: string | undefined
  try {
    // Node only honors the parent argument behind --experimental-import-meta-resolve, and
    // import() of the bare package directory fails with ERR_UNSUPPORTED_DIR_IMPORT. require
    // resolution picks the "require"/"default" export target, which import() loads fine.
    entrypoint =
      typeof Bun !== "undefined"
        ? import.meta.resolve(name, dir)
        : pathToFileURL(createRequire(path.join(dir, "package.json")).resolve(name)).href
  } catch {
    entrypoint = undefined
  }
  return {
    directory: dir,
    entrypoint,
  }
}

interface ArboristNode {
  name: string
  path: string
}

interface ArboristTree {
  edgesOut: Map<string, { to?: ArboristNode }>
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const afs = yield* FSUtil.Service
    const global = yield* Global.Service
    const fs = yield* FileSystem.FileSystem
    const flock = yield* EffectFlock.Service
    const directory = (pkg: string) => path.join(global.cache, "packages", sanitize(parse(pkg).key))
    // Read by the sweep to find installs nobody has used for a while.
    const markUsed = (dir: string) => afs.writeWithDirs(path.join(dir, USED_MARKER), "").pipe(Effect.ignore)
    const current = Effect.fnUntraced(function* (dir: string, name: string, floating: boolean) {
      if (!(yield* afs.existsSafe(path.join(dir, "node_modules", name)))) return false
      if (!floating) return true
      return (yield* ageOf(afs, path.join(dir, REFRESH_MARKER))) < Duration.toMillis(REFRESH_AFTER)
    })
    const reify = (input: { dir: string; add?: string[] }) =>
      Effect.gen(function* () {
        const { Arborist } = yield* Effect.promise(() => import("@npmcli/arborist"))
        const add = input.add ?? []
        const npmOptions = yield* NpmConfig.load(input.dir)
        const arborist = new Arborist({
          ...npmOptions,
          path: input.dir,
          binLinks: true,
          progress: false,
          savePrefix: "",
          ignoreScripts: true,
        })
        return yield* Effect.tryPromise({
          try: () =>
            arborist.reify({
              ...npmOptions,
              add,
              save: true,
              saveType: "prod",
            }),
          catch: (cause) =>
            new InstallFailedError({
              cause,
              add,
              dir: input.dir,
            }),
        }) as Effect.Effect<ArboristTree, InstallFailedError>
      }).pipe(
        Effect.withSpan("Npm.reify", {
          attributes: input,
        }),
      )

    const add = Effect.fn("Npm.add")(function* (pkg: string) {
      const dir = directory(pkg)
      const spec = parse(pkg)
      const target = path.join(dir, "node_modules", spec.name)
      yield* markUsed(dir)

      if (yield* current(dir, spec.name, spec.floating)) return resolveEntryPoint(spec.name, target)

      yield* flock.acquire(`npm-install:${dir}`)
      // Another fiber or process may have installed the package while this one waited for the lock.
      if (yield* current(dir, spec.name, spec.floating)) return resolveEntryPoint(spec.name, target)

      const installed = yield* afs.existsSafe(target)
      const tree = yield* reify({ dir, add: [pkg] }).pipe(
        // A failed refresh keeps serving the previous install instead of breaking the caller.
        Effect.catchIf(
          () => installed,
          (error) =>
            Effect.logWarning("npm refresh failed; using existing install", { pkg, cause: error.cause }).pipe(
              Effect.as(undefined),
            ),
        ),
      )
      // Records the attempt even on failure so an offline machine retries once per window, not on every call.
      yield* afs.writeFileString(path.join(dir, REFRESH_MARKER), "").pipe(Effect.ignore)
      if (!tree) return resolveEntryPoint(spec.name, target)
      const first = tree.edgesOut.values().next().value?.to
      if (!first) {
        const result = resolveEntryPoint(spec.name, target)
        if (result.entrypoint) return result
        return yield* new InstallFailedError({ add: [pkg], dir })
      }
      return resolveEntryPoint(first.name, first.path)
    }, Effect.scoped)

    const needsInstall = Effect.fn("Npm.needsInstall")(function* (
      dir: string,
      input: Parameters<Interface["install"]>[1],
    ) {
      if (!(yield* afs.existsSafe(path.join(dir, "node_modules")))) return true

      const pkg = yield* afs.readJson(path.join(dir, "package.json")).pipe(Effect.orElseSucceed(() => ({})))
      const lock = yield* afs.readJson(path.join(dir, "package-lock.json")).pipe(Effect.orElseSucceed(() => ({})))

      const pkgAny = pkg as any
      const lockAny = lock as any
      const declared = new Set([
        ...Object.keys(pkgAny?.dependencies || {}),
        ...Object.keys(pkgAny?.devDependencies || {}),
        ...Object.keys(pkgAny?.peerDependencies || {}),
        ...Object.keys(pkgAny?.optionalDependencies || {}),
        ...(input?.add || []).map((pkg) => pkg.name),
      ])

      const root = lockAny?.packages?.[""] || {}
      const locked = new Set([
        ...Object.keys(root?.dependencies || {}),
        ...Object.keys(root?.devDependencies || {}),
        ...Object.keys(root?.peerDependencies || {}),
        ...Object.keys(root?.optionalDependencies || {}),
      ])

      return [...declared].some((name) => !locked.has(name))
    })

    const install: Interface["install"] = Effect.fn("Npm.install")(function* (dir, input) {
      const canWrite = yield* afs.access(dir, { writable: true }).pipe(
        Effect.as(true),
        Effect.orElseSucceed(() => false),
      )
      if (!canWrite) return

      if (!(yield* needsInstall(dir, input))) return
      yield* flock.acquire(`npm-install:${dir}`)
      // Another fiber or process may have finished the install while this one waited for the lock.
      if (!(yield* needsInstall(dir, input))) return
      yield* reify({ dir, add: input?.add.map((pkg) => [pkg.name, pkg.version].filter(Boolean).join("@")) ?? [] })
    }, Effect.scoped)

    const which = Effect.fn("Npm.which")(function* (pkg: string, bin?: string) {
      const dir = directory(pkg)
      const binDir = path.join(dir, "node_modules", ".bin")
      yield* markUsed(dir)

      const pick = Effect.fnUntraced(function* () {
        const files = yield* fs.readDirectory(binDir).pipe(Effect.catch(() => Effect.succeed([] as string[])))

        if (files.length === 0) return Option.none<string>()
        // Caller picked a specific bin (e.g. pyright exposes both `pyright` and
        // `pyright-langserver`); trust the hint if the package provides it.
        if (bin) return files.includes(bin) ? Option.some(bin) : Option.none<string>()
        if (files.length === 1) return Option.some(files[0])

        const pkgJson = yield* afs.readJson(path.join(dir, "node_modules", pkg, "package.json")).pipe(Effect.option)

        if (Option.isSome(pkgJson)) {
          const parsed = pkgJson.value as { bin?: string | Record<string, string> }
          if (parsed?.bin) {
            const unscoped = pkg.startsWith("@") ? pkg.split("/")[1] : pkg
            const parsedBin = parsed.bin
            if (typeof parsedBin === "string") return Option.some(unscoped)
            const keys = Object.keys(parsedBin)
            if (keys.length === 1) return Option.some(keys[0])
            return parsedBin[unscoped] ? Option.some(unscoped) : Option.some(keys[0])
          }
        }

        return Option.some(files[0])
      })

      return Option.getOrUndefined(
        yield* Effect.gen(function* () {
          const bin = yield* pick()
          if (Option.isSome(bin)) {
            return Option.some(path.join(binDir, bin.value))
          }

          yield* fs.remove(path.join(dir, "package-lock.json")).pipe(Effect.orElseSucceed(() => {}))

          yield* add(pkg)

          const resolved = yield* pick()
          if (Option.isNone(resolved)) return Option.none<string>()
          return Option.some(path.join(binDir, resolved.value))
        }).pipe(
          Effect.scoped,
          Effect.orElseSucceed(() => Option.none<string>()),
        ),
      )
    })

    yield* sweep().pipe(
      Effect.catchCause((cause) => Effect.logError("npm cache sweep failed", { cause: Cause.pretty(cause) })),
      Effect.repeat(Schedule.spaced(Duration.hours(1))),
      Effect.delay(Duration.minutes(5)),
      Effect.forkScoped,
    )

    return Service.of({
      add,
      install,
      which,
    })
  }),
)

/** Removes cached package installs that no process has used within UNUSED_AFTER. */
export const sweep = Effect.fn("Npm.sweep")(function* () {
  const fs = yield* FSUtil.Service
  const global = yield* Global.Service
  const flock = yield* EffectFlock.Service
  const unused = (dir: string) =>
    Effect.gen(function* () {
      const marker = path.join(dir, USED_MARKER)
      // Installs from before the marker existed fall back to the directory mtime.
      const age = (yield* fs.existsSafe(marker)) ? yield* ageOf(fs, marker) : yield* ageOf(fs, dir)
      return age > Duration.toMillis(UNUSED_AFTER)
    })
  for (const dir of yield* packageDirs(fs, path.join(global.cache, "packages"), 8)) {
    if (!(yield* unused(dir))) continue
    // The install lock keeps the sweep from deleting a directory another process is installing into.
    yield* Effect.gen(function* () {
      yield* flock.acquire(`npm-install:${dir}`)
      if (!(yield* unused(dir))) return
      yield* fs.remove(dir, { recursive: true })
    }).pipe(Effect.scoped, Effect.ignore)
  }
})

// Package directories nest under `@scope/` (and under URL path segments for git specs), so walk
// until a directory looks like an install root.
const packageDirs = (fs: FSUtil.Interface, dir: string, depth: number): Effect.Effect<string[]> =>
  fs.readDirectoryEntries(dir).pipe(
    Effect.orElseSucceed((): FSUtil.DirEntry[] => []),
    Effect.flatMap((entries) =>
      Effect.forEach(
        entries.filter((entry) => entry.type === "directory"),
        (entry) =>
          Effect.gen(function* () {
            const child = path.join(dir, entry.name)
            const roots = yield* Effect.forEach([USED_MARKER, "package.json", "node_modules"], (name) =>
              fs.existsSafe(path.join(child, name)),
            )
            if (roots.some(Boolean)) return [child]
            if (depth <= 1) return []
            return yield* packageDirs(fs, child, depth - 1)
          }),
      ),
    ),
    Effect.map((dirs) => dirs.flat()),
  )

export const node = makeGlobalNode({
  service: Service,
  layer: layer,
  deps: [FSUtil.node, Global.node, filesystem, EffectFlock.node],
})

const { runPromise } = makeRuntime(Service, LayerNode.compile(node))

export async function install(...args: Parameters<Interface["install"]>) {
  return runPromise((svc) => svc.install(...args))
}

export async function add(...args: Parameters<Interface["add"]>) {
  return runPromise((svc) => svc.add(...args))
}

export async function which(...args: Parameters<Interface["which"]>) {
  return runPromise((svc) => svc.which(...args))
}
