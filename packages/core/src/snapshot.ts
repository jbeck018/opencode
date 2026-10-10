export * as Snapshot from "./snapshot"

import { makeLocationNode } from "./effect/app-node"
import path from "path"
import { Context, Duration, Effect, Layer, Schedule, Schema } from "effect"
import { Config } from "./config"
import { File } from "./file"
import { FSUtil } from "./fs-util"
import { Git } from "./git"
import { Global } from "./global"
import { Location } from "./location"
import { AbsolutePath, RelativePath } from "./schema"
import { SnapshotRepo } from "./snapshot-repo"
import { EffectFlock } from "./util/effect-flock"
import { Hash } from "./util/hash"

export const ID = Schema.String.pipe(Schema.brand("Snapshot.ID"))
export type ID = typeof ID.Type

export class Error extends Schema.TaggedError<Error>()("Snapshot.Error", {
  operation: Schema.Literals(["capture", "files", "diff", "preview", "restore"]),
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

export interface CompareInput {
  readonly from: ID
  readonly to: ID
}

export interface DiffInput extends CompareInput {
  readonly context?: number
  readonly paths?: readonly RelativePath[]
}

export interface RestoreInput {
  /** Paths are relative to the project root. */
  readonly files: ReadonlyMap<RelativePath, ID>
}

export interface PreviewInput extends RestoreInput {
  readonly context?: number
}

export interface Interface {
  /**
   * Capture the current Location-scoped filesystem state as a content-addressed
   * tree. Returns `undefined` when snapshots are disabled, unsupported, or the
   * best-effort capture fails.
   */
  readonly capture: () => Effect.Effect<ID | undefined>

  /**
   * List project-relative paths changed between two captured trees without
   * loading file contents or generating patches.
   */
  readonly files: (input: CompareInput) => Effect.Effect<readonly RelativePath[], Error>

  /**
   * Generate structured per-file diffs between two captured trees. `context`
   * controls unchanged lines around each unified diff hunk.
   */
  readonly diff: (input: DiffInput) => Effect.Effect<readonly File.Diff[], Error>

  /**
   * Preview the filesystem result of a selective restore without modifying the
   * worktree. Each project-relative path maps to the tree it would be restored
   * from.
   */
  readonly preview: (input: PreviewInput) => Effect.Effect<readonly File.Diff[], Error>

  /**
   * Restore selected project-relative paths from their associated trees. A path
   * absent from its selected tree is removed; paths outside the map are untouched.
   */
  readonly restore: (input: RestoreInput) => Effect.Effect<void, Error>

  /**
   * Replace the snapshot index with a captured tree and check out all its entries.
   * Files absent from the tree remain untouched. Prefer selective `restore` when
   * only known paths should change.
   */
  readonly checkout: (snapshot: ID) => Effect.Effect<void, Error>

  /**
   * Compact the snapshot repository and prune unreachable objects. Runs hourly
   * while the Location is loaded. Captured trees older than the prune window
   * are deleted, so undo/restore cannot reach past it.
   */
  readonly cleanup: () => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/Snapshot") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = yield* Config.Service
    const fs = yield* FSUtil.Service
    const git = yield* Git.Service
    const global = yield* Global.Service
    const location = yield* Location.Service
    const flock = yield* EffectFlock.Service
    const source = yield* git.repo.discover(location.project.directory)
    const worktree = source
      ? AbsolutePath.make(yield* fs.realPath(source.worktree).pipe(Effect.orDie))
      : location.project.directory
    const gitDirectory = AbsolutePath.make(path.join(global.data, "snapshot", location.project.id, Hash.fast(worktree)))

    // Records the worktree so the v1 sweep can tell when it is gone.
    const record = SnapshotRepo.record(fs, gitDirectory, worktree)

    const scope = Effect.fnUntraced(function* () {
      const relative = path.relative(worktree, location.directory)
      if (relative.startsWith("..") || path.isAbsolute(relative))
        return yield* new Error({ operation: "capture", message: "Location is outside the project" })
      return RelativePath.make(relative.replaceAll("\\", "/") || ".")
    })

    // Writers of the repository (capture, preview, restore, checkout and its creation) hold the cross-process
    // lock that v1 snapshots share. EffectFlock is not re-entrant, so callers already holding it pass `held`.
    const locked = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(flock.withLock(SnapshotRepo.lockKey(gitDirectory), SnapshotRepo.lockOptions))

    const repository = Effect.fnUntraced(function* (held = false) {
      if (!source) return yield* new Error({ operation: "capture", message: "Project is not a Git repository" })
      const existing = new Git.Repository({
        worktree,
        gitDirectory,
        commonDirectory: gitDirectory,
      })
      if (yield* fs.existsSafe(path.join(gitDirectory, "HEAD"))) return existing
      const create = Effect.gen(function* () {
        // Another process may have created it while this one waited for the lock.
        if (yield* fs.existsSafe(path.join(gitDirectory, "HEAD"))) return existing
        return yield* git.repo
          .create({
            worktree,
            gitDirectory,
            seed: source,
          })
          .pipe(Effect.tap(() => record))
      })
      return yield* (held ? create : locked(create)).pipe(Effect.mapError((cause) => failure("capture", cause)))
    })

    const enabled = Effect.fnUntraced(function* () {
      if (location.vcs?.type !== "git") return false
      return Config.latest(yield* config.entries(), "snapshots") !== false
    })

    const capture = Effect.fn("Snapshot.capture")(function* () {
      if (!(yield* enabled())) return undefined
      return yield* Effect.gen(function* () {
        const repo = yield* repository(true)
        return ID.make(
          yield* git.tree.capture({
            repository: repo,
            scopes: [yield* scope()],
            ignores: source,
            maximumUntrackedFileBytes: 2 * 1024 * 1024,
          }),
        )
      }).pipe(
        locked,
        Effect.catch((cause) => Effect.logWarning("failed to capture snapshot", { cause }).pipe(Effect.as(undefined))),
      )
    })

    const compare = Effect.fnUntraced(function* (operation: "files" | "diff", input: CompareInput) {
      const repo = yield* repository().pipe(Effect.mapError((cause) => failure(operation, cause)))
      return { repository: repo, from: Git.TreeID.make(input.from), to: Git.TreeID.make(input.to) }
    })

    const files = Effect.fn("Snapshot.files")(function* (input: CompareInput) {
      const comparison = yield* compare("files", input)
      const files = yield* git.tree.files(comparison).pipe(Effect.mapError((cause) => failure("files", cause)))
      if (!source) return files
      const ignored = yield* git.index
        .ignored({ repository: source, paths: files })
        .pipe(Effect.mapError((cause) => failure("files", cause)))
      return files.filter((file) => !ignored.has(file))
    })

    const diff = Effect.fn("Snapshot.diff")(function* (input: DiffInput) {
      const comparison = yield* compare("diff", input)
      const files = yield* git.tree.files(comparison).pipe(Effect.mapError((cause) => failure("diff", cause)))
      const ignored = source
        ? yield* git.index
            .ignored({ repository: source, paths: files })
            .pipe(Effect.mapError((cause) => failure("diff", cause)))
        : new Set<RelativePath>()
      return yield* git.tree
        .diff({
          ...comparison,
          context: input.context,
          paths: (input.paths ?? files).filter((file) => !ignored.has(file)),
        })
        .pipe(Effect.mapError((cause) => failure("diff", cause)))
    })

    const plan = Effect.fnUntraced(function* (operation: "preview" | "restore", input: RestoreInput) {
      const files = new Map<RelativePath, Git.TreeID>()
      for (const [file, snapshot] of input.files) {
        const absolute = path.resolve(worktree, file)
        if (!FSUtil.contains(worktree, absolute))
          return yield* new Error({ operation, message: `Path escapes the project: ${file}` })
        files.set(file, Git.TreeID.make(snapshot))
      }
      return files
    })

    const preview = Effect.fn("Snapshot.preview")(function* (input: PreviewInput) {
      if (!(yield* enabled())) return yield* new Error({ operation: "preview", message: "Snapshots are disabled" })
      const files = yield* plan("preview", input)
      return yield* Effect.gen(function* () {
        const repo = yield* repository(true)
        const current = yield* git.tree.capture({
          repository: repo,
          scopes: Array.from(files.keys()),
          ignores: source,
          maximumUntrackedFileBytes: 2 * 1024 * 1024,
        })
        return yield* git.tree.preview({
          repository: repo,
          current,
          files,
          context: input.context,
        })
      }).pipe(
        locked,
        Effect.mapError((cause) => failure("preview", cause)),
      )
    })

    const restore = Effect.fn("Snapshot.restore")(function* (input: RestoreInput) {
      if (!(yield* enabled())) return yield* new Error({ operation: "restore", message: "Snapshots are disabled" })
      const files = yield* plan("restore", input)
      yield* Effect.gen(function* () {
        yield* git.tree.restore({ repository: yield* repository(true), files })
      }).pipe(
        locked,
        Effect.mapError((cause) => failure("restore", cause)),
      )
    })

    const checkout = Effect.fn("Snapshot.checkout")(function* (snapshot: ID) {
      yield* Effect.gen(function* () {
        yield* git.tree.checkout({ repository: yield* repository(true), tree: Git.TreeID.make(snapshot) })
      }).pipe(
        locked,
        Effect.mapError((cause) => failure("restore", cause)),
      )
    })

    const cleanup = Effect.fn("Snapshot.cleanup")(function* () {
      if (!(yield* enabled())) return
      if (!(yield* fs.existsSafe(path.join(gitDirectory, "HEAD")))) return
      yield* Effect.gen(function* () {
        // The v1 snapshot service gcs the same repositories, so only one claimant per hour runs it.
        const claimed = yield* Effect.gen(function* () {
          yield* record
          return yield* SnapshotRepo.claimGc(fs, gitDirectory)
        }).pipe(locked)
        if (!claimed) return
        // gc tolerates concurrent writers and the claim keeps other gcs out, so it runs without the lock
        // instead of blocking every capture on this repository for its whole run.
        yield* git.repo.gc(new Git.Repository({ worktree, gitDirectory, commonDirectory: gitDirectory }), {
          prune: SnapshotRepo.PRUNE,
        })
      }).pipe(Effect.catch((cause) => Effect.logWarning("snapshot cleanup failed", { cause })))
    })

    yield* cleanup().pipe(
      Effect.repeat(Schedule.spaced(Duration.hours(1))),
      Effect.delay(Duration.minutes(1)),
      Effect.forkScoped,
    )

    return Service.of({ capture, files, diff, preview, restore, checkout, cleanup })
  }),
)

export const locationLayer = layer.pipe(Layer.provideMerge(Config.locationLayer))

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Config.node, EffectFlock.node, FSUtil.node, Git.node, Global.node, Location.node],
})

export const noopLayer = Layer.succeed(
  Service,
  Service.of({
    capture: () => Effect.succeed(undefined),
    files: () => Effect.succeed([]),
    diff: () => Effect.succeed([]),
    preview: () => Effect.succeed([]),
    restore: () => Effect.void,
    checkout: () => Effect.void,
    cleanup: () => Effect.void,
  }),
)

function failure(operation: Error["operation"], cause: unknown) {
  if (cause instanceof Error && cause.operation === operation) return cause
  return new Error({
    operation,
    message: cause instanceof globalThis.Error ? cause.message : String(cause),
    cause,
  })
}

/** Legacy persisted session diff shape. */
export type LegacyFileDiff = {
  file?: string
  patch?: string
  additions: number
  deletions: number
  status?: "added" | "deleted" | "modified"
}
