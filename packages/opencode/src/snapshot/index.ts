import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Cause, Clock, Duration, Effect, Exit, Layer, Option, Schedule, Schema, Semaphore, Context } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { formatPatch, structuredPatch } from "diff"
import path from "path"
import { AppProcess } from "@opencode-ai/core/process"
import { InstanceState } from "@/effect/instance-state"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Hash } from "@opencode-ai/core/util/hash"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { SnapshotRepo } from "@opencode-ai/core/snapshot-repo"
import { Config } from "@/config/config"
import { Global } from "@opencode-ai/core/global"
import { Info } from "@opencode-ai/schema/file-diff"

export const Patch = Schema.Struct({
  hash: Schema.String,
  files: Schema.mutable(Schema.Array(Schema.String)),
})
export type Patch = typeof Patch.Type

export const FileDiff = Info
export type FileDiff = typeof FileDiff.Type

const prune = SnapshotRepo.PRUNE
// How long a recorded worktree must stay missing before the sweep deletes its repository.
const MISSING_GRACE = Duration.days(14)
// Volumes mounted here vanish whenever the disk or share is detached.
const MOUNT_ROOTS = ["/Volumes/", "/media/", "/mnt/", "/run/media/"]
const limit = 2 * 1024 * 1024
const core = ["-c", "core.longpaths=true", "-c", "core.symlinks=true"]
const cfg = ["-c", "core.autocrlf=false", ...core]
const quote = [...cfg, "-c", "core.quotepath=false"]
interface GitResult {
  readonly code: ChildProcessSpawner.ExitCode
  readonly text: string
  readonly stderr: string
}

type State = Omit<Interface, "init">

export interface Interface {
  readonly init: () => Effect.Effect<void>
  readonly cleanup: () => Effect.Effect<void>
  readonly track: () => Effect.Effect<string | undefined>
  readonly patch: (hash: string) => Effect.Effect<Patch>
  readonly restore: (snapshot: string) => Effect.Effect<void>
  readonly revert: (patches: Patch[]) => Effect.Effect<void>
  readonly diff: (hash: string) => Effect.Effect<string>
  readonly diffFull: (from: string, to: string, context?: number) => Effect.Effect<FileDiff[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Snapshot") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const appProcess = yield* AppProcess.Service
    const config = yield* Config.Service
    const flock = yield* EffectFlock.Service
    const locks = new Map<string, Semaphore.Semaphore>()

    const lock = (key: string) => {
      const hit = locks.get(key)
      if (hit) return hit

      const next = Semaphore.makeUnsafe(1)
      locks.set(key, next)
      return next
    }

    // Deletes repositories whose recorded worktree has been gone for MISSING_GRACE. track, cleanup, restore and
    // revert (and v2 capture, preview, restore and checkout) hold the cross-process flock, so a repository is never
    // deleted under one of them. v1 patch and diff take only this process's semaphore. Every path takes the flock
    // before the semaphore and never waits on the flock while holding the semaphore, so the two cannot deadlock.
    const sweep = Effect.fnUntraced(function* () {
      const root = path.join(Global.Path.data, "snapshot")
      const projects = yield* fs
        .readDirectory(root)
        .pipe(Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed<string[]>([])))
      const gitdirs = yield* Effect.forEach(projects, (project) =>
        fs.readDirectory(path.join(root, project)).pipe(
          Effect.map((entries) => entries.map((entry) => path.join(root, project, entry))),
          Effect.orElseSucceed((): string[] => []),
        ),
      )
      const results = yield* Effect.forEach(gitdirs.flat(), (gitdir) =>
        Effect.gen(function* () {
          const worktree = (yield* fs
            .readFileString(path.join(gitdir, SnapshotRepo.WORKTREE_FILE))
            .pipe(Effect.orElseSucceed(() => ""))).trim()
          // Repositories without a record are kept until cleanup or v2 capture maps them, and an empty record
          // is a write in progress. Age alone is never a reason to delete: undo of an old session needs them.
          if (!worktree) return
          // Removable and network volumes come and go; their repositories are kept and gc bounds them.
          if (mounted(worktree)) return
          yield* settle(gitdir, worktree).pipe(
            lock(gitdir).withPermits(1),
            flock.withLock(SnapshotRepo.lockKey(gitdir), SnapshotRepo.lockOptions),
          )
        }).pipe(Effect.exit),
      )
      const failed = results.find(Exit.isFailure)
      if (failed) return yield* failed
    })

    // A single missing observation never deletes: an unplugged disk, a disconnected share, a container sharing
    // the data directory, or a project being moved or re-cloned all look missing for a while. The first miss is
    // recorded, a later sighting clears it, and only a worktree still missing MISSING_GRACE later is deleted.
    const settle = Effect.fnUntraced(function* (gitdir: string, worktree: string) {
      // Another process's sweep may have deleted it since the listing.
      if (yield* missing(gitdir)) return
      const marker = path.join(gitdir, SnapshotRepo.MISSING_FILE)
      if (!(yield* missing(worktree))) return yield* fs.remove(marker, { force: true })
      const since = Number((yield* fs.readFileStringSafe(marker)) ?? NaN)
      const now = yield* Clock.currentTimeMillis
      if (!Number.isFinite(since) || since > now)
        return yield* fs
          .writeFileString(marker, String(now))
          .pipe(Effect.catchReason("PlatformError", "NotFound", () => Effect.void))
      if (now - since < Duration.toMillis(MISSING_GRACE)) return
      yield* fs.remove(gitdir, { recursive: true })
      yield* Effect.logInfo("removed snapshot repository", { gitdir, worktree })
    })

    // Only a definite NotFound counts as missing; any other stat failure keeps the snapshots.
    const missing = (file: string) =>
      fs.stat(file).pipe(
        Effect.as(false),
        Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(true)),
        Effect.orElseSucceed(() => false),
      )

    // Every loaded instance runs cleanup, but the sweep covers all repositories, so it runs once per process.
    // A failed sweep is not cached, so the next cleanup tick retries it.
    const sweepOnce = yield* Effect.cachedWithTTL(sweep(), (exit) =>
      Exit.isSuccess(exit) ? Duration.infinity : Duration.zero,
    )

    const state = yield* InstanceState.make<State>(
      Effect.fn("Snapshot.state")(function* (ctx) {
        const state = {
          directory: ctx.directory,
          worktree: ctx.worktree,
          gitdir: path.join(Global.Path.data, "snapshot", ctx.project.id, Hash.fast(ctx.worktree)),
          vcs: ctx.project.vcs,
          exclude: undefined as string | undefined,
          // The tree last written from the index, with the index file's stamp at that moment.
          tree: undefined as { hash: string; index: string } | undefined,
        }

        const args = (cmd: string[]) => ["--git-dir", state.gitdir, "--work-tree", state.worktree, ...cmd]

        const encodeNulTerminatedPaths = (files: string[]) => files.join("\0") + "\0"
        const encodeTopLevelLiteralPathspecs = (files: string[]) =>
          encodeNulTerminatedPaths(files.map((file) => `:(top,literal)${file}`))

        const git = Effect.fnUntraced(
          function* (cmd: string[], opts?: { cwd?: string; env?: Record<string, string>; stdin?: string }) {
            const result = yield* appProcess.run(
              ChildProcess.make("git", cmd, { cwd: opts?.cwd, env: opts?.env, extendEnv: true }),
              { stdin: opts?.stdin },
            )
            return {
              code: ChildProcessSpawner.ExitCode(result.exitCode),
              text: result.stdout.toString("utf8"),
              stderr: result.stderr.toString("utf8"),
            } satisfies GitResult
          },
          Effect.catch((err) =>
            Effect.succeed({
              code: ChildProcessSpawner.ExitCode(1),
              text: "",
              stderr: err instanceof Error ? err.message : String(err),
            }),
          ),
        )

        const ignore = Effect.fnUntraced(function* (files: string[]) {
          if (!files.length) return new Set<string>()
          // check-ignore treats a leading colon as pathspec magic but accepts and echoes a protective ./ prefix.
          const checkIgnorePaths = files.map((item) => (item.startsWith(":") ? `./${item}` : item))
          const check = yield* git(
            [
              ...quote,
              "--git-dir",
              path.join(state.worktree, ".git"),
              "--work-tree",
              state.worktree,
              "check-ignore",
              "--no-index",
              "--stdin",
              "-z",
            ],
            {
              cwd: state.worktree,
              stdin: encodeNulTerminatedPaths(checkIgnorePaths),
            },
          )
          if (check.code !== 0 && check.code !== 1) return new Set<string>()
          return new Set(
            check.text
              .split("\0")
              .filter(Boolean)
              .map((item) => (item.startsWith("./:") ? item.slice(2) : item)),
          )
        })

        const drop = Effect.fnUntraced(function* (files: string[]) {
          if (!files.length) return
          yield* git(
            [
              ...cfg,
              ...args(["rm", "--cached", "-f", "--ignore-unmatch", "--pathspec-from-file=-", "--pathspec-file-nul"]),
            ],
            {
              cwd: state.worktree,
              stdin: encodeTopLevelLiteralPathspecs(files),
            },
          )
        })

        const stage = Effect.fnUntraced(function* (files: string[]) {
          if (!files.length) return
          const result = yield* git(
            [...cfg, ...args(["add", "--all", "--sparse", "--pathspec-from-file=-", "--pathspec-file-nul"])],
            {
              cwd: state.worktree,
              stdin: encodeTopLevelLiteralPathspecs(files),
            },
          )
          if (result.code === 0) return
          yield* Effect.logWarning("failed to add snapshot files", {
            exitCode: result.code,
            stderr: result.stderr,
          })
        })

        const exists = (file: string) => fs.exists(file).pipe(Effect.orDie)
        // Git replaces the index by renaming a lock file, so any write changes its inode.
        const indexStamp = () =>
          fs.stat(path.join(state.gitdir, "index")).pipe(
            Effect.map((info) =>
              [Option.getOrUndefined(info.ino), Option.getOrUndefined(info.mtime)?.getTime(), String(info.size)].join(
                ":",
              ),
            ),
            Effect.catch(() => Effect.succeed(undefined)),
          )
        const read = (file: string) => fs.readFileString(file).pipe(Effect.catch(() => Effect.succeed("")))
        const remove = (file: string) => fs.remove(file).pipe(Effect.catch(() => Effect.void))
        const locked = <A, E, R>(fx: Effect.Effect<A, E, R>) => lock(state.gitdir).withPermits(1)(fx)
        // Writers of the shared index also hold the cross-process flock, taken before the semaphore so a flock
        // wait never holds up this process's patch and diff.
        const exclusive = <A, E, R>(fx: Effect.Effect<A, E, R>) =>
          locked(fx).pipe(flock.withLock(SnapshotRepo.lockKey(state.gitdir), SnapshotRepo.lockOptions))

        const enabled = Effect.fnUntraced(function* () {
          if (state.vcs !== "git") return false
          return (yield* config.get()).snapshot !== false
        })

        const excludes = Effect.fnUntraced(function* () {
          // Every track and patch syncs excludes, and the path only moves with the git directory.
          state.exclude ??=
            (yield* git(["rev-parse", "--path-format=absolute", "--git-path", "info/exclude"], {
              cwd: state.worktree,
            })).text.trim() || undefined
          const file = state.exclude
          if (!file) return
          if (!(yield* exists(file))) return
          return file
        })

        const sync = Effect.fnUntraced(function* (list: string[] = []) {
          const file = yield* excludes()
          const target = path.join(state.gitdir, "info", "exclude")
          const text = [
            file ? (yield* read(file)).trimEnd() : "",
            ...list.map((item) => `/${item.replaceAll("\\", "/")}`),
          ]
            .filter(Boolean)
            .join("\n")
          yield* fs.ensureDir(path.join(state.gitdir, "info")).pipe(Effect.orDie)
          yield* fs.writeFileString(target, text ? `${text}\n` : "").pipe(Effect.orDie)
        })

        // Reuse the hashes for the git storage between the original repo and snapshot
        // on huge repos like chromium checkout the git add --all rebuilding the
        // hashes can take minutes. By doing this we eliminating this at all
        const seed = Effect.fnUntraced(function* () {
          if (state.vcs !== "git") return

          const commonDir = yield* git(["rev-parse", "--path-format=absolute", "--git-common-dir"], {
            cwd: state.worktree,
          })

          if (commonDir.code !== 0) return
          const source = commonDir.text.trim()
          if (!source || !(yield* exists(source))) return

          // Share the source object database (and the source's own alternates,
          // skipping any that no longer exist) so seeded blobs resolve.
          const sourceObjects = path.join(source, "objects")
          const chained = (yield* read(path.join(sourceObjects, "info", "alternates")))
            .split("\n")
            .map((line) => line.trim())
            .filter(Boolean)
          const alternates: string[] = []
          for (const candidate of [sourceObjects, ...chained]) {
            if (yield* exists(candidate)) alternates.push(candidate)
          }
          if (!alternates.length) return

          yield* fs.ensureDir(path.join(state.gitdir, "objects", "info")).pipe(Effect.orDie)
          yield* fs
            .writeFileString(path.join(state.gitdir, "objects", "info", "alternates"), alternates.join("\n") + "\n")
            .pipe(Effect.orDie)

          // Seed the index from the source repo so already-hashed entries are reused.
          // Best-effort: a missing/incompatible index just falls back to a full add.
          const sourceIndex = path.join(source, "index")
          if (yield* exists(sourceIndex)) {
            yield* fs.copyFile(sourceIndex, path.join(state.gitdir, "index")).pipe(Effect.catch(() => Effect.void))
          }
        })

        const add = Effect.fnUntraced(function* () {
          yield* sync()
          // One listing for both changed tracked files (tag "C", deletions included) and untracked
          // files (tag "?").
          const listed = yield* git(
            [
              ...quote,
              ...args([
                "ls-files",
                "-t",
                "--full-name",
                "--modified",
                "--others",
                "--exclude-standard",
                "-z",
                "--",
                ".",
              ]),
            ],
            { cwd: state.directory },
          )
          if (listed.code !== 0) {
            yield* Effect.logWarning("failed to list snapshot files", {
              exitCode: listed.code,
              stderr: listed.stderr,
            })
            return
          }

          const entries = listed.text
            .split("\0")
            .filter(Boolean)
            .map((item) => ({ tag: item.slice(0, 1), file: item.slice(2) }))
          const tracked = entries.filter((item) => item.tag !== "?").map((item) => item.file)
          const untracked = entries.filter((item) => item.tag === "?").map((item) => item.file)
          const all = Array.from(new Set([...tracked, ...untracked]))
          if (!all.length) return

          // Resolve source-repo ignore rules against the exact candidate set.
          // --no-index keeps this pattern-based even when a path is already tracked.
          const ignored = yield* ignore(all)

          // Remove newly-ignored files from snapshot index to prevent re-adding
          if (ignored.size > 0) {
            const ignoredFiles = Array.from(ignored)
            yield* Effect.logInfo("removing gitignored files from snapshot", { count: ignoredFiles.length })
            yield* drop(ignoredFiles)
          }

          const allow = all.filter((item) => !ignored.has(item))
          if (!allow.length) return

          const large = new Set(
            (yield* Effect.all(
              allow.map((item) =>
                fs
                  .stat(path.join(state.worktree, item))
                  .pipe(Effect.catch(() => Effect.void))
                  .pipe(
                    Effect.map((stat) => {
                      if (!stat || stat.type !== "File") return
                      const size = typeof stat.size === "bigint" ? Number(stat.size) : stat.size
                      return size > limit ? item : undefined
                    }),
                  ),
              ),
              { concurrency: 8 },
            )).filter((item): item is string => Boolean(item)),
          )
          const block = new Set(untracked.filter((item) => large.has(item)))
          // The sync above already wrote the excludes without blocked files.
          if (block.size) yield* sync(Array.from(block))
          // Stage only the allowed candidate paths so snapshot updates stay scoped.
          yield* stage(allow.filter((item) => !block.has(item)))
        })

        const cleanup = Effect.fnUntraced(function* () {
          yield* Effect.gen(function* () {
            // The v2 snapshot service gcs the same repositories, so only one claimant per hour runs it.
            const claimed = yield* exclusive(
              Effect.gen(function* () {
                if (!(yield* enabled())) return false
                if (!(yield* exists(state.gitdir))) return false
                yield* SnapshotRepo.record(fs, state.gitdir, state.worktree)
                return yield* SnapshotRepo.claimGc(fs, state.gitdir)
              }),
            )
            if (!claimed) return
            // gc tolerates concurrent writers and the claim keeps other gcs out, so it runs without either lock
            // instead of blocking every track, patch and diff on this repository for its whole run.
            const result = yield* git(args(["gc", `--prune=${prune}`]), { cwd: state.directory })
            if (result.code !== 0) {
              yield* Effect.logWarning("cleanup failed", {
                exitCode: result.code,
                stderr: result.stderr,
              })
              return
            }
            yield* Effect.logInfo("cleanup", { prune })
          }).pipe(Effect.catch((cause) => Effect.logWarning("cleanup failed", { cause })))
          yield* sweepOnce.pipe(
            Effect.catchCause((cause) => Effect.logWarning("snapshot sweep failed", { cause: Cause.pretty(cause) })),
          )
        })

        const track = Effect.fnUntraced(function* () {
          return yield* exclusive(
            Effect.gen(function* () {
              if (!(yield* enabled())) return
              const existed = yield* exists(state.gitdir)
              yield* fs.ensureDir(state.gitdir).pipe(Effect.orDie)
              if (!existed) {
                yield* git(["init"], {
                  env: { GIT_DIR: state.gitdir, GIT_WORK_TREE: state.worktree },
                })
                // One append instead of a `git config` process per key. Git uses the last value of a
                // key, so these override what init detected (it writes symlinks = false on Windows).
                // manyFiles, the v4 index and the untracked cache keep the first add bounded on very
                // large worktrees.
                const config = path.join(state.gitdir, "config")
                yield* fs
                  .writeFileString(
                    config,
                    (yield* read(config)) +
                      [
                        "[core]",
                        "\tautocrlf = false",
                        "\tlongpaths = true",
                        "\tsymlinks = true",
                        "\tfsmonitor = false",
                        "\tuntrackedCache = true",
                        "[feature]",
                        "\tmanyFiles = true",
                        "[index]",
                        "\tversion = 4",
                        "\tthreads = true",
                        "",
                      ].join("\n"),
                  )
                  .pipe(Effect.orDie)
                yield* seed()
                yield* Effect.logInfo("initialized")
              }
              yield* add()
              // Nothing was staged since the last write-tree, so the index still holds that tree.
              const before = yield* indexStamp()
              if (state.tree && before && state.tree.index === before) return state.tree.hash
              const result = yield* git(args(["write-tree"]), { cwd: state.directory })
              const hash = result.text.trim()
              const index = yield* indexStamp()
              state.tree = result.code === 0 && hash && index ? { hash, index } : undefined
              yield* Effect.logDebug("tracking", { hash, cwd: state.directory, git: state.gitdir })
              return hash
            }),
          ).pipe(
            Effect.catchTag("LockTimeoutError", (cause) =>
              Effect.logWarning("failed to lock snapshot repository, skipping snapshot", { cause }).pipe(
                Effect.as(undefined),
              ),
            ),
            Effect.catch((cause) =>
              Effect.logWarning("failed to track snapshot", { tag: cause._tag, cause }).pipe(Effect.as(undefined)),
            ),
          )
        })

        const patch = Effect.fnUntraced(function* (hash: string) {
          return yield* locked(
            Effect.gen(function* () {
              yield* add()
              // The index is still the one this tree was written from, so nothing changed since.
              const index = yield* indexStamp()
              if (state.tree?.hash === hash && index && state.tree.index === index) return { hash, files: [] }
              const result = yield* git(
                [...quote, ...args(["diff", "--cached", "--no-ext-diff", "--name-only", hash, "--", "."])],
                {
                  cwd: state.directory,
                },
              )
              if (result.code !== 0) {
                yield* Effect.logWarning("failed to get diff", { hash, exitCode: result.code })
                return { hash, files: [] }
              }
              const files = result.text
                .trim()
                .split("\n")
                .map((x) => x.trim())
                .filter(Boolean)

              // Hide ignored-file removals from the user-facing patch output.
              const ignored = yield* ignore(files)

              return {
                hash,
                files: files
                  .filter((item) => !ignored.has(item))
                  .map((x) => path.join(state.worktree, x).replaceAll("\\", "/")),
              }
            }),
          )
        })

        const restore = Effect.fnUntraced(function* (snapshot: string) {
          return yield* exclusive(
            Effect.gen(function* () {
              yield* Effect.logInfo("restore", { commit: snapshot })
              const result = yield* git([...core, ...args(["read-tree", snapshot])], { cwd: state.worktree })
              if (result.code === 0) {
                const checkout = yield* git([...core, ...args(["checkout-index", "-a", "-f"])], {
                  cwd: state.worktree,
                })
                if (checkout.code === 0) return
                yield* Effect.logError("failed to restore snapshot", {
                  snapshot,
                  exitCode: checkout.code,
                  stderr: checkout.stderr,
                })
                return
              }
              yield* Effect.logError("failed to restore snapshot", {
                snapshot,
                exitCode: result.code,
                stderr: result.stderr,
              })
            }),
          ).pipe(
            Effect.catch((cause) =>
              Effect.logError("failed to restore snapshot", { snapshot, tag: cause._tag, cause }),
            ),
          )
        })

        const revert = Effect.fnUntraced(function* (patches: Patch[]) {
          return yield* exclusive(
            Effect.gen(function* () {
              const ops: { hash: string; file: string; rel: string }[] = []
              const seen = new Set<string>()
              for (const item of patches) {
                for (const file of item.files) {
                  if (seen.has(file)) continue
                  seen.add(file)
                  ops.push({
                    hash: item.hash,
                    file,
                    rel: path.relative(state.worktree, file).replaceAll("\\", "/"),
                  })
                }
              }

              const single = Effect.fnUntraced(function* (op: (typeof ops)[number]) {
                yield* Effect.logInfo("reverting", { file: op.file, hash: op.hash })
                const result = yield* git([...core, ...args(["checkout", op.hash, "--", op.file])], {
                  cwd: state.worktree,
                })
                if (result.code === 0) return
                const tree = yield* git([...core, ...args(["ls-tree", op.hash, "--", op.rel])], {
                  cwd: state.worktree,
                })
                if (tree.code === 0 && tree.text.trim()) {
                  yield* Effect.logInfo("file existed in snapshot but checkout failed, keeping", {
                    file: op.file,
                    hash: op.hash,
                  })
                  return
                }
                yield* Effect.logInfo("file did not exist in snapshot, deleting", { file: op.file, hash: op.hash })
                yield* remove(op.file)
              })

              const clash = (a: string, b: string) => a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`)

              for (let i = 0; i < ops.length; ) {
                const first = ops[i]!
                const run = [first]
                let j = i + 1
                // Only batch adjacent files when their paths cannot affect each other.
                while (j < ops.length && run.length < 100) {
                  const next = ops[j]!
                  if (next.hash !== first.hash) break
                  if (run.some((item) => clash(item.rel, next.rel))) break
                  run.push(next)
                  j += 1
                }

                if (run.length === 1) {
                  yield* single(first)
                  i = j
                  continue
                }

                const tree = yield* git(
                  [...core, ...args(["ls-tree", "--name-only", first.hash, "--", ...run.map((item) => item.rel)])],
                  {
                    cwd: state.worktree,
                  },
                )

                if (tree.code !== 0) {
                  yield* Effect.logInfo("batched ls-tree failed, falling back to single-file revert", {
                    hash: first.hash,
                    files: run.length,
                  })
                  for (const op of run) {
                    yield* single(op)
                  }
                  i = j
                  continue
                }

                const have = new Set(
                  tree.text
                    .trim()
                    .split("\n")
                    .map((item) => item.trim())
                    .filter(Boolean),
                )
                const list = run.filter((item) => have.has(item.rel))
                if (list.length) {
                  yield* Effect.logInfo("reverting", { hash: first.hash, files: list.length })
                  const result = yield* git(
                    [...core, ...args(["checkout", first.hash, "--", ...list.map((item) => item.file)])],
                    {
                      cwd: state.worktree,
                    },
                  )
                  if (result.code !== 0) {
                    yield* Effect.logInfo("batched checkout failed, falling back to single-file revert", {
                      hash: first.hash,
                      files: list.length,
                    })
                    for (const op of run) {
                      yield* single(op)
                    }
                    i = j
                    continue
                  }
                }

                for (const op of run) {
                  if (have.has(op.rel)) continue
                  yield* Effect.logInfo("file did not exist in snapshot, deleting", { file: op.file, hash: op.hash })
                  yield* remove(op.file)
                }

                i = j
              }
            }),
          ).pipe(Effect.catch((cause) => Effect.logError("failed to revert snapshot", { tag: cause._tag, cause })))
        })

        const diff = Effect.fnUntraced(function* (hash: string) {
          return yield* locked(
            Effect.gen(function* () {
              yield* add()
              const result = yield* git([...quote, ...args(["diff", "--cached", "--no-ext-diff", hash, "--", "."])], {
                cwd: state.worktree,
              })
              if (result.code !== 0) {
                yield* Effect.logWarning("failed to get diff", {
                  hash,
                  exitCode: result.code,
                  stderr: result.stderr,
                })
                return ""
              }
              return result.text.trim()
            }),
          )
        })

        const diffFull = Effect.fnUntraced(function* (from: string, to: string, context = Number.MAX_SAFE_INTEGER) {
          // Steps that change no files track the same tree before and after.
          if (from === to) return [] as FileDiff[]
          return yield* locked(
            Effect.gen(function* () {
              type Row = {
                file: string
                status: "added" | "deleted" | "modified"
                binary: boolean
                additions: number
                deletions: number
              }

              type Ref = {
                file: string
                side: "before" | "after"
                ref: string
              }

              const show = Effect.fnUntraced(function* (row: Row) {
                if (row.binary) return ["", ""]
                if (row.status === "added") {
                  return [
                    "",
                    yield* git([...cfg, ...args(["show", `${to}:${row.file}`])]).pipe(Effect.map((item) => item.text)),
                  ]
                }
                if (row.status === "deleted") {
                  return [
                    yield* git([...cfg, ...args(["show", `${from}:${row.file}`])]).pipe(
                      Effect.map((item) => item.text),
                    ),
                    "",
                  ]
                }
                return yield* Effect.all(
                  [
                    git([...cfg, ...args(["show", `${from}:${row.file}`])]).pipe(Effect.map((item) => item.text)),
                    git([...cfg, ...args(["show", `${to}:${row.file}`])]).pipe(Effect.map((item) => item.text)),
                  ],
                  { concurrency: 2 },
                )
              })

              const load = Effect.fnUntraced(
                function* (rows: Row[]) {
                  const refs = rows.flatMap((row) => {
                    if (row.binary) return []
                    if (row.status === "added")
                      return [{ file: row.file, side: "after", ref: `${to}:${row.file}` } satisfies Ref]
                    if (row.status === "deleted") {
                      return [{ file: row.file, side: "before", ref: `${from}:${row.file}` } satisfies Ref]
                    }
                    return [
                      { file: row.file, side: "before", ref: `${from}:${row.file}` } satisfies Ref,
                      { file: row.file, side: "after", ref: `${to}:${row.file}` } satisfies Ref,
                    ]
                  })
                  if (!refs.length) return new Map<string, { before: string; after: string }>()

                  const batch = yield* appProcess.run(
                    ChildProcess.make("git", [...cfg, ...args(["cat-file", "--batch"])], {
                      cwd: state.directory,
                      extendEnv: true,
                    }),
                    { stdin: refs.map((item) => item.ref).join("\n") + "\n" },
                  )
                  if (batch.exitCode !== 0) {
                    yield* Effect.logInfo(
                      "git cat-file --batch failed during snapshot diff, falling back to per-file git show",
                      {
                        stderr: batch.stderr.toString("utf8"),
                        refs: refs.length,
                      },
                    )
                    return
                  }
                  const out = batch.stdout

                  const fail = (msg: string, extra?: Record<string, string>) => {
                    return undefined
                  }

                  const map = new Map<string, { before: string; after: string }>()
                  const dec = new TextDecoder()
                  let i = 0
                  for (const ref of refs) {
                    let end = i
                    while (end < out.length && out[end] !== 10) end += 1
                    if (end >= out.length) {
                      return fail(
                        "git cat-file --batch returned a truncated header during snapshot diff, falling back to per-file git show",
                      )
                    }

                    const head = dec.decode(out.slice(i, end))
                    i = end + 1
                    const hit = map.get(ref.file) ?? { before: "", after: "" }
                    if (head.endsWith(" missing")) {
                      map.set(ref.file, hit)
                      continue
                    }

                    const match = head.match(/^[0-9a-f]+ blob (\d+)$/)
                    if (!match) {
                      return fail(
                        "git cat-file --batch returned an unexpected header during snapshot diff, falling back to per-file git show",
                        { head },
                      )
                    }

                    const size = Number(match[1])
                    if (!Number.isInteger(size) || size < 0 || i + size >= out.length || out[i + size] !== 10) {
                      return fail(
                        "git cat-file --batch returned truncated content during snapshot diff, falling back to per-file git show",
                        { head },
                      )
                    }

                    const text = dec.decode(out.slice(i, i + size))
                    if (ref.side === "before") hit.before = text
                    if (ref.side === "after") hit.after = text
                    map.set(ref.file, hit)
                    i += size + 1
                  }

                  if (i !== out.length) {
                    return fail(
                      "git cat-file --batch returned trailing data during snapshot diff, falling back to per-file git show",
                    )
                  }

                  return map
                },
                Effect.scoped,
                Effect.catch(() =>
                  Effect.succeed<Map<string, { before: string; after: string }> | undefined>(undefined),
                ),
              )

              const result: FileDiff[] = []
              const status = new Map<string, "added" | "deleted" | "modified">()

              // One diff for both line counts and added/deleted files ("create mode" / "delete mode").
              const numstat = yield* git(
                [
                  ...quote,
                  ...args(["diff", "--no-ext-diff", "--no-renames", "--numstat", "--summary", from, to, "--", "."]),
                ],
                {
                  cwd: state.directory,
                },
              )
              const lines = numstat.text.trim().split("\n")
              for (const line of lines) {
                const match = line.match(/^ (create|delete) mode \d+ (.+)$/)
                if (match) status.set(match[2]!, match[1] === "create" ? "added" : "deleted")
              }

              const rows = lines
                .filter((line) => line.includes("\t"))
                .flatMap((line) => {
                  const [adds, dels, file] = line.split("\t")
                  if (!file) return []
                  const binary = adds === "-" && dels === "-"
                  const additions = binary ? 0 : parseInt(adds)
                  const deletions = binary ? 0 : parseInt(dels)
                  return [
                    {
                      // Report real paths so stored summaries, on-demand diffs and git refs agree.
                      file: unquoteGitPath(file),
                      status: status.get(file) ?? "modified",
                      binary,
                      additions: Number.isFinite(additions) ? additions : 0,
                      deletions: Number.isFinite(deletions) ? deletions : 0,
                    } satisfies Row,
                  ]
                })

              // Hide ignored-file removals from the user-facing diff output.
              const ignored = yield* ignore(rows.map((r) => r.file))
              if (ignored.size > 0) {
                const filtered = rows.filter((r) => !ignored.has(r.file))
                rows.length = 0
                rows.push(...filtered)
              }

              const step = 100
              // Viewers treat the empty-header form ("--- file\t") as whole-file content. Limited-context patches
              // omit the headers so a hunk at the top of a long file is not mistaken for the complete file.
              const whole = context === Number.MAX_SAFE_INTEGER ? "" : undefined
              const patch = (file: string, before: string, after: string) =>
                formatPatch(structuredPatch(file, file, before, after, whole, whole, { context }))

              for (let i = 0; i < rows.length; i += step) {
                const run = rows.slice(i, i + step)
                const text = yield* load(run)

                for (const row of run) {
                  const hit = text?.get(row.file) ?? { before: "", after: "" }
                  const [before, after] = row.binary ? ["", ""] : text ? [hit.before, hit.after] : yield* show(row)
                  result.push({
                    file: row.file,
                    patch: row.binary ? "" : patch(row.file, before, after),
                    additions: row.additions,
                    deletions: row.deletions,
                    status: row.status,
                  })
                }
              }

              return result
            }),
          )
        })

        yield* cleanup().pipe(
          Effect.catchCause((cause) => Effect.logError("cleanup loop failed", { cause: Cause.pretty(cause) })),
          Effect.repeat(Schedule.spaced(Duration.hours(1))),
          Effect.delay(Duration.minutes(1)),
          Effect.forkScoped,
        )

        return { cleanup, track, patch, restore, revert, diff, diffFull }
      }),
    )

    return Service.of({
      init: Effect.fn("Snapshot.init")(function* () {
        yield* InstanceState.get(state)
      }),
      cleanup: Effect.fn("Snapshot.cleanup")(function* () {
        return yield* InstanceState.useEffect(state, (s) => s.cleanup())
      }),
      track: Effect.fn("Snapshot.track")(function* () {
        return yield* InstanceState.useEffect(state, (s) => s.track())
      }),
      patch: Effect.fn("Snapshot.patch")(function* (hash: string) {
        return yield* InstanceState.useEffect(state, (s) => s.patch(hash))
      }),
      restore: Effect.fn("Snapshot.restore")(function* (snapshot: string) {
        return yield* InstanceState.useEffect(state, (s) => s.restore(snapshot))
      }),
      revert: Effect.fn("Snapshot.revert")(function* (patches: Patch[]) {
        return yield* InstanceState.useEffect(state, (s) => s.revert(patches))
      }),
      diff: Effect.fn("Snapshot.diff")(function* (hash: string) {
        return yield* InstanceState.useEffect(state, (s) => s.diff(hash))
      }),
      diffFull: Effect.fn("Snapshot.diffFull")(function* (from: string, to: string, context?: number) {
        return yield* InstanceState.useEffect(state, (s) => s.diffFull(from, to, context))
      }),
    })
  }),
)

// Git quotes paths with control characters, quotes or backslashes even with core.quotepath=false.
export function unquoteGitPath(input: string) {
  if (!input.startsWith('"')) return input
  if (!input.endsWith('"')) return input
  const body = input.slice(1, -1)
  const bytes: number[] = []

  for (let i = 0; i < body.length; i++) {
    const char = body[i]!
    if (char !== "\\") {
      bytes.push(char.charCodeAt(0))
      continue
    }

    const next = body[i + 1]
    if (!next) {
      bytes.push("\\".charCodeAt(0))
      continue
    }

    if (next >= "0" && next <= "7") {
      const chunk = body.slice(i + 1, i + 4)
      const match = chunk.match(/^[0-7]{1,3}/)
      if (!match) {
        bytes.push(next.charCodeAt(0))
        i++
        continue
      }
      bytes.push(parseInt(match[0], 8))
      i += match[0].length
      continue
    }

    const escaped =
      next === "n"
        ? "\n"
        : next === "r"
          ? "\r"
          : next === "t"
            ? "\t"
            : next === "b"
              ? "\b"
              : next === "f"
                ? "\f"
                : next === "v"
                  ? "\v"
                  : next === "\\" || next === '"'
                    ? next
                    : undefined

    bytes.push((escaped ?? next).charCodeAt(0))
    i++
  }

  return Buffer.from(bytes).toString()
}

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [FSUtil.node, AppProcess.node, Config.node, EffectFlock.node],
})

function mounted(worktree: string) {
  if (MOUNT_ROOTS.some((root) => worktree.startsWith(root))) return true
  // Windows network shares, and drive letters other than the system drive (removable or mapped drives).
  if (worktree.startsWith("\\\\")) return true
  const drive = /^([a-zA-Z]):/.exec(worktree)?.[1]?.toUpperCase()
  return drive !== undefined && drive !== (process.env.SystemDrive ?? "C:").charAt(0).toUpperCase()
}

export * as Snapshot from "."
