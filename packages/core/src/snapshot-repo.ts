export * as SnapshotRepo from "./snapshot-repo"

import path from "path"
import { Clock, Duration, Effect } from "effect"
import type { FSUtil } from "./fs-util"

// Bookkeeping shared by the v1 (opencode) and v2 (core) snapshot services, which write into the same
// repositories under `<data>/snapshot/<project>/<worktree hash>`, possibly from several processes at once.

/** The canonical (realpath) worktree a repository belongs to, read by the v1 sweep. */
export const WORKTREE_FILE = "opencode-worktree"
/** When the sweep first found the recorded worktree missing. Cleared whenever the worktree is seen again. */
export const MISSING_FILE = "opencode-worktree-missing"
/** When a gc last started on the repository. */
const GC_FILE = "opencode-gc"
const GC_INTERVAL = Duration.hours(1)

/**
 * Snapshot trees are never referenced by a commit or ref, so every snapshot object is unreachable and
 * `git gc` prunes it once it is older than this: undo/revert only reaches back this far.
 */
export const PRUNE = "7.days"

/** Cross-process lock (EffectFlock) key held by every writer and by the sweep of one repository. */
export const lockKey = (gitdir: string) => `snapshot:${gitdir}`

/** Records the canonical worktree and clears any missing mark, since the caller just saw the worktree. */
export const record = (fs: FSUtil.Interface, gitdir: string, worktree: string) =>
  fs.realPath(worktree).pipe(
    Effect.flatMap((real) => fs.writeFileString(path.join(gitdir, WORKTREE_FILE), real)),
    Effect.andThen(fs.remove(path.join(gitdir, MISSING_FILE), { force: true })),
    Effect.ignore,
  )

/**
 * Claims the gc of a repository: true when no writer started one within the last hour, in which case the
 * claim is recorded. Both snapshot services run cleanup hourly on the same repositories, so this keeps them
 * (and every other process) from running concurrent gcs that contend on `gc.pid`. Callers hold `lockKey`.
 */
export const claimGc = Effect.fnUntraced(function* (fs: FSUtil.Interface, gitdir: string) {
  const file = path.join(gitdir, GC_FILE)
  const last = Number((yield* fs.readFileStringSafe(file).pipe(Effect.orElseSucceed(() => undefined))) ?? NaN)
  const now = yield* Clock.currentTimeMillis
  if (Number.isFinite(last) && last <= now && now - last < Duration.toMillis(GC_INTERVAL)) return false
  yield* fs.writeFileString(file, String(now)).pipe(Effect.ignore)
  return true
})
