import type { SnapshotFileDiff } from "@opencode-ai/sdk/v2"
import { queryOptions, skipToken } from "@tanstack/solid-query"
import type { DirectorySDK } from "@/context/sdk"
import type { SummaryDiff } from "./timeline-row"

type DiffStat = { file?: string; additions: number; deletions: number }

export function uniqueSummaryDiffs(diffs: SnapshotFileDiff[] | undefined) {
  const files = new Set<string>()
  return (diffs ?? [])
    .reduceRight<SummaryDiff[]>((result, diff) => {
      if (!isSummaryDiff(diff)) return result
      const file = diff.file
      if (files.has(file)) return result
      files.add(file)
      result.push(diff)
      return result
    }, [])
    .reverse()
}

// Stored turn summaries keep standard-context patches; the server rebuilds full-file context on request.
export function turnDiffQuery(input: {
  sdk: DirectorySDK
  sessionID: string | undefined
  messageID: string | undefined
  diffs: readonly DiffStat[]
  enabled: boolean
}) {
  const sessionID = input.sessionID
  const messageID = input.messageID
  return queryOptions({
    queryKey: [
      "session-turn-diff",
      input.sdk.scope,
      input.sdk.directory,
      sessionID,
      messageID,
      input.diffs.map((diff) => `${diff.file}:${diff.additions}:${diff.deletions}`).join("\n"),
    ] as const,
    staleTime: Number.POSITIVE_INFINITY,
    queryFn:
      input.enabled && sessionID && messageID && input.diffs.length > 0
        ? () =>
            input.sdk.client.session.diff({ sessionID, messageID }).then((result) => uniqueSummaryDiffs(result.data))
        : skipToken,
  })
}

export function fullTurnDiff<T extends DiffStat>(diff: T, full: readonly SummaryDiff[] | undefined) {
  return (
    full?.find(
      (item) => item.file === diff.file && item.additions === diff.additions && item.deletions === diff.deletions,
    ) ?? diff
  )
}

function isSummaryDiff(diff: SnapshotFileDiff): diff is SummaryDiff {
  return typeof diff.file === "string"
}
