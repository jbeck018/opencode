import { describe, expect, test } from "bun:test"
import type { SnapshotFileDiff } from "@opencode-ai/sdk/v2"
import { fullTurnDiff, uniqueSummaryDiffs } from "./summary-diffs"

const diff = (file: string, additions: number) =>
  ({
    file,
    additions,
    deletions: 0,
  }) satisfies SnapshotFileDiff

describe("uniqueSummaryDiffs", () => {
  test("drops entries without files and preserves unique input", () => {
    const alpha = diff("alpha.ts", 1)
    const beta = diff("beta.ts", 1)
    const invalid = { additions: 1, deletions: 0 } satisfies SnapshotFileDiff

    expect(uniqueSummaryDiffs(undefined)).toEqual([])
    expect(uniqueSummaryDiffs([])).toEqual([])
    expect(uniqueSummaryDiffs([invalid])).toEqual([])

    const result = uniqueSummaryDiffs([alpha, invalid, beta])
    expect(result).toEqual([alpha, beta])
    expect(result[0]).toBe(alpha)
    expect(result[1]).toBe(beta)
  })

  test("keeps the last diff per file in the legacy display order", () => {
    const oldAlpha = diff("alpha.ts", 1)
    const oldBeta = diff("beta.ts", 1)
    const newAlpha = diff("alpha.ts", 2)
    const charlie = diff("charlie.ts", 1)
    const newBeta = diff("beta.ts", 2)

    const result = uniqueSummaryDiffs([oldAlpha, oldBeta, newAlpha, charlie, newBeta])

    expect(result).toEqual([newAlpha, charlie, newBeta])
    expect(result[0]).toBe(newAlpha)
    expect(result[1]).toBe(charlie)
    expect(result[2]).toBe(newBeta)
  })
})

describe("fullTurnDiff", () => {
  test("prefers the on-demand full-context patch for the same change", () => {
    const stored = { file: "alpha.ts", patch: "@@ -4,1 +4,1 @@", additions: 1, deletions: 1 }
    const full = { file: "alpha.ts", patch: "@@ -1,9 +1,9 @@", additions: 1, deletions: 1 }

    expect(fullTurnDiff(stored, [full])).toBe(full)
  })

  test("keeps the stored patch until a matching full diff is available", () => {
    const stored = { file: "alpha.ts", patch: "@@ -4,1 +4,1 @@", additions: 1, deletions: 1 }

    expect(fullTurnDiff(stored, undefined)).toBe(stored)
    expect(fullTurnDiff(stored, [{ ...stored, patch: "newer", additions: 2 }])).toBe(stored)
    expect(fullTurnDiff(stored, [{ ...stored, file: "beta.ts" }])).toBe(stored)
  })

  test("matches git-quoted stored names against the server's unquoted paths", () => {
    const stored = { file: '"say \\"hi\\".txt"', patch: "@@ -4,1 +4,1 @@", additions: 1, deletions: 0 }
    const full = { file: 'say "hi".txt', patch: "@@ -1,9 +1,9 @@", additions: 1, deletions: 0 }

    expect(fullTurnDiff(stored, [full])).toBe(full)
  })
})
