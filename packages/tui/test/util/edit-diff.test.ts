import { describe, expect, test } from "bun:test"
import { editDiff } from "../../src/util/edit-diff"

const input = { filePath: "src/a.ts", oldString: "one\ntwo\n", newString: "one\nthree\n" }

describe("editDiff", () => {
  test("prefers the metadata diff, then the filediff patch", () => {
    expect(editDiff(input, { diff: "D", filediff: { patch: "P" } }, true)).toBe("D")
    expect(editDiff(input, { filediff: { file: "src/a.ts", patch: "P" } }, true)).toBe("P")
  })

  test("rebuilds the diff from the edit input once compaction pruned the metadata patch", () => {
    const diff = editDiff(input, { filediff: { file: "src/a.ts", additions: 1, deletions: 1 } }, true)
    expect(diff).toContain("--- src/a.ts")
    expect(diff).toContain("-two")
    expect(diff).toContain("+three")
  })

  test("does not invent a diff for an unfinished edit or missing input", () => {
    expect(editDiff(input, {}, false)).toBeUndefined()
    expect(editDiff({ filePath: "src/a.ts" }, {}, true)).toBeUndefined()
  })
})
