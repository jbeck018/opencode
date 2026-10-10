import { describe, expect, test } from "bun:test"
import { editDiffSource } from "./message-part-edit"

describe("editDiffSource", () => {
  test("uses the stored patch", () => {
    expect(editDiffSource({ file: "a.ts", patch: "@@ -1 +1 @@\n-a\n+b\n" }, "/repo/a.ts")).toEqual({
      file: "a.ts",
      patch: "@@ -1 +1 @@\n-a\n+b\n",
      before: undefined,
      after: undefined,
    })
  })

  test("uses stored before and after contents and falls back to the input path", () => {
    expect(editDiffSource({ before: "a", after: "b" }, "/repo/a.ts")).toEqual({
      file: "/repo/a.ts",
      patch: undefined,
      before: "a",
      after: "b",
    })
  })

  test("skips pruned metadata that keeps only the file name and counts", () => {
    expect(editDiffSource({ file: "a.ts", additions: 3, deletions: 1 } as never, "/repo/a.ts")).toBeUndefined()
  })

  test("skips missing metadata", () => {
    expect(editDiffSource(undefined, "/repo/a.ts")).toBeUndefined()
  })
})
