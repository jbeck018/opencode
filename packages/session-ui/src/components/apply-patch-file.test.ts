import { describe, expect, test } from "bun:test"
import { patchFiles } from "./apply-patch-file"
import { text } from "./session-diff"

describe("apply patch file", () => {
  test("parses patch metadata from the server", () => {
    const file = patchFiles([
      {
        filePath: "/tmp/a.ts",
        relativePath: "a.ts",
        type: "update",
        patch:
          "Index: a.ts\n===================================================================\n--- a.ts\t\n+++ a.ts\t\n@@ -1,2 +1,2 @@\n one\n-two\n+three\n",
        additions: 1,
        deletions: 1,
      },
    ])[0]

    expect(file).toBeDefined()
    expect(file?.view?.fileDiff.name).toBe("a.ts")
    expect(file?.view?.fileDiff.isPartial).toBe(false)
    expect(text(file!.view!, "deletions")).toBe("one\ntwo\n")
    expect(text(file!.view!, "additions")).toBe("one\nthree\n")
  })

  test("keeps compacted entries without patch content as header-only files", () => {
    const files = patchFiles([
      { filePath: "/tmp/a.ts", relativePath: "a.ts", type: "update", additions: 2, deletions: 1 },
      { filePath: "/tmp/b.ts", relativePath: "b.ts", type: "delete", additions: 0, deletions: 4 },
      { relativePath: "c.ts", type: "update", additions: 1, deletions: 0 },
    ])

    expect(files.map((file) => [file.relativePath, file.type, file.additions, file.deletions, file.view])).toEqual([
      ["a.ts", "update", 2, 1, undefined],
      ["b.ts", "delete", 0, 4, undefined],
    ])
  })

  test("keeps legacy before and after payloads working", () => {
    const file = patchFiles([
      {
        filePath: "/tmp/a.ts",
        relativePath: "a.ts",
        type: "update",
        before: "one\n",
        after: "two\n",
        additions: 1,
        deletions: 1,
      },
    ])[0]

    expect(file).toBeDefined()
    expect(text(file!.view!, "deletions")).toBe("one\n")
    expect(text(file!.view!, "additions")).toBe("two\n")
  })
})
