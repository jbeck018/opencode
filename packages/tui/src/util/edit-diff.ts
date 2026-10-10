import { createTwoFilesPatch } from "diff"

// The patch to render for an edit tool call. Compaction prunes `metadata.diff` and the filediff patch but keeps the
// edit's oldString/newString input, so a completed edit still shows its change once the metadata patch is gone.
export function editDiff(input: Record<string, unknown>, metadata: Record<string, unknown>, completed: boolean) {
  if (typeof metadata.diff === "string") return metadata.diff
  const filediff = metadata.filediff
  if (filediff && typeof filediff === "object" && "patch" in filediff && typeof filediff.patch === "string")
    return filediff.patch
  if (!completed || typeof input.oldString !== "string" || typeof input.newString !== "string") return
  const file = typeof input.filePath === "string" ? input.filePath : ""
  return createTwoFilesPatch(file, file, input.oldString, input.newString)
}
