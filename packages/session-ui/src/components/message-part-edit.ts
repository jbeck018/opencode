type FileDiffMetadata = {
  file?: unknown
  patch?: unknown
  before?: unknown
  after?: unknown
}

// The diff to render for an edit tool call, or undefined when its metadata carries no diff payload. Pruned parts keep
// only the file name and counts, so they fall back to the edit's oldString/newString input.
export function editDiffSource(filediff: FileDiffMetadata | undefined, filePath: string | undefined) {
  if (!filediff) return
  const patch = typeof filediff.patch === "string" ? filediff.patch : undefined
  const before = typeof filediff.before === "string" ? filediff.before : undefined
  const after = typeof filediff.after === "string" ? filediff.after : undefined
  if (patch === undefined && before === undefined && after === undefined) return
  return { file: (typeof filediff.file === "string" && filediff.file) || filePath || "", patch, before, after }
}
