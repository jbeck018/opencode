import { glob, globSync, type GlobOptions } from "glob"
import { minimatch } from "minimatch"

export namespace Glob {
  export interface Options {
    cwd?: string
    absolute?: boolean
    include?: "file" | "all"
    dot?: boolean
    symlink?: boolean
  }

  function toGlobOptions(options: Options): GlobOptions {
    return {
      cwd: options.cwd,
      absolute: options.absolute,
      dot: options.dot,
      follow: options.symlink ?? false,
      nodir: options.include !== "all",
    }
  }

  export async function scan(pattern: string, options: Options = {}): Promise<string[]> {
    return glob(pattern, toGlobOptions(options)) as Promise<string[]>
  }

  export function scanSync(pattern: string, options: Options = {}): string[] {
    return globSync(pattern, toGlobOptions(options)) as string[]
  }

  // The glob package's default: case-insensitive matching on macOS and Windows.
  export const caseInsensitive = process.platform === "darwin" || process.platform === "win32"

  /**
   * The names a relative pattern's first path segment can take, when it is a literal (`agent/*.md`)
   * or a brace list of literals (`{agent,agents}/**`). Undefined when it could match anything.
   */
  export function literalRoots(pattern: string): string[] | undefined {
    const slash = pattern.indexOf("/")
    if (slash <= 0) return undefined
    const first = pattern.slice(0, slash)
    const names = first.startsWith("{") && first.endsWith("}") ? first.slice(1, -1).split(",") : [first]
    if (names.some((name) => name === "" || name === "." || name === ".." || /[*?[\]{}()!@+\\]/.test(name)))
      return undefined
    return names
  }

  export function match(pattern: string, filepath: string): boolean {
    return minimatch(filepath, pattern, { dot: true })
  }
}
