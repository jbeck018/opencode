import { afterEach, expect, test } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Global } from "@opencode-ai/core/global"
import { trace } from "../../../src/cli/cmd/run/trace"

const directTrace = process.env.OPENCODE_DIRECT_TRACE

afterEach(() => {
  if (directTrace === undefined) delete process.env.OPENCODE_DIRECT_TRACE
  else process.env.OPENCODE_DIRECT_TRACE = directTrace
})

test("trace pruning keeps traces another run may still be writing", async () => {
  const dir = path.join(Global.Path.log, "direct")
  await fs.rm(dir, { recursive: true, force: true })
  await fs.mkdir(dir, { recursive: true })
  const name = (index: number) => `20200101T0000${String(index).padStart(2, "0")}Z-${1000 + index}.jsonl`
  const hourAgo = new Date(Date.now() - 60 * 60 * 1000)
  // The three oldest names belong to runs that are still writing; the rest are stale.
  await Promise.all(
    Array.from({ length: 15 }, (_, index) => index).map(async (index) => {
      await Bun.write(path.join(dir, name(index)), "{}\n")
      if (index >= 3) await fs.utimes(path.join(dir, name(index)), hourAgo, hourAgo)
    }),
  )
  process.env.OPENCODE_DIRECT_TRACE = "1"

  trace()

  const names = (await fs.readdir(dir)).filter((item) => item.endsWith(".jsonl"))
  // Fifteen old traces plus the new one; the six oldest names are candidates, and only the three stale ones go.
  expect(names).toHaveLength(13)
  expect(names).toEqual(expect.arrayContaining([name(0), name(1), name(2), name(6)]))
  expect(names.filter((item) => [name(3), name(4), name(5)].includes(item))).toEqual([])
})
