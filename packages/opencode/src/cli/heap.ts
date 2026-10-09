import fs from "fs/promises"
import path from "path"
import { writeHeapSnapshot } from "node:v8"
import { Flag } from "@opencode-ai/core/flag/flag"
import { Global } from "@opencode-ai/core/global"
const MINUTE = 60_000
const LIMIT = 2 * 1024 * 1024 * 1024
const KEEP = 10

let timer: Timer | undefined
let lock = false
let armed = true

export function start() {
  if (!Flag.OPENCODE_AUTO_HEAP_SNAPSHOT) return
  if (timer) return

  const run = async () => {
    if (lock) return

    const stat = process.memoryUsage()
    if (stat.rss <= LIMIT) {
      armed = true
      return
    }
    if (!armed) return

    lock = true
    armed = false
    const file = path.join(
      Global.Path.log,
      `heap-${process.pid}-${new Date().toISOString().replace(/[:.]/g, "")}.heapsnapshot`,
    )
    await Promise.resolve()
      .then(() => writeHeapSnapshot(file))
      .then(() => prune())
      .catch(() => {})

    lock = false
  }

  timer = setInterval(() => {
    void run()
  }, MINUTE)
  timer.unref?.()
}

async function prune() {
  const files = await fs.readdir(Global.Path.log)
  const snapshots = await Promise.all(
    files
      .filter((name) => name.startsWith("heap-") && name.endsWith(".heapsnapshot"))
      .map(async (name) => ({ name, time: (await fs.stat(path.join(Global.Path.log, name))).mtimeMs })),
  )
  await Promise.all(
    snapshots
      .sort((a, b) => b.time - a.time)
      .slice(KEEP)
      .map((item) => fs.rm(path.join(Global.Path.log, item.name), { force: true })),
  )
}

export * as Heap from "./heap"
