import yargs from "yargs"
import type { Argv } from "yargs"
import { hideBin } from "yargs/helpers"
import { UI } from "./cli/ui"
import { InstallationVersion } from "@opencode-ai/core/installation/version"
import { EOL } from "os"
import { errorMessage } from "./util/error"

const args = hideBin(process.argv)
// Only the flags needed before yargs runs: which command, and whether a TUI launch
// can start the shared server right away.
const early = yargs(args)
  .parserConfiguration({ "populate--": true })
  // Disable the built-ins first; declaring --help/--version over them warns.
  .help(false)
  .version(false)
  .options({
    help: { type: "boolean", alias: "h" },
    version: { type: "boolean", alias: "v" },
    mini: { type: "boolean" },
    standalone: { type: "boolean" },
    mdns: { type: "boolean" },
    pure: { type: "boolean" },
    "print-logs": { type: "boolean" },
    "log-level": { type: "string" },
    port: { type: "string" },
    hostname: { type: "string" },
  })
  .parseSync()
// Set before any opencode module is evaluated so flag snapshots see them.
if (early["print-logs"]) process.env.OPENCODE_PRINT_LOGS = "1"
if (early["log-level"]) process.env.OPENCODE_LOG_LEVEL = early["log-level"]
if (early.pure) process.env.OPENCODE_PURE = "1"

// Order is the order `opencode --help` lists commands.
const COMMANDS: { names: string[]; load: () => Promise<(argv: Argv) => Argv> }[] = [
  {
    names: ["acp"],
    load: async () => {
      const { AcpCommand } = await import("./cli/cmd/acp")
      return (argv: Argv) => argv.command(AcpCommand)
    },
  },
  {
    names: ["mcp"],
    load: async () => {
      const { McpCommand } = await import("./cli/cmd/mcp")
      return (argv: Argv) => argv.command(McpCommand)
    },
  },
  {
    names: [],
    load: async () => {
      const { TuiThreadCommand } = await import("./cli/cmd/tui")
      return (argv: Argv) => argv.command(TuiThreadCommand)
    },
  },
  {
    names: ["attach"],
    load: async () => {
      const { AttachCommand } = await import("./cli/cmd/attach")
      return (argv: Argv) => argv.command(AttachCommand)
    },
  },
  {
    names: ["run"],
    load: async () => {
      const { RunCommand } = await import("./cli/cmd/run")
      return (argv: Argv) => argv.command(RunCommand)
    },
  },
  {
    names: ["generate"],
    load: async () => {
      const { GenerateCommand } = await import("./cli/cmd/generate")
      return (argv: Argv) => argv.command(GenerateCommand)
    },
  },
  {
    names: ["debug"],
    load: async () => {
      const { DebugCommand } = await import("./cli/cmd/debug")
      return (argv: Argv) => argv.command(DebugCommand)
    },
  },
  {
    names: ["console"],
    load: async () => {
      const { ConsoleCommand } = await import("./cli/cmd/account")
      return (argv: Argv) => argv.command(ConsoleCommand)
    },
  },
  {
    names: ["providers", "auth"],
    load: async () => {
      const { ProvidersCommand } = await import("./cli/cmd/providers")
      return (argv: Argv) => argv.command(ProvidersCommand)
    },
  },
  {
    names: ["agent"],
    load: async () => {
      const { AgentCommand } = await import("./cli/cmd/agent")
      return (argv: Argv) => argv.command(AgentCommand)
    },
  },
  {
    names: ["upgrade"],
    load: async () => {
      const { UpgradeCommand } = await import("./cli/cmd/upgrade")
      return (argv: Argv) => argv.command(UpgradeCommand)
    },
  },
  {
    names: ["uninstall"],
    load: async () => {
      const { UninstallCommand } = await import("./cli/cmd/uninstall")
      return (argv: Argv) => argv.command(UninstallCommand)
    },
  },
  {
    names: ["serve"],
    load: async () => {
      const { ServeCommand } = await import("./cli/cmd/serve")
      return (argv: Argv) => argv.command(ServeCommand)
    },
  },
  {
    names: ["web"],
    load: async () => {
      const { WebCommand } = await import("./cli/cmd/web")
      return (argv: Argv) => argv.command(WebCommand)
    },
  },
  {
    names: ["models"],
    load: async () => {
      const { ModelsCommand } = await import("./cli/cmd/models")
      return (argv: Argv) => argv.command(ModelsCommand)
    },
  },
  {
    names: ["stats"],
    load: async () => {
      const { StatsCommand } = await import("./cli/cmd/stats")
      return (argv: Argv) => argv.command(StatsCommand)
    },
  },
  {
    names: ["export"],
    load: async () => {
      const { ExportCommand } = await import("./cli/cmd/export")
      return (argv: Argv) => argv.command(ExportCommand)
    },
  },
  {
    names: ["import"],
    load: async () => {
      const { ImportCommand } = await import("./cli/cmd/import")
      return (argv: Argv) => argv.command(ImportCommand)
    },
  },
  {
    names: ["github"],
    load: async () => {
      const { GithubCommand } = await import("./cli/cmd/github")
      return (argv: Argv) => argv.command(GithubCommand)
    },
  },
  {
    names: ["pr"],
    load: async () => {
      const { PrCommand } = await import("./cli/cmd/pr")
      return (argv: Argv) => argv.command(PrCommand)
    },
  },
  {
    names: ["session"],
    load: async () => {
      const { SessionCommand } = await import("./cli/cmd/session")
      return (argv: Argv) => argv.command(SessionCommand)
    },
  },
  {
    names: ["plugin", "plug"],
    load: async () => {
      const { PluginCommand } = await import("./cli/cmd/plug")
      return (argv: Argv) => argv.command(PluginCommand)
    },
  },
  {
    names: ["db"],
    load: async () => {
      const { DbCommand } = await import("./cli/cmd/db")
      return (argv: Argv) => argv.command(DbCommand)
    },
  },
]

// The first positional naming a command selects it, matching how yargs dispatches.
const positionals = early._.map(String)
const name = positionals.find((arg) => COMMANDS.some((entry) => entry.names.includes(arg)))
const selected = name === undefined ? undefined : COMMANDS.find((entry) => entry.names.includes(name))
const everything =
  (early.help && !selected) || positionals[0] === "completion" || args.includes("--get-yargs-completions")
const tui = !selected && !everything && !early.version

// A plain TUI launch starts finding or spawning the shared server before the TUI
// itself loads; the TUI command picks up the same in-flight connection.
if (
  tui &&
  !early.mini &&
  !early.standalone &&
  early.port === undefined &&
  early.hostname === undefined &&
  !early.mdns
) {
  const { SharedServer } = await import("./server/shared")
  // Wait only until the server is found or spawned (milliseconds), not until it has
  // booted: module evaluation below is synchronous and would otherwise hold the
  // spawn back until the whole TUI has loaded.
  if (!SharedServer.disabled()) await SharedServer.connect().started
}

async function commands() {
  // Sequential on purpose: evaluating several command graphs concurrently trips
  // import cycles (e.g. Global <-> EffectFlock) that a single ordered load does not.
  if (everything) {
    const loaded: Array<(argv: Argv) => Argv> = []
    for (const entry of COMMANDS) loaded.push(await entry.load())
    return loaded
  }
  if (selected) return [await selected.load()]
  if (tui) return [await COMMANDS.find((entry) => entry.names.length === 0)!.load()]
  return []
}

function show(out: string) {
  const text = out.trimStart()
  if (!text.startsWith("opencode ")) {
    process.stderr.write(UI.logo() + EOL + EOL)
    process.stderr.write(text + EOL)
    return
  }
  process.stderr.write(out)
}

const cli = yargs(args)
  .parserConfiguration({ "populate--": true })
  .scriptName("opencode")
  .wrap(100)
  .help("help", "show help")
  .alias("help", "h")
  .version("version", "show version number", InstallationVersion)
  .alias("version", "v")
  .option("print-logs", {
    describe: "print logs to stderr",
    type: "boolean",
  })
  .option("log-level", {
    describe: "log level",
    type: "string",
    choices: ["DEBUG", "INFO", "WARN", "ERROR"],
  })
  .option("pure", {
    describe: "run without external plugins",
    type: "boolean",
  })
  .middleware(async (opts) => {
    if (opts.printLogs) process.env.OPENCODE_PRINT_LOGS = "1"
    if (opts.logLevel) process.env.OPENCODE_LOG_LEVEL = opts.logLevel
    if (opts.pure) {
      process.env.OPENCODE_PURE = "1"
    }

    const { Heap } = await import("./cli/heap")
    Heap.start()

    process.env.AGENT = "1"
    process.env.OPENCODE = "1"
    process.env.OPENCODE_PID = String(process.pid)
  })
  .usage("")
  .completion("completion", "generate shell completion script")
  .fail((msg, err) => {
    if (
      msg?.startsWith("Unknown argument") ||
      msg?.startsWith("Not enough non-option arguments") ||
      msg?.startsWith("Invalid values:")
    ) {
      if (err) throw err
      cli.showHelp(show)
    }
    if (err) throw err
    process.exit(1)
  })
  .strict()

for (const register of await commands()) register(cli)

try {
  if (args.includes("-h") || args.includes("--help")) {
    await cli.parse(args, (err: Error | undefined, _argv: unknown, out: string) => {
      if (err) throw err
      if (!out) return
      show(out)
    })
  } else {
    await cli.parse()
  }
} catch (e) {
  const { FormatError } = await import("./cli/error")
  const formatted = FormatError(e)
  if (formatted) UI.error(formatted)
  if (formatted === undefined) {
    UI.error("Unexpected error" + EOL)
    process.stderr.write(errorMessage(e) + EOL)
  }
  process.exitCode = 1
} finally {
  // Some subprocesses don't react properly to SIGTERM and similar signals.
  // Most notably, some docker-container-based MCP servers don't handle such signals unless
  // run using `docker run --init`.
  // Explicitly exit to avoid any hanging subprocesses.
  process.exit()
}
