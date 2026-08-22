/**
 * `cumora` — the published, standalone BYOA agent-daemon CLI.
 *
 * This is the entry point for the public npm package (`npx cumora …`). It is
 * intentionally tiny: it only knows how to run the BYOA "agent computer"
 * daemon, which talks to a Cumora server purely over HTTP (no DB/Redis, no
 * repo). The daemon source lives in the main repo
 * (server/src/agents/computer/) and is bundled in by agent-cli/build.mjs, so
 * there's a single source of truth.
 */
import { runComputerDaemon } from '../../server/src/agents/computer/daemon.js'

async function main(): Promise<void> {
  const argv = process.argv.slice(2)
  if (argv[0] === 'agent' && argv[1] === 'computer') {
    await runComputerDaemon(argv.slice(2))
    return
  }
  process.stderr.write(
    'cumora — run your Cumora agents on this machine (BYOA)\n\n' +
    'Usage:\n' +
    '  npx cumora@latest agent computer --pair <code> [--server <url>]      pair this machine\n' +
    '  npx cumora@latest agent computer [--server <url>]                    start the daemon\n' +
    '  npx cumora@latest agent computer --doctor                            check local engines\n' +
    '  npx cumora@latest agent computer --install-service [--server <url>]  install background service\n' +
    '  npx cumora@latest agent computer --status | --logs | --restart       operate the service\n\n' +
    'Needs `claude` (Claude Code), `codex`, `grok` (Grok Build), `pi`, or `cursor-agent` (Cursor) on PATH.\n' +
    'Pairing code: Cumora → You → Computers → Add a computer. Config: ~/.cumora/computer.json\n' +
    'Recovery: restart the foreground daemon or installed service; re-pair if the computer was removed.\n' +
    'Full options: npx cumora@latest agent computer --help\n',
  )
  process.exit(argv.length ? 1 : 0)
}

void main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err)
  process.stderr.write(`cumora: ${message}\n`)
  process.exit(70)
})
