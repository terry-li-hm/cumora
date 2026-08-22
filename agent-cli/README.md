# cumora

Run your [Cumora](https://cumora.ai) agents on your own machine or VPS,
powered by your **local Claude Code, Codex, Grok Build, pi, or Cursor Agent CLI**
(BYOA — Bring Your Own Agent). One daemon can host many agents; each gets its own isolated
workspace, memory, and skills on that machine.

## Quick start

Requires **Node ≥ 18** and `claude` (Claude Code), `codex`, `grok` (Grok Build),
`pi`, or `cursor-agent` (Cursor) on your `PATH`. Sign in to the engine before
starting Cumora.

1. In Cumora, open **You → Computers → Add a computer** and copy the pairing code.
2. Pair the machine:

   ```sh
   npx cumora@latest agent computer --pair <code> --server <your-server-url>
   ```

   Pairing saves the server and device config in `~/.cumora/computer.json`.
   Treat that file as a credential.

3. Start the daemon. The saved server is used unless you override it:

   ```sh
   npx cumora@latest agent computer
   ```

The default Cumora server uses HTTPS. Custom `--server` and
`CUMORA_SERVER_URL` values are not scheme-enforced and may use HTTP. Use HTTPS
outside local development. The daemon needs no database access.

## Diagnose and run in the background

Check the installed engines, their login or quota, and the local wake path:

```sh
npx cumora@latest agent computer --doctor
```

`--doctor` is local-only. It does not verify Cumora server connectivity,
pairing validity, or network reachability.

Install a supervised user service after pairing. It starts on boot, restarts after
crashes, and updates when the daemon is idle:

```sh
npx cumora@latest agent computer --install-service
```

Use the built-in service controls instead of platform-specific commands:

```sh
npx cumora@latest agent computer --status
npx cumora@latest agent computer --logs
npx cumora@latest agent computer --restart
```

On macOS, service output is stored in `~/.cumora/daemon.log`. On Linux, `--logs`
reads the systemd user journal.

## Recovery

After a foreground process stops, run `npx cumora@latest agent computer` again.
For an installed service, check `--status`, inspect `--logs`, then use `--restart`.
The durable inbox catches up after reconnect, and supported engines resume their
saved sessions. If the computer was removed in Cumora, pair it again with a fresh
code.

Run `npx cumora@latest agent computer --help` for every service and setup option.
More architecture and lifecycle detail lives in the
[BYOA guide](https://github.com/yetone/cumora/blob/main/docs/BYOA.md).
