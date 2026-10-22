/**
 * Tiny dev helper — mint a runtime JWT for an agent and print it +
 * the company id. Used to drive a manual HTTP-mode turn-worker run
 * for smoke-testing the /runtime/* endpoints.
 *
 *   tsx server/src/agents/runtime/probe.ts <agentId>
 *
 * Prints EXACTLY two lines to stdout:
 *   COMPANY=<companyId>
 *   TOKEN=<jwt>
 *
 * The token is minted through the same assignment-bound path the
 * orchestrator uses (mintManagedRuntimeToken), so a probe credential
 * carries the agent's observed cloud computer + persisted assignment
 * version and is rejected the moment the assignment changes — never a
 * legacy unbound token. The probe pins the historical default TTL of
 * `signAgentToken` (exp − iat = 3600s); only pods keep the 24h lifetime.
 *
 * CLI contract preserved from the original: usage error → exit 2, agent
 * not found / no managed assignment → exit 1 (stderr), success → the
 * two stdout lines + exit 0. Because importing the orchestrator
 * transitively loads modules that log to stdout at import time (e.g.
 * storage's mode banner), the import + mint run with console.log
 * suppressed so stdout stays exactly two lines. Exit discipline: the
 * orchestrator import also opens the eager Redis clients, so after
 * printing we disconnect Redis, end the pool, and exit(0) — the process
 * is bounded on the success path instead of dangling on those handles
 * until a test-runner timeout reaps it.
 */
import { pool } from '../../db/pool.js'

/** Historical probe TTL — matches signAgentToken's default (1h). */
const PROBE_TOKEN_TTL_SECONDS = 60 * 60

/** Run `fn` with console.log silenced (stdout-preserving). Errors and
 *  console.warn still reach stderr; only the chatty module-load banners
 *  that would pollute the two-line stdout contract are dropped. */
async function withQuietStdout<T>(fn: () => Promise<T>): Promise<T> {
  const original = console.log
  console.log = () => { /* swallow import-time banners */ }
  try {
    return await fn()
  } finally {
    console.log = original
  }
}

async function main(): Promise<void> {
  const agentId = process.argv[2]
  if (!agentId) {
    console.error('usage: tsx server/src/agents/runtime/probe.ts <agentId>')
    process.exit(2)
  }
  const minted = await withQuietStdout(async () => {
    const { rows } = await pool.query<{ id: string }>(
      'SELECT id FROM participants WHERE id = $1',
      [agentId],
    )
    if (rows.length === 0) {
      console.error(`agent ${agentId} not found`)
      process.exit(1)
    }
    const { mintManagedRuntimeToken } = await import('./orchestrator.js')
    return mintManagedRuntimeToken(agentId, PROBE_TOKEN_TTL_SECONDS)
  })
  if (!minted) {
    console.error(`agent ${agentId} has no current managed computer assignment — remint after assigning the agent to a cloud computer`)
    process.exit(1)
  }
  console.log(`COMPANY=${minted.companyId ?? ''}`)
  console.log(`TOKEN=${minted.token}`)
  // Bounded success exit: tear down the eager Redis clients the orchestrator
  // import opened, then end the pool and exit explicitly. process.exit makes
  // the exit unconditional even if some transitive import retains a handle.
  try {
    const { redis, sub } = await import('../../redis.js')
    redis.disconnect()
    sub.disconnect()
  } catch { /* already gone — exit below still bounds the process */ }
  try { await pool.end() } catch { /* already ended */ }
  process.exit(0)
}

void main().catch((err) => {
  console.error(err)
  process.exit(1)
})
