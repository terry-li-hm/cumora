/**
 * Integration test: the `/runtime/*` HTTP surface that agent-runner pods
 * talk to (server/src/agents/runtime/server.ts).
 *
 * The unit tests in __tests__ cover the argv-strip helper and the JWT
 * sign/verify in isolation; this file glues them together with a real
 * Express mount + real Postgres so we can assert the *security-critical*
 * end-to-end behaviour:
 *
 *  - Auth gate: missing / wrong-scheme / bad-sig / expired tokens are
 *    rejected with 401 BEFORE any handler logic runs.
 *  - Assignment revalidation: EVERY authenticated request re-checks the
 *    runtime credential against the agent's CURRENT assignment — company,
 *    host computer, opaque persisted assignment version, computer
 *    revocation, departure, and token expiry. Stale credentials from a
 *    previous assignment (local A→B, managed A→B, A→B→A return,
 *    departure→reactivation) must 401; an unchanged re-assignment must
 *    keep working tokens valid (idempotence).
 *  - Identity pin: when a request body claims `agentId: <spoof>` while
 *    the JWT pins someone else, the row that lands in Postgres uses the
 *    JWT subject, not the body claim.
 *  - Payload validation: 400s for missing required fields, without ever
 *    touching the DB.
 *
 * The full /cli endpoint (which actually spawns runCli) is intentionally
 * not exercised here — booting the CLI requires the whole agent stack.
 * Its security-critical bit (argv strip + JWT-sub injection) lives in
 * agents-runtime-cli-argv.test.ts as a unit test.
 */
import { test, before, beforeEach, after } from 'node:test'
import assert from 'node:assert/strict'
import { createServer, type Server } from 'node:http'
import { createHmac, randomUUID } from 'node:crypto'
import type { ReadableStreamDefaultReader } from 'node:stream/web'
import { ensureSchemaOnce, resetAllTables, teardownAll } from './_helpers.js'
import { signAgentToken, verifyAgentToken } from '../agents/runtime/jwt.js'
import { mintAgentRuntimeToken, assignAgentToComputer, revokeComputer, pairComputer } from '../agents/computer/registry.js'
import { pool } from '../db/pool.js'
import { env } from '../env.js'

let server: Server
let baseUrl = ''

before(async () => {
  await ensureSchemaOnce()
  // Mount the runtime router on a minimal Express app. We can't use the
  // real index.ts entrypoint because it boots schedulers + cron + redis
  // subscribers we don't want in this test process.
  const expressMod = await import('express')
  const express = expressMod.default
  const { runtimeRouter, __setRuntimeAssignmentOverrideForTesting } = await import('../agents/runtime/server.js')
  // Guarded so a baseline (pre-binding) server keeps its preserved tests
  // green in red-tree runs; only the revalidation regressions fail there.
  __setRuntimeAssignmentOverrideForTesting?.(null)
  const app = express()
  app.use(express.json({ limit: '4mb' }))
  app.use('/runtime', runtimeRouter)
  await new Promise<void>((resolve) => {
    server = createServer(app).listen(0, () => {
      const addr = server.address()
      if (addr && typeof addr === 'object') baseUrl = `http://127.0.0.1:${addr.port}`
      resolve()
    })
  })
})

beforeEach(async () => {
  await resetAllTables()
})

after(async () => {
  await teardownAll(server)
})

async function call(
  path: string,
  opts: { method?: string; token?: string; body?: unknown; headers?: Record<string, string> } = {},
): Promise<{ status: number; body: any }> {
  const headers: Record<string, string> = { 'content-type': 'application/json', ...(opts.headers ?? {}) }
  if (opts.token) headers['authorization'] = `Bearer ${opts.token}`
  const res = await fetch(`${baseUrl}${path}`, {
    method: opts.method ?? 'POST',
    headers,
    body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
  })
  const text = await res.text()
  let parsed: any = null
  try { parsed = text ? JSON.parse(text) : null } catch { parsed = text }
  return { status: res.status, body: parsed }
}

async function seedCompany(companyId?: string): Promise<string> {
  const id = companyId ?? `c-${randomUUID().slice(0, 8)}`
  await pool.query(
    `INSERT INTO companies (id, name, slug, owner_user_id) VALUES ($1, $2, $3, $4)`,
    [id, `Co ${id}`, id, 'test-owner'],
  )
  return id
}

async function seedComputer(
  companyId: string,
  kind: 'cloud' | 'local',
  engines: string[],
): Promise<string> {
  const computerId = kind === 'cloud' ? `cloud-${companyId}` : `local-${randomUUID().slice(0, 8)}`
  await pool.query(
    `INSERT INTO computers (id, company_id, name, kind, available_engines, status)
       VALUES ($1, $2, $3, $4, $5::jsonb, 'online')`,
    [computerId, companyId, kind === 'cloud' ? 'Cumora Cloud' : 'Test Mac', kind, JSON.stringify(engines)],
  )
  return computerId
}

async function seedAgentOn(opts: {
  companyId?: string
  kind?: 'cloud' | 'local'
}): Promise<{
  agentId: string
  companyId: string
  computerId: string
  assignmentVersion: string
  token: string
}> {
  const companyId = await seedCompany(opts.companyId)
  const kind = opts.kind ?? 'cloud'
  const computerId = await seedComputer(companyId, kind, kind === 'cloud' ? ['managed'] : ['claude'])
  const agentId = `a-${randomUUID().slice(0, 8)}`
  await pool.query(
    `INSERT INTO participants (id, company_id, kind, name, role, initial, avatar_bg, status, computer_id, engine)
       VALUES ($1, $2, 'agent', $3, 'tester', $4, '#abcdef', 'avail', $5, $6)`,
    [agentId, companyId, agentId, agentId.slice(0, 1).toUpperCase(), computerId, kind === 'cloud' ? 'managed' : 'claude'],
  )
  const minted = await mintAgentRuntimeToken({ agentId, computerId })
  assert.ok(minted, `seed mint must succeed for ${kind} agent`)
  return {
    agentId,
    companyId,
    computerId,
    assignmentVersion: minted!.assignmentVersion,
    token: minted!.token,
  }
}

async function seedAgent(): Promise<{ agentId: string; companyId: string; computerId: string; assignmentVersion: string; token: string }> {
  return seedAgentOn({ kind: 'cloud' })
}

async function seedByoaAgent(): Promise<{
  agentId: string
  companyId: string
  computerId: string
  assignmentVersion: string
  token: string
}> {
  return seedAgentOn({ kind: 'local' })
}

/** Hand-mint a LEGACY (pre-assignment-binding) BYOA token — same HS256
 *  shape the old daemon minted, with no computerId / assignmentVersion. */
function signLegacyByoaToken(args: {
  agentId: string
  companyId: string
  ttlSeconds?: number
}): string {
  const now = Math.floor(Date.now() / 1000)
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url')
  const payload = Buffer.from(JSON.stringify({
    sub: args.agentId,
    companyId: args.companyId,
    runtimeKind: 'byoa',
    scope: 'agent-runner',
    iat: now,
    exp: now + (args.ttlSeconds ?? 60 * 60),
  })).toString('base64url')
  const signature = createHmac('sha256', env.AGENT_RUNTIME_SECRET)
    .update(`${header}.${payload}`)
    .digest('base64url')
  return `${header}.${payload}.${signature}`
}

// ── auth gate ──────────────────────────────────────────────────────────

test('[integration] runtime: missing Authorization → 401', async () => {
  const r = await call('/runtime/inbox', { method: 'GET' })
  assert.equal(r.status, 401)
  assert.match(String(r.body?.error ?? ''), /bearer/i)
})

test('[integration] runtime: wrong scheme (Basic instead of Bearer) → 401', async () => {
  const r = await call('/runtime/inbox', {
    method: 'GET',
    headers: { authorization: 'Basic dXNlcjpwYXNz' },
  })
  assert.equal(r.status, 401)
})

test('[integration] runtime: bad signature → 401', async () => {
  // Mint a valid token, then tamper with the signature segment.
  const { token } = await seedAgent()
  const tampered = token.replace(/\.[^.]*$/, '.AAAA')
  const r = await call('/runtime/inbox', { method: 'GET', token: tampered })
  assert.equal(r.status, 401)
  assert.match(String(r.body?.error ?? ''), /signature|malformed/i)
})

test('[integration] runtime: malformed token (not three segments) → 401', async () => {
  const r = await call('/runtime/inbox', { method: 'GET', token: 'not-a-jwt' })
  assert.equal(r.status, 401)
  assert.match(String(r.body?.error ?? ''), /malformed/i)
})

test('[integration] runtime: expired token → 401', async () => {
  const seeded = await seedAgent()
  // ttlSeconds = -1 → exp = now - 1s, definitely expired.
  const tok = signAgentToken({
    agentId: seeded.agentId, companyId: seeded.companyId, runtimeKind: 'managed',
    computerId: seeded.computerId, assignmentVersion: seeded.assignmentVersion, ttlSeconds: -1,
  })
  const r = await call('/runtime/inbox', { method: 'GET', token: tok })
  assert.equal(r.status, 401)
  assert.match(String(r.body?.error ?? ''), /expired/i)
})

// ── assignment revalidation: every request re-checks the live state ───

test('[integration] runtime: missing, cross-company, departed, and revoked assignments → 401', async (t) => {
  await t.test('missing agent', async () => {
    const token = signAgentToken({
      agentId: 'missing-agent', companyId: 'missing-company', runtimeKind: 'managed',
      computerId: 'cloud-missing-company', assignmentVersion: 'av-x',
    })
    const r = await call('/runtime/inbox', { method: 'GET', token })
    assert.equal(r.status, 401)
    assert.match(String(r.body?.error ?? ''), /no longer assigned/i)
  })

  await t.test('cross-company computer assignment', async () => {
    const seeded = await seedAgent()
    const otherCompanyId = await seedCompany()
    const otherComputerId = await seedComputer(otherCompanyId, 'cloud', ['managed'])
    await pool.query(
      `UPDATE participants SET computer_id = $1, engine = 'managed'
        WHERE id = $2 AND company_id = $3`,
      [otherComputerId, seeded.agentId, seeded.companyId],
    )
    const r = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
    assert.equal(r.status, 401)
    assert.match(String(r.body?.error ?? ''), /no longer assigned/i)
  })

  await t.test('departed agent', async () => {
    const seeded = await seedByoaAgent()
    await pool.query(`UPDATE participants SET departed_at = NOW() WHERE id = $1`, [seeded.agentId])
    const r = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
    assert.equal(r.status, 401)
    assert.match(String(r.body?.error ?? ''), /no longer assigned/i)
  })

  await t.test('revoked computer', async () => {
    const seeded = await seedByoaAgent()
    await pool.query(`UPDATE computers SET revoked_at = NOW() WHERE id = $1`, [seeded.computerId])
    const r = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
    assert.equal(r.status, 401)
    assert.match(String(r.body?.error ?? ''), /no longer assigned/i)
  })
})

test('[integration] runtime: local A→B reassignment invalidates the A credential; remint on B works', async () => {
  const seeded = await seedByoaAgent()
  const before = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(before.status, 200, 'token must work before the move')

  const computerB = await seedComputer(seeded.companyId, 'local', ['claude'])
  const moved = await assignAgentToComputer({
    agentId: seeded.agentId, companyId: seeded.companyId, computerId: computerB,
  })
  assert.ok(moved, 'assignment to B must succeed')
  assert.ok(typeof moved!.assignmentVersion === 'string' && moved!.assignmentVersion.length > 0,
    'assignment result must expose the new opaque version')
  assert.notEqual(moved!.assignmentVersion, seeded.assignmentVersion,
    'effective host change must rotate the assignment version')

  const stale = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(stale.status, 401, 'credential minted on host A must die after A→B')
  assert.match(String(stale.body?.error ?? ''), /no longer assigned/i)

  const remint = await mintAgentRuntimeToken({ agentId: seeded.agentId, computerId: computerB })
  assert.ok(remint, 'remint on B must succeed')
  assert.equal(remint!.assignmentVersion, moved!.assignmentVersion)
  const fresh = await call('/runtime/inbox', { method: 'GET', token: remint!.token })
  assert.equal(fresh.status, 200, 'reminted B credential must be admitted')
})

test('[integration] runtime: local A→B→A return does NOT resurrect the original A credential', async () => {
  const seeded = await seedByoaAgent()
  const computerB = await seedComputer(seeded.companyId, 'local', ['claude'])

  await assignAgentToComputer({ agentId: seeded.agentId, companyId: seeded.companyId, computerId: computerB })
  const back = await assignAgentToComputer({
    agentId: seeded.agentId, companyId: seeded.companyId, computerId: seeded.computerId, engine: 'claude',
  })
  assert.ok(back)
  assert.notEqual(back!.assignmentVersion, seeded.assignmentVersion,
    'returning to the same host must still have rotated the version twice')

  const revived = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(revived.status, 401,
    'A→B→A must not revive the credential minted during the FIRST stay on A')
  assert.match(String(revived.body?.error ?? ''), /no longer assigned/i)

  const remint = await mintAgentRuntimeToken({ agentId: seeded.agentId, computerId: seeded.computerId })
  assert.ok(remint)
  assert.notEqual(remint!.assignmentVersion, seeded.assignmentVersion)
  const fresh = await call('/runtime/inbox', { method: 'GET', token: remint!.token })
  assert.equal(fresh.status, 200, 'a NEW mint on the returned-to host must work')
})

test('[integration] runtime: managed A→B reassignment invalidates the managed credential', async () => {
  const seeded = await seedAgent()
  // A second managed (cloud) computer in the same company — pod moves
  // between managed hosts must rotate the binding just like BYOA moves.
  const cloudB = `cloud-${randomUUID().slice(0, 8)}`
  await pool.query(
    `INSERT INTO computers (id, company_id, name, kind, available_engines, status)
       VALUES ($1, $2, 'Second Cloud', 'cloud', '["managed"]'::jsonb, 'online')`,
    [cloudB, seeded.companyId],
  )
  const moved = await assignAgentToComputer({
    agentId: seeded.agentId, companyId: seeded.companyId, computerId: cloudB,
  })
  assert.ok(moved)
  assert.notEqual(moved!.assignmentVersion, seeded.assignmentVersion)

  const stale = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(stale.status, 401, 'managed credential minted on cloud A must die after A→B')
  const remint = await mintAgentRuntimeToken({ agentId: seeded.agentId, computerId: cloudB })
  assert.ok(remint)
  assert.equal(remint!.runtimeKind, 'managed')
  const fresh = await call('/runtime/inbox', { method: 'GET', token: remint!.token })
  assert.equal(fresh.status, 200)
})

test('[integration] runtime: managed→local→managed return does NOT resurrect the managed credential', async () => {
  const seeded = await seedAgent()
  const localHost = await seedComputer(seeded.companyId, 'local', ['claude'])

  await assignAgentToComputer({ agentId: seeded.agentId, companyId: seeded.companyId, computerId: localHost })
  const back = await assignAgentToComputer({
    agentId: seeded.agentId, companyId: seeded.companyId, computerId: seeded.computerId,
  })
  assert.ok(back)
  assert.notEqual(back!.assignmentVersion, seeded.assignmentVersion,
    'the cloud computer id is identical, but the version must have rotated twice')

  const revived = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(revived.status, 401,
    'managed→local→managed must not revive the original managed credential — host identity alone is insufficient')
  assert.match(String(revived.body?.error ?? ''), /no longer assigned/i)
})

test('[integration] runtime: unchanged re-assignment is idempotent — valid tokens keep working', async () => {
  const seeded = await seedByoaAgent()
  const again = await assignAgentToComputer({
    agentId: seeded.agentId, companyId: seeded.companyId, computerId: seeded.computerId, engine: 'claude',
  })
  assert.ok(again)
  assert.equal(again!.assignmentVersion, seeded.assignmentVersion,
    'repeating the SAME assignment must NOT rotate the version')

  const r = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(r.status, 200, 'unchanged assignment must preserve valid work/credentials')
})

test('[integration] runtime: departure→reactivation does NOT resurrect the pre-departure credential', async () => {
  const seeded = await seedByoaAgent()
  await pool.query(`UPDATE participants SET departed_at = NOW() WHERE id = $1`, [seeded.agentId])
  const departed = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(departed.status, 401)

  // Reactivation (the re-hire route clears departed_at via plain SQL).
  await pool.query(`UPDATE participants SET departed_at = NULL WHERE id = $1`, [seeded.agentId])
  const revived = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(revived.status, 401,
    'reactivation must not revive the credential minted before departure')
  assert.match(String(revived.body?.error ?? ''), /no longer assigned/i)

  const remint = await mintAgentRuntimeToken({ agentId: seeded.agentId, computerId: seeded.computerId })
  assert.ok(remint, 'remint after reactivation must succeed')
  assert.notEqual(remint!.assignmentVersion, seeded.assignmentVersion)
  const fresh = await call('/runtime/inbox', { method: 'GET', token: remint!.token })
  assert.equal(fresh.status, 200)
})

test('[integration] runtime: revocation invalidates derived credentials — and STAYS invalid after the computer is un-revoked', async () => {
  const seeded = await seedByoaAgent()
  assert.equal(await revokeComputer({ computerId: seeded.computerId, companyId: seeded.companyId }), true)

  const revoked = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(revoked.status, 401)
  assert.match(String(revoked.body?.error ?? ''), /no longer assigned/i)

  // Re-pairing the same computer row clears revoked_at. Without a version
  // rotation at revocation time this would silently resurrect the old token.
  await pool.query(`UPDATE computers SET revoked_at = NULL WHERE id = $1`, [seeded.computerId])
  const still = await call('/runtime/inbox', { method: 'GET', token: seeded.token })
  assert.equal(still.status, 401,
    'un-revoking the computer row must not revive credentials minted before revocation')
})

test('[integration] runtime: revoking an agentless computer succeeds and stays revoked; repeat-revoke returns false', async () => {
  // Regression for the CTE boolean: revokeComputer's success signal is the
  // REVOKED-COMPUTERS row, never the outer participants-rotation row count —
  // a non-cloud computer with NO assigned agents is still genuinely revoked
  // (and must reach the offline broadcast), while a repeat-revoke revoked
  // nothing and must stay false.
  const companyId = await seedCompany()
  const computerId = await seedComputer(companyId, 'local', ['claude'])
  // No participant is assigned to this computer — rotation matches zero rows.
  const first = await revokeComputer({ computerId, companyId })
  assert.equal(first, true, 'an agentless non-cloud computer is still genuinely revoked')
  const { rows } = await pool.query<{ revoked_at: string | null; status: string }>(
    `SELECT revoked_at, status FROM computers WHERE id = $1`, [computerId])
  assert.ok(rows[0].revoked_at !== null, 'the revocation itself must have committed')
  assert.equal(rows[0].status, 'offline')

  const second = await revokeComputer({ computerId, companyId })
  assert.equal(second, false, 'repeat-revoke of an already-revoked row must return false')
})

test('[integration] runtime: departed and cross-computer agents cannot mint runtime tokens', async () => {
  const seeded = await seedByoaAgent()
  const computerB = await seedComputer(seeded.companyId, 'local', ['claude'])
  assert.ok(await mintAgentRuntimeToken({ agentId: seeded.agentId, computerId: seeded.computerId }))

  await pool.query(`UPDATE participants SET departed_at = NOW() WHERE id = $1`, [seeded.agentId])
  assert.equal(await mintAgentRuntimeToken({ agentId: seeded.agentId, computerId: seeded.computerId }), null,
    'departed agent must not mint')

  await pool.query(`UPDATE participants SET departed_at = NULL WHERE id = $1`, [seeded.agentId])
  assert.equal(await mintAgentRuntimeToken({ agentId: seeded.agentId, computerId: computerB }), null,
    'minting against a computer the agent is NOT assigned to must fail')
})

test('[integration] runtime: legacy unbound tokens fail closed and cannot extend the upgrade window', async () => {
  const seeded = await seedByoaAgent()
  const shortLegacy = signLegacyByoaToken({ agentId: seeded.agentId, companyId: seeded.companyId })
  const shortResult = await call('/runtime/inbox', { method: 'GET', token: shortLegacy })
  assert.equal(shortResult.status, 401, 'legacy unbound token must fail closed')
  assert.match(String(shortResult.body?.error ?? ''), /no longer assigned/i)

  const overlongLegacy = signLegacyByoaToken({ agentId: seeded.agentId, companyId: seeded.companyId, ttlSeconds: 2 * 60 * 60 + 1 })
  const overlongResult = await call('/runtime/inbox', { method: 'GET', token: overlongLegacy })
  assert.equal(overlongResult.status, 401)
  assert.match(String(overlongResult.body?.error ?? ''), /upgrade window/i)
})

// ── revoke/pair interleaving: revocation must win over in-flight pairing ─
//
// pairComputer looks a non-revoked row up and updates it by id afterwards;
// revokeComputer flips revoked_at AND rotates the assigned agents' versions.
// A revocation landing between a pairing's lookup and its write must win:
// pairing fails closed, the computer stays revoked, and the version rotation
// committed atomically with the revocation. Exercised deterministically via
// the registry's test interleave seam (production revokeComputer in the gap).

async function seedInterleaveFixture(): Promise<{
  companyId: string
  computerId: string
  agentId: string
  versionBefore: string
}> {
  const seeded = await seedByoaAgent()
  const { rows } = await pool.query<{ assignment_version: string }>(
    `SELECT assignment_version FROM participants WHERE id = $1`, [seeded.agentId])
  return {
    companyId: seeded.companyId,
    computerId: seeded.computerId,
    agentId: seeded.agentId,
    versionBefore: rows[0].assignment_version,
  }
}

async function assertComputerStaysRevokedAndRotated(args: {
  companyId: string
  computerId: string
  agentId: string
  versionBefore: string
}): Promise<void> {
  const { rows: comp } = await pool.query<{ revoked_at: string | null; credential_hash: string | null }>(
    `SELECT revoked_at, credential_hash FROM computers WHERE id = $1`, [args.computerId])
  assert.ok(comp[0].revoked_at !== null, 'the computer must REMAIN revoked — pairing cannot undo it')
  assert.equal(comp[0].credential_hash, null, 'no fresh device credential may be grafted onto the revoked row')
  const { rows: agent } = await pool.query<{ assignment_version: string }>(
    `SELECT assignment_version FROM participants WHERE id = $1`, [args.agentId])
  assert.notEqual(agent[0].assignment_version, args.versionBefore,
    'the version rotation belonging to the revocation must have committed (atomically with the revoke)')
}

test('[integration] runtime: hostname-path pairing interleaved with revocation fails closed', async () => {
  const registry = await import('../agents/computer/registry.js') as {
    __setPairComputerInterleaveForTesting?: (fn: ((computerId: string, companyId: string) => Promise<void>) | null) => void
  }
  assert.equal(typeof registry.__setPairComputerInterleaveForTesting, 'function',
    'registry must expose the deterministic revoke/pair interleave seam')
  const fx = await seedInterleaveFixture()
  try {
    registry.__setPairComputerInterleaveForTesting!(async (computerId, companyId) => {
      // The production revocation lands exactly between pairing's lookup
      // and its UPDATE.
      assert.equal(await revokeComputer({ computerId, companyId }), true)
    })
    // The hostname path runs off the COMPANY add token. Mint one.
    const { rows } = await pool.query<{ pair_token: string | null }>(
      `SELECT pair_token FROM companies WHERE id = $1`, [fx.companyId])
    let companyToken = rows[0].pair_token
    if (!companyToken) {
      await pool.query(`UPDATE companies SET pair_token = $1 WHERE id = $2`,
        [`tok-${randomUUID().slice(0, 12)}`, fx.companyId])
      const again = await pool.query<{ pair_token: string | null }>(
        `SELECT pair_token FROM companies WHERE id = $1`, [fx.companyId])
      companyToken = again.rows[0].pair_token
    }
    const result = await pairComputer({ code: companyToken!, hostName: 'Test Mac', engines: ['claude'] })
    assert.equal(result, null,
      'pairing whose lookup pre-dated the revocation must fail closed, not resurrect the row')
    await assertComputerStaysRevokedAndRotated(fx)
  } finally {
    registry.__setPairComputerInterleaveForTesting?.(null)
  }
})

test('[integration] runtime: reconnect-token pairing interleaved with revocation fails closed', async () => {
  const registry = await import('../agents/computer/registry.js') as {
    __setPairComputerInterleaveForTesting?: (fn: ((computerId: string, companyId: string) => Promise<void>) | null) => void
  }
  assert.equal(typeof registry.__setPairComputerInterleaveForTesting, 'function')
  const fx = await seedInterleaveFixture()
  // Give the computer its own persistent reconnect token.
  const reconnectToken = `reconnect-${randomUUID().slice(0, 12)}`
  await pool.query(`UPDATE computers SET pair_token = $1 WHERE id = $2`, [reconnectToken, fx.computerId])
  try {
    registry.__setPairComputerInterleaveForTesting!(async (computerId, companyId) => {
      assert.equal(await revokeComputer({ computerId, companyId }), true)
    })
    const result = await pairComputer({ code: reconnectToken, engines: ['claude'] })
    assert.equal(result, null,
      'reconnect pairing whose lookup pre-dated the revocation must fail closed')
    await assertComputerStaysRevokedAndRotated(fx)
  } finally {
    registry.__setPairComputerInterleaveForTesting?.(null)
  }
})

// ── wake streams: open streams revalidate before every event ─────────

async function openWakeStream(token: string): Promise<{
  reader: ReadableStreamDefaultReader<Uint8Array>
  /** Abort the underlying fetch — deterministically tears the SSE socket
   *  down even when the implementation under test never closes it. */
  abort: () => void
}> {
  const ctrl = new AbortController()
  const response = await fetch(`${baseUrl}/runtime/wake-stream`, {
    headers: { authorization: `Bearer ${token}`, accept: 'text/event-stream' },
    signal: ctrl.signal,
  })
  assert.equal(response.status, 200)
  assert.ok(response.body)
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let opening = ''
  while (!opening.includes('event: ready')) {
    const first = await reader.read()
    assert.equal(first.done, false)
    opening += decoder.decode(first.value, { stream: true })
  }
  return { reader, abort: () => ctrl.abort() }
}

async function readStreamToEnd(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  timeoutMs = 3_000,
): Promise<string> {
  const decoder = new TextDecoder()
  let body = ''
  let timer: ReturnType<typeof setTimeout> | undefined
  const closed = (async () => {
    for (;;) {
      const chunk = await reader.read()
      if (chunk.done) return body + decoder.decode()
      body += decoder.decode(chunk.value, { stream: true })
    }
  })()
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error('wake stream did not close')), timeoutMs)
  })
  try {
    return await Promise.race([closed, timeout])
  } catch (err) {
    await reader.cancel().catch(() => {})
    throw err
  } finally {
    if (timer) clearTimeout(timer)
  }
}

/** Read frames until the accumulated body matches `pattern` (or time out).
 *  Used for streams that stay open — the caller cancels the reader after. */
async function readStreamUntil(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  pattern: RegExp,
  timeoutMs = 3_000,
): Promise<string> {
  const decoder = new TextDecoder()
  let body = ''
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const remaining = deadline - Date.now()
    if (remaining <= 0) {
      await reader.cancel().catch(() => {})
      throw new Error(`wake stream did not deliver ${pattern} within ${timeoutMs}ms; body so far: ${body}`)
    }
    const chunk = await Promise.race([
      reader.read(),
      new Promise<never>((_r, rej) => setTimeout(() => rej(new Error('read stalled')), remaining)),
    ]).catch(async (err) => {
      await reader.cancel().catch(() => {})
      throw err
    })
    if (chunk.done) {
      throw new Error(`wake stream closed before ${pattern}; body: ${body}`)
    }
    body += decoder.decode(chunk.value, { stream: true })
    if (pattern.test(body)) return body
  }
}

test('[integration] runtime: open wake streams close before future events after assignment invalidation', async (t) => {
  const { deliver } = await import('../agents/runtime/wake-bus.js')
  const wake = { kind: 'wake' as const, reason: 'manual' as const }

  await t.test('reassignment', async () => {
    const seeded = await seedByoaAgent()
    const stream = await openWakeStream(seeded.token)
    try {
      const computerB = await seedComputer(seeded.companyId, 'local', ['claude'])
      await assignAgentToComputer({ agentId: seeded.agentId, companyId: seeded.companyId, computerId: computerB })
      assert.equal(await deliver(seeded.agentId, wake), 0,
        'no authorized stream may accept an event after reassignment — publisher falls back')
      assert.doesNotMatch(await readStreamToEnd(stream.reader), /event: wake/)
    } finally {
      stream.abort()
      await stream.reader.cancel().catch(() => {})
    }
  })

  await t.test('departure', async () => {
    const seeded = await seedByoaAgent()
    const stream = await openWakeStream(seeded.token)
    try {
      await pool.query(`UPDATE participants SET departed_at = NOW() WHERE id = $1`, [seeded.agentId])
      assert.equal(await deliver(seeded.agentId, wake), 0,
        'no authorized stream may accept an event after departure — publisher falls back')
      assert.doesNotMatch(await readStreamToEnd(stream.reader), /event: wake/)
    } finally {
      stream.abort()
      await stream.reader.cancel().catch(() => {})
    }
  })

  await t.test('computer revocation', async () => {
    const seeded = await seedByoaAgent()
    const stream = await openWakeStream(seeded.token)
    try {
      assert.equal(await revokeComputer({ computerId: seeded.computerId, companyId: seeded.companyId }), true)
      assert.equal(await deliver(seeded.agentId, wake), 0,
        'no authorized stream may accept an event after revocation — publisher falls back')
      assert.doesNotMatch(await readStreamToEnd(stream.reader), /event: wake/)
    } finally {
      stream.abort()
      await stream.reader.cancel().catch(() => {})
    }
  })

  await t.test('token expiry', async () => {
    const seeded = await seedByoaAgent()
    const token = signAgentToken({
      agentId: seeded.agentId,
      companyId: seeded.companyId,
      runtimeKind: 'byoa',
      computerId: seeded.computerId,
      assignmentVersion: seeded.assignmentVersion,
      ttlSeconds: 2,
    })
    const stream = await openWakeStream(token)
    try {
      assert.doesNotMatch(await readStreamToEnd(stream.reader, 6_000), /event: wake/,
        'stream must close at credential expiry without delivering future events')
    } finally {
      stream.abort()
      await stream.reader.cancel().catch(() => {})
    }
  })

  await t.test('healthy stream still accepts events (fallback is not taken spuriously)', async () => {
    const seeded = await seedByoaAgent()
    const stream = await openWakeStream(seeded.token)
    try {
      const accepted = await deliver(seeded.agentId, wake)
      assert.ok(accepted >= 1, 'an authorized, current stream must accept the event')
      const body = await readStreamUntil(stream.reader, /event: wake/)
      assert.match(body, /event: wake/)
    } finally {
      stream.abort()
      await stream.reader.cancel().catch(() => {})
    }
  })
})

// ── identity pin: agentId comes from JWT, not request body ────────────

test('[integration] runtime: /runs records the JWT subject, not whatever the body claims', async () => {
  const seeded = await seedAgent()
  const { agentId, companyId, token } = seeded
  // Try to spoof: body claims a different agentId. The endpoint shape
  // doesn't actually read agentId from body (the server takes it from
  // c.sub), but we send the spoof anyway to catch any regression that
  // adds a body.agentId read.
  const r = await call('/runtime/runs', {
    token,
    body: { trigger: { kind: 'test' }, inboxCount: 0, agentId: 'someone-else' },
  })
  assert.equal(r.status, 200)
  const runId = String(r.body?.runId ?? '')
  assert.ok(runId.length > 0, 'runId returned')
  const { rows } = await pool.query<{ agent_id: string; company_id: string }>(
    `SELECT agent_id, company_id FROM agent_runs WHERE id = $1`,
    [runId],
  )
  assert.equal(rows.length, 1)
  assert.equal(rows[0].agent_id, agentId, 'JWT subject pins agent_id')
  assert.equal(rows[0].company_id, companyId, 'JWT companyId pins company_id')
})

test('[integration] runtime: /events row uses JWT subject for agent_id', async () => {
  const seeded = await seedAgent()
  const { agentId, companyId, token } = seeded
  // First create a run to attach an event to (events.run_id FK → agent_runs.id).
  const runRes = await call('/runtime/runs', {
    token, body: { trigger: { kind: 'test' }, inboxCount: 0 },
  })
  const runId = String(runRes.body.runId)
  const r = await call('/runtime/events', {
    token,
    body: {
      runId,
      kind: 'test.event',
      title: 'pinning check',
      data: { hi: 1 },
      // Body claims someone else's id — must be ignored.
      agentId: 'someone-else',
      companyId: 'wrong-co',
    },
  })
  assert.equal(r.status, 200)
  const { rows } = await pool.query<{ agent_id: string; company_id: string; kind: string }>(
    `SELECT agent_id, company_id, kind FROM agent_events WHERE run_id = $1 AND kind = 'test.event'`,
    [runId],
  )
  assert.equal(rows.length, 1)
  assert.equal(rows[0].agent_id, agentId)
  assert.equal(rows[0].company_id, companyId)
})

// ── payload validation ────────────────────────────────────────────────

test('[integration] runtime: /events 400 when runId / kind / title is missing', async () => {
  const { token } = await seedAgent()
  const r = await call('/runtime/events', { token, body: { kind: 'x', title: 'y' } })
  assert.equal(r.status, 400)
  assert.match(String(r.body?.error ?? ''), /runId|kind|title/i)
})

test('[integration] runtime: /status 400 when status field missing', async () => {
  const { token } = await seedAgent()
  const r = await call('/runtime/status', { token, body: {} })
  assert.equal(r.status, 400)
})

test('[integration] runtime: /typing 400 when conversationId missing', async () => {
  const { token } = await seedAgent()
  const r = await call('/runtime/typing', { token, body: { done: false } })
  assert.equal(r.status, 400)
})

test('[integration] runtime: /runs/:runId/finish 400 when status missing', async () => {
  const { token } = await seedAgent()
  const r = await call('/runtime/runs/some-run/finish', { token, body: {} })
  assert.equal(r.status, 400)
})

test('[integration] runtime: /notices 400 when any of the required fields missing', async () => {
  const { token } = await seedAgent()
  const r = await call('/runtime/notices', { token, body: { conversationId: 'c-1' } })
  assert.equal(r.status, 400)
  assert.match(String(r.body?.error ?? ''), /required/i)
})

// ── happy-path smokes (round-trips through inproc-client into PG) ─────

test('[integration] runtime: /runs + /runs/:runId/finish persists status transition', async () => {
  const { agentId, token } = await seedAgent()
  const created = await call('/runtime/runs', { token, body: { trigger: { kind: 't' }, inboxCount: 0 } })
  assert.equal(created.status, 200)
  const runId = String(created.body.runId)

  const done = await call(`/runtime/runs/${runId}/finish`, {
    token, body: { status: 'completed', summary: 'ok', toolCallCount: 3, tokenCount: 1234 },
  })
  assert.equal(done.status, 200)

  const { rows } = await pool.query<{ status: string; summary: string; tool_call_count: number; token_count: number; agent_id: string }>(
    `SELECT status, summary, tool_call_count, token_count, agent_id FROM agent_runs WHERE id = $1`,
    [runId],
  )
  assert.equal(rows.length, 1)
  assert.equal(rows[0].status, 'completed')
  assert.equal(rows[0].summary, 'ok')
  assert.equal(rows[0].tool_call_count, 3)
  assert.equal(rows[0].token_count, 1234)
  assert.equal(rows[0].agent_id, agentId)
})

// ── minted claim shape ────────────────────────────────────────────────

test('[integration] runtime: minted tokens carry host + assignment version claims for both kinds', async () => {
  const managed = await seedAgent()
  const managedClaims = verifyAgentToken(managed.token)
  assert.equal(managedClaims.runtimeKind, 'managed')
  assert.equal(managedClaims.computerId, managed.computerId,
    'managed mint must pin the cloud computer')
  assert.equal(managedClaims.assignmentVersion, managed.assignmentVersion,
    'managed mint must pin the observed assignment version')

  const byoa = await seedByoaAgent()
  const byoaClaims = verifyAgentToken(byoa.token)
  assert.equal(byoaClaims.runtimeKind, 'byoa')
  assert.equal(byoaClaims.computerId, byoa.computerId,
    'BYOA mint must pin the paired computer')
  assert.equal(byoaClaims.assignmentVersion, byoa.assignmentVersion,
    'BYOA mint must pin the observed assignment version')
})
