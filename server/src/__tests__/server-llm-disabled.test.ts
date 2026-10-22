import assert from 'node:assert/strict'
import type { Server } from 'node:http'
import { after, afterEach, test } from 'node:test'
import express from 'express'
import {
  __setEmbedTextOverrideForTesting,
  backfillMemoryEmbeddings,
  embedText,
} from '../agents/embeddings.js'
import { ensurePod } from '../agents/runtime/orchestrator.js'
import {
  runtimeAllowedWhenServerLlmDisabled,
  signAgentToken,
  verifyAgentToken,
} from '../agents/runtime/jwt.js'
import { runtimeRouter } from '../agents/runtime/server.js'
import { pool } from '../db/pool.js'
import { env } from '../env.js'
import { __setLlmClientOverrideForTesting, getLlmClient } from '../llm.js'

const savedDisabled = env.SERVER_LLM_DISABLED

afterEach(async () => {
  env.SERVER_LLM_DISABLED = savedDisabled
  __setLlmClientOverrideForTesting(null)
  __setEmbedTextOverrideForTesting(null)
  // Resolved dynamically + guarded so a baseline (pre-binding) server without
  // the override hook keeps its preserved tests green in red-tree runs — only
  // the tests that genuinely need assignment revalidation fail there, and no
  // ESM module-link error takes the file down before any test runs.
  const srv = await import('../agents/runtime/server.js') as {
    __setRuntimeAssignmentOverrideForTesting?: (fn: unknown) => void
  }
  srv.__setRuntimeAssignmentOverrideForTesting?.(null)
})

after(async () => {
  try { await pool.end() } catch { /* ignore */ }
  try {
    const { redis, sub } = await import('../redis.js')
    redis.disconnect()
    sub.disconnect()
  } catch { /* ignore */ }
})

test('CUMORA_DISABLE_SERVER_LLM blocks the shared client before provider resolution', async () => {
  let overrideCalls = 0
  __setLlmClientOverrideForTesting(async () => {
    overrideCalls++
    throw new Error('provider override must not run')
  })
  env.SERVER_LLM_DISABLED = true

  await assert.rejects(
    getLlmClient('personal'),
    /server-side LLM calls are disabled by CUMORA_DISABLE_SERVER_LLM/,
  )
  assert.equal(overrideCalls, 0)
})

test('CUMORA_DISABLE_SERVER_LLM identifies stale managed credentials after cloud-to-BYOA reassignment', () => {
  // Both credentials name the same agent. Current DB assignment therefore
  // cannot distinguish a stale cloud pod from the newly paired daemon; the
  // immutable mint-origin claim must.
  const staleManaged = verifyAgentToken(signAgentToken({
    agentId: 'reassigned-agent', companyId: 'personal', runtimeKind: 'managed',
    computerId: 'cloud-personal', assignmentVersion: 'av-1',
  }))
  const currentByoa = verifyAgentToken(signAgentToken({
    agentId: 'reassigned-agent', companyId: 'personal', runtimeKind: 'byoa',
    computerId: 'local-1', assignmentVersion: 'av-2',
  }))

  assert.equal(runtimeAllowedWhenServerLlmDisabled(staleManaged), false)
  assert.equal(runtimeAllowedWhenServerLlmDisabled(currentByoa), true)
})

test('runtime credentials are pinned to their current, non-revoked assignment version', async () => {
  // Structural parameter shapes of the production matcher, declared LOCALLY
  // (never `never`) so the literal assignments below type-check for real,
  // while the matcher itself stays dynamically resolved — a baseline jwt
  // module fails this test at the capability assertion instead of an ESM
  // link error, and no type-level reference to the new export exists that
  // would also fail to compile against baseline sources.
  interface MatcherClaimsShape {
    sub: string
    companyId: string | null
    runtimeKind: 'managed' | 'byoa'
    computerId?: string
    assignmentVersion?: string
    scope: 'agent-runner'
    iat: number
    exp: number
  }
  interface MatcherAssignmentShape {
    computerId: string | null
    computerKind: 'cloud' | 'local' | 'vps' | null
    engine: string | null
    assignmentVersion: string | null
    computerRevoked: boolean
  }
  const jwtMod = await import('../agents/runtime/jwt.js') as {
    runtimeCredentialMatchesAssignment?: (claims: MatcherClaimsShape, assignment: MatcherAssignmentShape | null) => boolean
  }
  const matcher = jwtMod.runtimeCredentialMatchesAssignment
  if (typeof matcher !== 'function') {
    assert.fail('assignment matcher must exist (capability check; end-to-end rejection is covered by the middleware tests)')
  }
  const managed = verifyAgentToken(signAgentToken({
    agentId: 'agent', companyId: 'personal', runtimeKind: 'managed',
    computerId: 'cloud-personal', assignmentVersion: 'av-1',
  }))
  const byoa = verifyAgentToken(signAgentToken({
    agentId: 'agent', companyId: 'personal', runtimeKind: 'byoa',
    computerId: 'local-1', assignmentVersion: 'av-1',
  }))

  assert.equal(matcher(managed, {
    computerId: 'cloud-personal', computerKind: 'cloud', engine: 'managed',
    assignmentVersion: 'av-1', computerRevoked: false,
  }), true)
  assert.equal(matcher(managed, {
    computerId: 'local-1', computerKind: 'local', engine: 'claude',
    assignmentVersion: 'av-1', computerRevoked: false,
  }), false, 'managed credential must not run on a BYOA host')
  assert.equal(matcher(byoa, {
    computerId: 'local-1', computerKind: 'local', engine: 'claude',
    assignmentVersion: 'av-1', computerRevoked: false,
  }), true)
  assert.equal(matcher(byoa, {
    computerId: 'local-2', computerKind: 'local', engine: 'claude',
    assignmentVersion: 'av-1', computerRevoked: false,
  }), false, 'host move must invalidate the credential')
  assert.equal(matcher(byoa, {
    computerId: 'local-1', computerKind: 'local', engine: 'claude',
    assignmentVersion: 'av-2', computerRevoked: false,
  }), false, 'assignment version rotation must invalidate the credential')
  assert.equal(matcher(byoa, {
    computerId: 'local-1', computerKind: 'local', engine: 'claude',
    assignmentVersion: 'av-1', computerRevoked: true,
  }), false, 'computer revocation must invalidate the credential')
  assert.equal(matcher(byoa, null), false)

  // A→B→A: returning to the original host must NOT revive the old
  // credential — the version rotated on each effective change.
  const returnedHome = {
    computerId: 'local-1', computerKind: 'local' as const, engine: 'claude',
    assignmentVersion: 'av-3', computerRevoked: false,
  }
  assert.equal(matcher(byoa, returnedHome), false,
    'old token minted on local-1 must stay dead after local-1→local-2→local-1')

  // Legacy unbound tokens (hand-minted without computerId/assignmentVersion)
  // cannot prove they survived no reassignment — fail closed.
  const legacyByoa = { ...byoa }
  delete legacyByoa.computerId
  delete legacyByoa.assignmentVersion
  assert.equal(matcher(legacyByoa, {
    computerId: 'local-1', computerKind: 'local', engine: 'claude',
    assignmentVersion: 'av-1', computerRevoked: false,
  }), false, 'legacy unbound BYOA credential must fail closed')
  const legacyManaged = { ...managed }
  delete legacyManaged.computerId
  delete legacyManaged.assignmentVersion
  assert.equal(matcher(legacyManaged, {
    computerId: 'cloud-personal', computerKind: 'cloud', engine: 'managed',
    assignmentVersion: 'av-1', computerRevoked: false,
  }), false, 'legacy unbound managed credential must fail closed')
})

test('runtime assignment lookup failure rejects authentication', async () => {
  env.SERVER_LLM_DISABLED = false
  const srv = await import('../agents/runtime/server.js') as {
    __setRuntimeAssignmentOverrideForTesting?: (fn: unknown) => void
  }
  if (typeof srv.__setRuntimeAssignmentOverrideForTesting === 'function') {
    // Candidate: force the live lookup itself to blow up — auth must fail
    // closed on a DB error, never fall back to admitting the credential.
    srv.__setRuntimeAssignmentOverrideForTesting(async () => {
      throw new Error('database unavailable')
    })
  }
  // Baseline (no override hook): the same request exercises the ORIGINAL
  // behavior through the identical interface — a signed credential for an
  // agent with no current assignment must still be rejected. Either way the
  // expectation is behavioral: 401, not a module/setup failure.
  const app = express()
  app.use('/runtime', runtimeRouter)
  app.get('/runtime/auth-probe', (_req, res) => res.json({ ok: true }))

  const server = await new Promise<Server>((resolve, reject) => {
    const listening = app.listen(0, '127.0.0.1', () => resolve(listening))
    listening.once('error', reject)
  })
  try {
    const address = server.address()
    assert.ok(address && typeof address !== 'string')
    const token = signAgentToken({
      agentId: 'agent', companyId: 'personal', runtimeKind: 'managed',
      computerId: 'cloud-personal', assignmentVersion: 'av-1',
    })
    const response = await fetch(`http://127.0.0.1:${address.port}/runtime/auth-probe`, {
      headers: { authorization: `Bearer ${token}` },
    })
    assert.equal(response.status, 401)
    assert.deepEqual(await response.json(), { error: 'runtime credential is no longer assigned' })
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((err) => err ? reject(err) : resolve())
    })
  }
})

test('CUMORA_DISABLE_SERVER_LLM middleware gates managed pods AND revalidates BYOA assignment', async () => {
  env.SERVER_LLM_DISABLED = true
  // No DB rows exist for this agent and no test override is installed: both
  // legs go through the middleware's REAL production path, so this test fails
  // behaviorally against the original (pre-binding) middleware — which
  // admitted ANY properly-signed credential — rather than via a missing
  // export or hook.
  const app = express()
  app.use('/runtime', runtimeRouter)
  // Sentinel sits after runtimeRouter. Reaching it with the pinned identity
  // proves the router's auth middleware admitted the token without invoking
  // any DB-backed handler.
  app.get('/runtime/auth-probe', (req, res) => {
    const claims = (req as unknown as { agent?: { sub: string; runtimeKind: string } }).agent
    res.json({ agentId: claims?.sub ?? null, runtimeKind: claims?.runtimeKind ?? null })
  })

  const server = await new Promise<Server>((resolve, reject) => {
    const listening = app.listen(0, '127.0.0.1', () => resolve(listening))
    listening.once('error', reject)
  })
  try {
    const address = server.address()
    assert.ok(address && typeof address !== 'string')
    const url = `http://127.0.0.1:${address.port}/runtime/auth-probe`
    const managedToken = signAgentToken({
      agentId: 'reassigned-agent', companyId: 'personal', runtimeKind: 'managed',
      computerId: 'cloud-personal', assignmentVersion: 'av-1',
    })
    const byoaToken = signAgentToken({
      agentId: 'reassigned-agent', companyId: 'personal', runtimeKind: 'byoa',
      computerId: 'local-1', assignmentVersion: 'av-2',
    })

    // Retained behavior: managed pods are refused while server-side LLM is
    // disabled, before any assignment lookup.
    const managed = await fetch(url, { headers: { authorization: `Bearer ${managedToken}` } })
    assert.equal(managed.status, 503)
    assert.deepEqual(await managed.json(), { error: 'managed agent runtime is disabled' })

    // New behavior: a BYOA credential with NO current assignment (the agent
    // was never on 'local-1') must fail closed — not be admitted merely
    // because its signature and runtimeKind are valid.
    const byoa = await fetch(url, { headers: { authorization: `Bearer ${byoaToken}` } })
    assert.equal(byoa.status, 401,
      'a BYOA credential with no current assignment must fail closed')
    assert.deepEqual(await byoa.json(), { error: 'runtime credential is no longer assigned' })
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((err) => err ? reject(err) : resolve())
    })
  }
})

test('CUMORA_DISABLE_SERVER_LLM blocks managed pod orchestration before provider-key resolution', async () => {
  env.SERVER_LLM_DISABLED = true

  assert.deepEqual(await ensurePod('managed-agent'), {
    created: false,
    ok: false,
    reason: 'managed agent pods are disabled by CUMORA_DISABLE_SERVER_LLM',
  })
})

test('CUMORA_DISABLE_SERVER_LLM skips memory embeddings without provider access', async () => {
  let overrideCalls = 0
  __setEmbedTextOverrideForTesting(async () => {
    overrideCalls++
    throw new Error('embedding override must not run')
  })
  env.SERVER_LLM_DISABLED = true

  assert.equal(await embedText('protected memory text'), null)
  await backfillMemoryEmbeddings()
  assert.equal(overrideCalls, 0)
})
