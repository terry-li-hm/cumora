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

afterEach(() => {
  env.SERVER_LLM_DISABLED = savedDisabled
  __setLlmClientOverrideForTesting(null)
  __setEmbedTextOverrideForTesting(null)
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
  }))
  const currentByoa = verifyAgentToken(signAgentToken({
    agentId: 'reassigned-agent', companyId: 'personal', runtimeKind: 'byoa',
  }))

  assert.equal(runtimeAllowedWhenServerLlmDisabled(staleManaged), false)
  assert.equal(runtimeAllowedWhenServerLlmDisabled(currentByoa), true)
})

test('CUMORA_DISABLE_SERVER_LLM runtime middleware rejects stale managed tokens but admits BYOA', async () => {
  env.SERVER_LLM_DISABLED = true
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
    })
    const byoaToken = signAgentToken({
      agentId: 'reassigned-agent', companyId: 'personal', runtimeKind: 'byoa',
    })

    const managed = await fetch(url, { headers: { authorization: `Bearer ${managedToken}` } })
    assert.equal(managed.status, 503)
    assert.deepEqual(await managed.json(), { error: 'managed agent runtime is disabled' })

    const byoa = await fetch(url, { headers: { authorization: `Bearer ${byoaToken}` } })
    assert.equal(byoa.status, 200)
    assert.deepEqual(await byoa.json(), {
      agentId: 'reassigned-agent',
      runtimeKind: 'byoa',
    })
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
