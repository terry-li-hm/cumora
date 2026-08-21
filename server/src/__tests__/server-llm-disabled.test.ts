import assert from 'node:assert/strict'
import { afterEach, test } from 'node:test'
import {
  __setEmbedTextOverrideForTesting,
  backfillMemoryEmbeddings,
  embedText,
} from '../agents/embeddings.js'
import { env } from '../env.js'
import { __setLlmClientOverrideForTesting, getLlmClient } from '../llm.js'

const savedDisabled = env.SERVER_LLM_DISABLED

afterEach(() => {
  env.SERVER_LLM_DISABLED = savedDisabled
  __setLlmClientOverrideForTesting(null)
  __setEmbedTextOverrideForTesting(null)
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
