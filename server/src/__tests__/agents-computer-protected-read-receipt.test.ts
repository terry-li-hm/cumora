import assert from 'node:assert/strict'
import { existsSync } from 'node:fs'
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, test } from 'node:test'
import { recordProtectedReadReceipt } from '../agents/computer/daemon.js'

const roots: string[] = []

afterEach(async () => {
  delete process.env.CUMORA_PROTECTED_READ_RECEIPTS
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })))
})

async function receiptPath(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'cumora-route-receipt-'))
  roots.push(root)
  const path = join(root, 'receipts.jsonl')
  process.env.CUMORA_PROTECTED_READ_RECEIPTS = path
  return path
}

test('approved native route records a non-secret mode-0600 receipt', async () => {
  const path = await receiptPath()
  await recordProtectedReadReceipt({
    agentId: 'grok',
    engine: 'grok',
    model: 'grok-4.6',
    purpose: 'cumora-chat-turn',
    conversationId: 'g-test',
  })
  const rows = (await readFile(path, 'utf8')).trim().split('\n').map((line) => JSON.parse(line))
  assert.equal(rows.length, 1)
  assert.equal(rows[0].providerRoute, 'grok')
  assert.equal(rows[0].accountSurface, "Terry's approved opted-out SuperGrok Heavy subscription")
  assert.equal(rows[0].purpose, 'cumora-chat-turn')
  assert.equal(rows[0].conversationId, 'g-test')
  assert.deepEqual(rows[0].exclusions, ['unrelated material', 'credentials', 'secrets', 'live corporate surfaces'])
  assert.equal((await stat(path)).mode & 0o777, 0o600)
  assert.equal(JSON.stringify(rows[0]).includes('prompt'), false)
})

test('pi xAI is not an approved route and writes no receipt', async () => {
  const path = await receiptPath()
  await recordProtectedReadReceipt({
    agentId: 'grok',
    engine: 'pi',
    model: 'xai/grok-4.6',
    purpose: 'cumora-chat-turn',
  })
  assert.equal(existsSync(path), false)
})

test('pi openai-codex records the approved ChatGPT account surface', async () => {
  const path = await receiptPath()
  await recordProtectedReadReceipt({
    agentId: 'sol',
    engine: 'pi',
    model: 'openai-codex/gpt-5.6-sol:high',
    purpose: 'cumora-agenda-turn',
  })
  const row = JSON.parse((await readFile(path, 'utf8')).trim())
  assert.equal(row.providerRoute, 'pi')
  assert.equal(row.accountSurface, "Terry's approved ChatGPT subscription")
})
