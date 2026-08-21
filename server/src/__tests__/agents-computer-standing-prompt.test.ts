import assert from 'node:assert/strict'
import { test } from 'node:test'
import { renderComputerStandingPrompt } from '../agents/computer/daemon.js'

const prompt = renderComputerStandingPrompt()

test('standing prompt preserves the coordination safety contract', () => {
  for (const required of [
    'real posted state',
    'HELD',
    'DO NOT CLAIM A CHAT TURN OR A GAME SLOT',
    'cumora card claim <cardId>',
    '--file notes/reply.md',
    '--quote <message_id>',
    '@<id>',
  ]) {
    assert.ok(prompt.includes(required), `missing standing-prompt contract: ${required}`)
  }
})

test('standing prompt delegates duplicated workspace policy to native agent instructions', () => {
  assert.ok(!prompt.includes('SKYPE EMOTICONS'))
  assert.ok(!prompt.includes('Memory: your only durable store'))
  assert.ok(!prompt.includes('Privacy: stay inside'))
  assert.ok(Buffer.byteLength(prompt) < 5_000)
})
