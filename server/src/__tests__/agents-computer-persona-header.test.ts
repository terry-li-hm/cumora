import assert from 'node:assert/strict'
import { test } from 'node:test'
import { renderPersonaHeader } from '../agents/computer/engine.js'

const generalist = 'No fixed persona. Follow the room brief temporary lens and output contract.'
const approved = renderPersonaHeader(
  { id: 'fable', name: 'Fable', role: 'General agent', systemPrompt: generalist },
  { chromatinAccess: true },
)
const restricted = renderPersonaHeader(
  { id: 'glm', name: 'GLM', role: 'General agent', systemPrompt: `${generalist} Public and synthetic work only.` },
  { personaFile: 'AGENTS.md', skillsDir: '.pi/skills/', chromatinAccess: false },
)

test('persona header preserves workspace, memory, and disclosure boundaries', () => {
  for (const required of [
    'temporary role and output contract',
    'memory/MEMORY.md',
    'only durable memory',
    'workspace/',
    'Everything outside this home is private and out of scope',
    '~/.ssh',
    'credentials',
    'browser data',
    'cumora <command> --help',
  ]) {
    assert.ok(approved.includes(required), `missing persona-header contract: ${required}`)
  }
})

test('eligible and restricted agents receive different Chromatin surfaces', () => {
  for (const required of [
    'chromatin/',
    'task-bound, read-only',
    'current human-directed task',
    'Never edit it or retain source files',
    'live corporate systems',
  ]) {
    assert.ok(approved.includes(required), `missing Chromatin contract: ${required}`)
  }
  assert.ok(!restricted.includes('chromatin/'))
  assert.ok(restricted.includes('Public and synthetic work only.'))
})

test('persona header is concise enough to keep native instruction priority', () => {
  assert.ok(Buffer.byteLength(approved) < 2_500)
  assert.ok(Buffer.byteLength(restricted) < 2_000)
})
