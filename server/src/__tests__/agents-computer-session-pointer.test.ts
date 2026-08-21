import assert from 'node:assert/strict'
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, test } from 'node:test'
import { persistSessionPointer, readSessionPointer } from '../agents/computer/daemon.js'

const roots: string[] = []
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })))
})

function permissions(mode: number): number {
  return mode & 0o777
}

test('session pointers and their directory are private on first write', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cumora-session-pointer-'))
  roots.push(root)
  const sessionFile = join(root, 'sessions', 'agent.session')

  await persistSessionPointer(sessionFile, 'session-123')

  assert.equal(await readFile(sessionFile, 'utf8'), 'session-123')
  assert.equal(permissions((await stat(join(root, 'sessions'))).mode), 0o700)
  assert.equal(permissions((await stat(sessionFile)).mode), 0o600)
})

test('reading an existing pointer repairs permissive legacy modes', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cumora-session-pointer-'))
  roots.push(root)
  const sessionsDir = join(root, 'sessions')
  const sessionFile = join(sessionsDir, 'agent.session')
  await mkdir(sessionsDir, { mode: 0o755 })
  await writeFile(sessionFile, 'legacy-session', { mode: 0o644 })
  await chmod(sessionsDir, 0o755)
  await chmod(sessionFile, 0o644)

  assert.equal(await readSessionPointer(sessionFile), 'legacy-session')
  assert.equal(permissions((await stat(sessionsDir)).mode), 0o700)
  assert.equal(permissions((await stat(sessionFile)).mode), 0o600)
})

test('clearing a pointer removes the file but keeps a private store', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cumora-session-pointer-'))
  roots.push(root)
  const sessionFile = join(root, 'sessions', 'agent.session')
  await persistSessionPointer(sessionFile, 'session-123')

  await persistSessionPointer(sessionFile, null)

  assert.equal(await readSessionPointer(sessionFile), null)
  assert.equal(permissions((await stat(join(root, 'sessions'))).mode), 0o700)
})
