/**
 * Integration tests for conversation list/search shaping.
 *
 * Direct conversation rows are shared by both participants, so the stored
 * `conversations.title` can only ever be correct for one viewer. The API must
 * return a viewer-specific title based on the other member instead.
 */
import { test, before, beforeEach, after } from 'node:test'
import assert from 'node:assert/strict'
import { createServer, type Server } from 'node:http'
import {
  buildApiTestApp, ensureSchemaOnce, resetAllTables, seedUserMembership, teardownAll,
} from './_helpers.js'
import { pool } from '../db/pool.js'

const ME_USER_ID = 'u-me'
const OTHER_USER_ID = 'u-ada'
let server: Server
let baseUrl = ''

before(async () => {
  await ensureSchemaOnce()
  const app = await buildApiTestApp(ME_USER_ID)
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

async function seedHumanDirectWithSelfStoredTitle(): Promise<{ companyId: string; conversationId: string }> {
  const companyId = 'c-direct-title'
  const conversationId = 'direct-ada-yetone'
  await pool.query(
    `INSERT INTO companies (id, name, slug, owner_user_id)
     VALUES ($1, 'Direct Title Co', 'direct-title-co', $2)`,
    [companyId, ME_USER_ID],
  )
  await seedUserMembership(ME_USER_ID, companyId, {
    email: 'yetone@test.local',
    displayName: 'Yetone',
  })
  await seedUserMembership(OTHER_USER_ID, companyId, {
    email: 'ada@test.local',
    displayName: 'Ada',
  })
  await pool.query(
    `INSERT INTO conversations (id, kind, title, members, tag, company_id)
     VALUES ($1, 'direct', 'Yetone', $2::jsonb, 'human', $3)`,
    [conversationId, JSON.stringify([OTHER_USER_ID, ME_USER_ID]), companyId],
  )
  return { companyId, conversationId }
}

test('[integration] GET /conversations returns the other member as a direct title', async () => {
  const { companyId, conversationId } = await seedHumanDirectWithSelfStoredTitle()

  const res = await fetch(`${baseUrl}/api/conversations`, {
    headers: { 'x-company-id': companyId },
  })
  assert.equal(res.status, 200)
  const rows = await res.json() as Array<{ id: string; title: string }>
  const direct = rows.find((r) => r.id === conversationId)

  assert.equal(direct?.title, 'Ada')
})

test('[integration] GET /search uses the same perspective-specific direct title', async () => {
  const { companyId, conversationId } = await seedHumanDirectWithSelfStoredTitle()

  const res = await fetch(`${baseUrl}/api/search?q=${encodeURIComponent('Ada')}`, {
    headers: { 'x-company-id': companyId },
  })
  assert.equal(res.status, 200)
  const body = await res.json() as { rooms: Array<{ id: string; title: string }> }
  const direct = body.rooms.find((r) => r.id === conversationId)

  assert.equal(direct?.title, 'Ada')
})

test('[integration] archive hides a conversation from the normal list and search, then restores it', async () => {
  const { companyId, conversationId } = await seedHumanDirectWithSelfStoredTitle()
  const headers = { 'x-company-id': companyId, 'content-type': 'application/json' }

  const archived = await fetch(`${baseUrl}/api/conversations/${conversationId}/archive`, {
    method: 'POST', headers, body: JSON.stringify({ archive: true }),
  })
  assert.equal(archived.status, 200)
  assert.deepEqual(await archived.json(), { ok: true, archived: true })

  const activeList = await fetch(`${baseUrl}/api/conversations`, { headers })
  assert.equal(activeList.status, 200)
  assert.equal((await activeList.json() as Array<{ id: string }>).some((c) => c.id === conversationId), false)

  const allList = await fetch(`${baseUrl}/api/conversations?includeArchived=true`, { headers })
  assert.equal(allList.status, 200)
  const allRows = await allList.json() as Array<{ id: string; archivedAt: string | null }>
  assert.ok(allRows.find((c) => c.id === conversationId)?.archivedAt)

  const search = await fetch(`${baseUrl}/api/search?q=${encodeURIComponent('Ada')}`, { headers })
  assert.equal(search.status, 200)
  const searchBody = await search.json() as { rooms: Array<{ id: string }> }
  assert.equal(searchBody.rooms.some((c) => c.id === conversationId), false)

  const restored = await fetch(`${baseUrl}/api/conversations/${conversationId}/archive`, {
    method: 'POST', headers, body: JSON.stringify({ archive: false }),
  })
  assert.equal(restored.status, 200)
  assert.deepEqual(await restored.json(), { ok: true, archived: false })

  const visibleAgain = await fetch(`${baseUrl}/api/conversations`, { headers })
  assert.equal((await visibleAgain.json() as Array<{ id: string }>).some((c) => c.id === conversationId), true)
})

test('[integration] a non-member cannot archive someone else’s conversation', async () => {
  const { companyId } = await seedHumanDirectWithSelfStoredTitle()
  const privateId = 'private-without-me'
  await pool.query(
    `INSERT INTO conversations (id, kind, title, members, company_id)
     VALUES ($1, 'direct', 'Private', $2::jsonb, $3)`,
    [privateId, JSON.stringify([OTHER_USER_ID]), companyId],
  )

  const res = await fetch(`${baseUrl}/api/conversations/${privateId}/archive`, {
    method: 'POST',
    headers: { 'x-company-id': companyId, 'content-type': 'application/json' },
    body: JSON.stringify({ archive: true }),
  })
  assert.equal(res.status, 404)
  const { rows } = await pool.query<{ n: number }>(
    `SELECT COUNT(*)::int AS n FROM conversation_archives WHERE conversation_id = $1`,
    [privateId],
  )
  assert.equal(rows[0]?.n, 0)
})

test('[integration] explicitly opening an archived direct chat restores it', async () => {
  const { companyId, conversationId } = await seedHumanDirectWithSelfStoredTitle()
  const headers = { 'x-company-id': companyId, 'content-type': 'application/json' }
  await fetch(`${baseUrl}/api/conversations/${conversationId}/archive`, {
    method: 'POST', headers, body: JSON.stringify({ archive: true }),
  })

  const opened = await fetch(`${baseUrl}/api/conversations/direct`, {
    method: 'POST', headers, body: JSON.stringify({ otherId: OTHER_USER_ID }),
  })
  assert.equal(opened.status, 200)
  assert.deepEqual(await opened.json(), { id: conversationId, created: false })

  const visible = await fetch(`${baseUrl}/api/conversations`, { headers })
  assert.equal((await visible.json() as Array<{ id: string }>).some((c) => c.id === conversationId), true)
})

test('[integration] concurrent direct opens resolve to one conversation', async () => {
  const companyId = 'c-direct-race'
  await pool.query(
    `INSERT INTO companies (id, name, slug, owner_user_id)
     VALUES ($1, 'Direct Race Co', 'direct-race-co', $2)`,
    [companyId, ME_USER_ID],
  )
  await seedUserMembership(ME_USER_ID, companyId, {
    email: 'yetone@test.local', displayName: 'Yetone',
  })
  await seedUserMembership(OTHER_USER_ID, companyId, {
    email: 'ada@test.local', displayName: 'Ada',
  })
  const request = () => fetch(`${baseUrl}/api/conversations/direct`, {
    method: 'POST',
    headers: { 'x-company-id': companyId, 'content-type': 'application/json' },
    body: JSON.stringify({ otherId: OTHER_USER_ID }),
  })
  const [a, b] = await Promise.all([request(), request()])
  assert.ok([200, 201].includes(a.status))
  assert.ok([200, 201].includes(b.status))
  const [aBody, bBody] = await Promise.all([
    a.json() as Promise<{ id: string }>, b.json() as Promise<{ id: string }>,
  ])
  assert.equal(aBody.id, bBody.id)
  const count = await pool.query<{ n: number }>(
    `SELECT COUNT(*)::int AS n FROM conversations WHERE company_id = $1 AND kind = 'direct'`,
    [companyId],
  )
  assert.equal(count.rows[0]?.n, 1)
})

test('[integration] adding a group member commits its membership audit row atomically', async () => {
  const companyId = 'c-add-member'
  const agentId = 'active-agent'
  await pool.query(
    `INSERT INTO companies (id, name, slug, owner_user_id)
     VALUES ($1, 'Add Member Co', 'add-member-co', $2)`,
    [companyId, ME_USER_ID],
  )
  await seedUserMembership(ME_USER_ID, companyId, {
    email: 'yetone@test.local', displayName: 'Yetone',
  })
  await pool.query(
    `INSERT INTO participants
       (id, kind, name, initial, avatar_bg, status, company_id)
     VALUES ($1, 'agent', 'Active Agent', 'A', '#000000', 'avail', $2)`,
    [agentId, companyId],
  )
  await pool.query(
    `INSERT INTO conversations (id, kind, title, members, company_id)
     VALUES ('g-add-member', 'group', 'Work room', $1::jsonb, $2)`,
    [JSON.stringify([ME_USER_ID]), companyId],
  )

  const res = await fetch(`${baseUrl}/api/conversations/g-add-member/members`, {
    method: 'POST',
    headers: { 'x-company-id': companyId, 'content-type': 'application/json' },
    body: JSON.stringify({ id: agentId }),
  })
  assert.equal(res.status, 200)
  const { rows } = await pool.query<{ members: string[]; notices: number }>(
    `SELECT c.members,
            (SELECT COUNT(*)::int FROM messages m
              WHERE m.conversation_id = c.id AND m.kind = 'system') AS notices
       FROM conversations c WHERE c.id = 'g-add-member'`,
  )
  assert.deepEqual(rows[0]?.members, [ME_USER_ID, agentId])
  assert.equal(rows[0]?.notices, 1)
})

test('[integration] leaving a group commits one audit row and is retry-safe', async () => {
  const companyId = 'c-leave-group'
  await pool.query(
    `INSERT INTO companies (id, name, slug, owner_user_id)
     VALUES ($1, 'Leave Co', 'leave-co', $2)`,
    [companyId, ME_USER_ID],
  )
  await seedUserMembership(ME_USER_ID, companyId, {
    email: 'yetone@test.local', displayName: 'Yetone',
  })
  await seedUserMembership(OTHER_USER_ID, companyId, {
    email: 'ada@test.local', displayName: 'Ada',
  })
  await pool.query(
    `INSERT INTO conversations (id, kind, title, members, company_id)
     VALUES ('g-leave', 'group', 'Leave room', $1::jsonb, $2)`,
    [JSON.stringify([ME_USER_ID, OTHER_USER_ID]), companyId],
  )
  const leave = () => fetch(`${baseUrl}/api/conversations/g-leave/leave`, {
    method: 'POST', headers: { 'x-company-id': companyId },
  })
  const first = await leave()
  assert.equal(first.status, 200)
  const second = await leave()
  assert.equal(second.status, 409)
  const state = await pool.query<{ members: string[]; notices: number }>(
    `SELECT c.members,
            (SELECT COUNT(*)::int FROM messages m
              WHERE m.conversation_id = c.id AND m.kind = 'system') AS notices
       FROM conversations c WHERE c.id = 'g-leave'`,
  )
  assert.deepEqual(state.rows[0]?.members, [OTHER_USER_ID])
  assert.equal(state.rows[0]?.notices, 1)
})

test('[integration] off-boarding removes an agent from groups but preserves direct-chat identity', async () => {
  const companyId = 'c-offboard-membership'
  const agentId = 'legacy-lens'
  await pool.query(
    `INSERT INTO companies (id, name, slug, owner_user_id)
     VALUES ($1, 'Offboard Co', 'offboard-co', $2)`,
    [companyId, ME_USER_ID],
  )
  await seedUserMembership(ME_USER_ID, companyId, {
    email: 'yetone@test.local', displayName: 'Yetone',
  })
  await pool.query(
    `INSERT INTO participants
       (id, kind, name, initial, avatar_bg, status, company_id)
     VALUES ($1, 'agent', 'Legacy Lens', 'L', '#000000', 'avail', $2)`,
    [agentId, companyId],
  )
  await pool.query(
    `INSERT INTO conversations (id, kind, title, members, company_id)
     VALUES
       ('g-offboard', 'group', 'Work room', $1::jsonb, $3),
       ('d-offboard', 'direct', 'Legacy Lens', $2::jsonb, $3)`,
    [JSON.stringify([ME_USER_ID, agentId]), JSON.stringify([ME_USER_ID, agentId]), companyId],
  )

  const res = await fetch(`${baseUrl}/api/agents/${agentId}`, {
    method: 'DELETE', headers: { 'x-company-id': companyId },
  })
  assert.equal(res.status, 200)

  const { rows } = await pool.query<{ id: string; members: string[] }>(
    `SELECT id, members FROM conversations WHERE id IN ('g-offboard', 'd-offboard') ORDER BY id`,
  )
  assert.deepEqual(rows.find((r) => r.id === 'g-offboard')?.members, [ME_USER_ID])
  assert.deepEqual(rows.find((r) => r.id === 'd-offboard')?.members, [ME_USER_ID, agentId])
  const archivedDirect = await pool.query<{ n: number }>(
    `SELECT COUNT(*)::int AS n FROM conversation_archives
      WHERE user_id = $1 AND conversation_id = 'd-offboard'`,
    [ME_USER_ID],
  )
  assert.equal(archivedDirect.rows[0]?.n, 1)

  const addDeparted = await fetch(`${baseUrl}/api/conversations/g-offboard/members`, {
    method: 'POST',
    headers: { 'x-company-id': companyId, 'content-type': 'application/json' },
    body: JSON.stringify({ id: agentId }),
  })
  assert.equal(addDeparted.status, 400)
  const createWithDeparted = await fetch(`${baseUrl}/api/conversations`, {
    method: 'POST',
    headers: { 'x-company-id': companyId, 'content-type': 'application/json' },
    body: JSON.stringify({ title: 'Should fail', members: [agentId] }),
  })
  assert.equal(createWithDeparted.status, 400)
})
