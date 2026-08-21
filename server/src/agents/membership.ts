/**
 * Shared membership-change plumbing.
 *
 * Both the agent-side CLI (`cumora invite / leave / kick`) and the
 * human-side HTTP endpoints (`POST /conversations/:id/members`, `POST
 * /conversations/:id/leave`) need to do the same two things on every
 * membership mutation:
 *
 *   1. Post a `kind='system'` message into the conversation describing
 *      what happened (joined / left / kicked), so the audit trail is
 *      visible to remaining members.
 *   2. Publish CH_MESSAGE_NEW so the mailbox scheduler wakes everyone
 *      who's a member at message-creation time — including the newly
 *      added member (for joins) or the departing one (for leaves /
 *      kicks, when the system message is posted BEFORE the members
 *      array update).
 *
 * Putting both in one file keeps the CLI and HTTP paths from drifting.
 */
import { randomUUID } from 'node:crypto'
import type { PoolClient } from 'pg'
import { pool } from '../db/pool.js'
import { CH_MESSAGE_NEW, publish } from '../redis.js'

type Queryable = Pick<PoolClient, 'query'>

/** Atomically claim the next sequence number for a conversation.
 *  Same UPSERT pattern as the human reply path and `cumora reply`. */
export async function nextConversationSequence(
  conversationId: string,
  db: Queryable = pool,
): Promise<number> {
  const { rows } = await db.query<{ seq: number }>(
    `INSERT INTO conversation_counters (conversation_id, next_sequence)
     VALUES ($1, 2)
     ON CONFLICT (conversation_id) DO UPDATE SET next_sequence = conversation_counters.next_sequence + 1
     RETURNING next_sequence - 1 AS seq`,
    [conversationId],
  )
  return rows[0]?.seq ?? 1
}

export type MembershipKind = 'joined' | 'left' | 'kicked'

interface MembershipMessageArgs {
  conversationId: string
  companyId: string | null
  actorId: string
  kind: MembershipKind
  participantId: string
}

export interface PersistedMembershipMessage {
  messageId: string
  sequence: number
  conversationId: string
  companyId: string | null
  actorId: string
  body: string
}

/** Persist the audit row using the caller's transaction when supplied.
 * Publishing is separate because Redis must only see rows after commit. */
export async function insertMembershipSystemMessage(
  args: MembershipMessageArgs,
  db: Queryable = pool,
): Promise<PersistedMembershipMessage> {
  const messageId = `m-${randomUUID()}`
  const sequence = await nextConversationSequence(args.conversationId, db)
  const body = JSON.stringify({
    kind: args.kind,
    participantId: args.participantId,
    actorId: args.actorId,
  })
  await db.query(
    `INSERT INTO messages (id, conversation_id, author_id, kind, body, sequence, company_id)
     VALUES ($1,$2,$3,'system',$4,$5,$6)`,
    [messageId, args.conversationId, args.actorId, body, sequence, args.companyId],
  )
  return {
    messageId, sequence, body,
    conversationId: args.conversationId,
    companyId: args.companyId,
    actorId: args.actorId,
  }
}

/** Broadcast an already-committed membership row. If Redis is unavailable the
 * row remains durable and ordinary polling/reload paths still discover it. */
export async function publishMembershipSystemMessage(
  message: PersistedMembershipMessage,
): Promise<void> {
  await publish(CH_MESSAGE_NEW, {
    type: 'message.new',
    conversationId: message.conversationId,
    companyId: message.companyId ?? undefined,
    message: {
      id: message.messageId,
      conversationId: message.conversationId,
      authorId: message.actorId,
      kind: 'system',
      body: message.body,
      sequence: message.sequence,
      at: new Date().toISOString(),
    },
  })
}

/** Insert a membership system row + broadcast it. Order of operations
 *  vs the actual `conversations.members` mutation matters:
 *    - For 'joined': call AFTER members has been updated. The new
 *      member is now in the array, so the scheduler wakes them on the
 *      CH_MESSAGE_NEW event and they perceive the join.
 *    - For 'left' / 'kicked': call BEFORE removing the departing
 *      member. The mailbox query filters by current members, so if we
 *      removed them first they'd never see the system row that explains
 *      why their inbox went quiet. */
export async function postMembershipSystemMessage(
  args: MembershipMessageArgs,
): Promise<{ messageId: string; sequence: number }> {
  const message = await insertMembershipSystemMessage(args)
  await publishMembershipSystemMessage(message)
  return { messageId: message.messageId, sequence: message.sequence }
}
