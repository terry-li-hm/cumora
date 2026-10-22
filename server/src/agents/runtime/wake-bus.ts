/**
 * Wake-bus — server-side broker that routes wake events to the right
 * agent's long-running pod over SSE.
 *
 *   scheduler hears  CH_MESSAGE_NEW (Redis pubsub)
 *      ↓
 *   bus.deliver(agentId, event)              [publishes to cumora:wake:<agentId>]
 *      ↓
 *   server holding that agent's SSE          [subscribed to the channel]
 *      ↓
 *   each subscribed SSE response gets the event written as
 *      `event: wake\n`
 *      `data: {...}\n\n`
 *
 * Multi-instance: Redis pubsub. We subscribe to `cumora:wake:<agentId>`
 * the moment we get our first local SSE attach for that agent, and
 * unsubscribe when the last local SSE drops. `deliver` waits for each
 * receiving server to report how many streams ACCEPTED the event after
 * revalidation. If none did, the orchestrator can spin up a pod.
 *
 * Per-agent channel subscriptions are serialized through an explicit
 * transition promise: an attach that races the final unsubscribe of the
 * same agent's channel WAITS for that unsubscribe to settle and then
 * re-subscribes before signalling `ready` — a subscriber must never be
 * told the stream is live while its Redis subscription is being torn
 * down (the historical race silently dropped every wake for a pod that
 * reconnected at exactly the wrong moment).
 *
 * Every delivery revalidates the subscriber's runtime credential
 * (company, assignment version, computer revocation, departure, token
 * expiry) immediately before the write; a stream that fails the check
 * is closed and reports non-acceptance so the publisher can fall back.
 *
 * Multiple subscribers per agent on the same server are allowed (the
 * agent's Pod may reconnect briefly during a rolling restart). Events
 * fan out to all live local subscribers; the Pod deduplicates by
 * event id (each event carries a fresh uuid).
 *
 * Single-instance deploys: this still works — the pub/sub round-trip
 * is local to the one Redis instance, so latency is sub-millisecond.
 */
import { randomUUID } from 'node:crypto'
import type { Response } from 'express'
import { redis, sub as redisSub } from '../../redis.js'

/** Discriminated union of all event types the bus carries.
 *
 *  `wake`  — historical: "you have new work; check your inbox." Pod
 *            triggers a turn (or schedules one after the current
 *            turn ends).
 *  `steer` — new (Phase 2 of the steering feature): "while you were
 *            in the middle of a turn, this specific message
 *            arrived; inject it as a user item at the next hop
 *            boundary." Payload carries the body + author so the
 *            pod's drain path can render the steer item without a
 *            DB round-trip. The message is ALSO in the messages
 *            table — that's the fallback path when the pod is
 *            idle / disconnected. */
export type WakeEvent = WakeEventBase & (WakeKindWake | WakeKindSteer)
type WakeEventPayload =
  | Omit<WakeEventBase & WakeKindWake, 'id' | 'at'>
  | Omit<WakeEventBase & WakeKindSteer, 'id' | 'at'>

interface WakeEventBase {
  /** Stable id so the Pod can dedupe across reconnect overlap. */
  id: string
  /** When the event was minted (server clock, ms since epoch). */
  at: number
}

/** Snapshot of a poll's state delivered alongside a `poll.updated` wake so
 *  the agent author can react in real time (watch, nudge, summarize)
 *  without first calling `cumora poll show`. Sized small — the agent can
 *  still fetch fuller voter details from the CLI when it cares. */
export interface PollWakeBrief {
  messageId: string
  conversationId: string
  question: string
  mode: 'single' | 'multi'
  status: 'open' | 'closed'
  /** Only meaningful when status === 'closed'. */
  closedReason: 'expired' | 'manual' | null
  expiresAt: string | null
  totalVotes: number
  /** Per-option count + voter display names. Texts are echoed from the
   *  poll payload so the agent can phrase nudges without
   *  cross-referencing option ids. */
  tallies: Array<{
    optionId: string
    text: string
    count: number
    voters: Array<{ id: string; name: string }>
  }>
  /** Conversation members who could vote but haven't yet. Excludes the
   *  author themselves. */
  pending: Array<{ id: string; name: string }>
  /** Who triggered this update. null when the server-side expiration
   *  sweeper closed the poll. */
  actor: { id: string | null; name: string | null }
  /** Discriminator the turn renderer uses to pick prompt language —
   *  'vote' for ongoing tallies, 'close' for terminal summary. */
  phase: 'vote' | 'close'
}

interface WakeKindWake {
  kind: 'wake'
  /** Why this agent was woken — for observability + debugging. */
  reason: 'message.new' | 'idle' | 'manual' | 'background_scan' | 'poll.updated'
  /** Optional conversation that triggered the wake. */
  conversationId?: string | null
  /** Short scheduler note rendered only for idle synthetic wakes. */
  idleReason?: string
  /** Server-side support-model triage note for ordinary message wakes. */
  triageNote?: string
  /** Internal background brief rendered as a normal model input. */
  backgroundBrief?: {
    title: string
    body: string
    source?: string
  }
  /** Set when reason === 'poll.updated'. Carries the current poll
   *  state so the turn renderer can paint a wake context without a
   *  separate DB round-trip. */
  pollBrief?: PollWakeBrief
}

interface WakeKindSteer {
  kind: 'steer'
  /** The conversation the steered message lives in. */
  conversationId: string
  /** messages.id — used by the pod for dedup against the queue and
   *  by the turn loop to advance conversation_reads on consume. */
  messageId: string
  /** Display name of the author. Embedded into the prompt prefix the
   *  model sees ("@alice: ..."), so DB-side ground truth is fine. */
  authorName: string
  /** The raw message body the model needs to see. */
  body: string
}

/** Per-agent Redis channel. Keep the prefix short — every PUBLISH
 *  command carries this on the wire. */
const CH_WAKE_PREFIX = 'cumora:wake:'

/** Auth material every subscriber must supply at attach time. Derived from
 *  the runtime JWT that opened the stream — the same revalidation the
 *  HTTP middleware applies per request, applied here before EVERY event. */
export interface WakeStreamAuth {
  /** Absolute expiry of the subscriber's credential (JWT exp, ms). */
  expiresAtMs: number
  /** Re-check company / assignment version / computer revocation /
   *  departure / token freshness. Resolving false (or throwing) makes
   *  the stream ineligible for the event and closes it. */
  revalidate: () => boolean | Promise<boolean>
}

interface Subscriber {
  agentId: string
  res: Response
  /** Set when the response closes — used to drop from the bus. */
  closed: boolean
  /** Events are eligible only after Redis setup and the ready frame complete. */
  ready: boolean
  /** Re-check token lifetime and current assignment before every event. */
  revalidate: () => boolean | Promise<boolean>
  /** Credential expiry (ms). Re-checked at the event-write boundary: the
   *  async revalidate await can straddle expiry, so the pre-await check in
   *  the closure alone is not sufficient. */
  expiresAtMs: number
  /** Explicit bounded FIFO. Promise chains retain every payload they capture. */
  deliveryQueue: QueuedDelivery[]
  pendingDeliveries: number
  processingDelivery: boolean
  close: () => void
}

interface QueuedDelivery {
  event: WakeEvent
  resolve: (accepted: boolean) => void
}

/** Per-agent channel state. Exactly one transition (subscribe or the final
 *  unsubscribe) may be in flight at a time; attaches and teardowns await
 *  the current transition before deciding what to do, so a reconnect can
 *  never observe a half-torn-down channel. */
interface AgentChannelState {
  agentId: string
  subscribers: Set<Subscriber>
  active: boolean
  transition: Promise<void> | null
}

const channels = new Map<string, AgentChannelState>()

// Cap how much unsent SSE data we let queue per subscriber before we give up on
// it. A daemon that can't drain makes the HTTP response buffer in process memory;
// under a wake flood that grows unbounded → OOM. A backed-up subscriber gets
// dropped (it reconnects and re-drains the inbox, which is durable).
const SSE_MAX_BUFFERED_BYTES = 1 * 1024 * 1024 // 1 MB

/** Includes the event currently being revalidated. The next event closes the
 * stream instead of retaining another caller-controlled payload. */
export const WAKE_REVALIDATION_QUEUE_MAX_EVENTS = 64

function writeEvent(s: Subscriber, evt: WakeEvent): boolean {
  if (s.closed) return false
  // Backpressure guard (OOM fix): if the socket's write queue is backed up the
  // client isn't keeping up — end the stream to reclaim the buffered memory
  // instead of piling more onto it. The wake is durable in the inbox, so the
  // daemon catches up on reconnect.
  const sock = s.res.socket
  if (sock && sock.writableLength > SSE_MAX_BUFFERED_BYTES) {
    s.close()
    return false
  }
  try {
    s.res.write(`event: ${evt.kind}\n`)
    s.res.write(`id: ${evt.id}\n`)
    s.res.write(`data: ${JSON.stringify(evt)}\n\n`)
    return true
  } catch {
    // Write failed (client gone, broken pipe) — mark closed; the
    // 'close' handler installed in attachWakeStream will scrub.
    s.close()
    return false
  }
}

function failQueuedDeliveries(s: Subscriber): void {
  for (const queued of s.deliveryQueue.splice(0)) {
    s.pendingDeliveries -= 1
    queued.resolve(false)
  }
}

function processDeliveryQueue(s: Subscriber): void {
  if (s.processingDelivery || s.closed) return
  const queued = s.deliveryQueue.shift()
  if (!queued) return
  s.processingDelivery = true
  void (async () => {
    let accepted = false
    try {
      let current = false
      try { current = await s.revalidate() } catch { current = false }
      // Re-check expiry AFTER the (potentially slow, DB-backed) await and
      // right at the write boundary: a credential that expired while the
      // revalidation lookup was in flight must not have its event written.
      if (current && Date.now() >= s.expiresAtMs) current = false
      if (!s.closed && current) accepted = writeEvent(s, queued.event)
      else if (!current) s.close()
    } finally {
      s.pendingDeliveries -= 1
      s.processingDelivery = false
      queued.resolve(accepted)
      if (s.closed) failQueuedDeliveries(s)
      else processDeliveryQueue(s)
    }
  })().catch(() => s.close())
}

function deliverTo(s: Subscriber, evt: WakeEvent): Promise<boolean> {
  if (s.closed || !s.ready) return Promise.resolve(false)
  if (s.pendingDeliveries >= WAKE_REVALIDATION_QUEUE_MAX_EVENTS) {
    s.close()
    return Promise.resolve(false)
  }
  s.pendingDeliveries += 1
  return new Promise<boolean>((resolve) => {
    s.deliveryQueue.push({ event: evt, resolve })
    processDeliveryQueue(s)
  })
}

// ─── Redis subscriber: fan out cluster-wide wake events to local SSE ─

let redisSubscriberInstalled = false
const CH_WAKE_ACK = 'cumora:wake-ack:v1'
const brokerId = randomUUID()
const DELIVERY_ACK_TIMEOUT_MS = 2_000

interface PendingDeliveryAck {
  expected: number | null
  accepted: number
  responders: Set<string>
  timer: ReturnType<typeof setTimeout>
  resolve: (accepted: number) => void
}

const pendingDeliveryAcks = new Map<string, PendingDeliveryAck>()
let ackSubscriptionActive = false
let ackSubscriptionPromise: Promise<void> | null = null
let wakeSubscribeOverride: ((channel: string) => Promise<void>) | null = null
let wakeUnsubscribeOverride: ((channel: string) => Promise<void>) | null = null

/** Deterministic Redis failure injection for the attach concurrency tests. */
export function __setWakeSubscribeOverrideForTesting(
  override: ((channel: string) => Promise<void>) | null,
): void {
  wakeSubscribeOverride = override
}

/** Deterministic injection point for the final-unsubscribe/attach race test:
 *  lets the test hold the last unsubscribe in flight while a new attach
 *  arrives, proving the attach waits for the transition to settle. */
export function __setWakeUnsubscribeOverrideForTesting(
  override: ((channel: string) => Promise<void>) | null,
): void {
  wakeUnsubscribeOverride = override
}

function settlePendingAck(eventId: string, pending: PendingDeliveryAck): void {
  if (pending.expected === null || pending.responders.size < pending.expected) return
  clearTimeout(pending.timer)
  pendingDeliveryAcks.delete(eventId)
  pending.resolve(pending.accepted)
}

function ensureAckSubscription(): Promise<void> {
  if (ackSubscriptionActive) return Promise.resolve()
  if (ackSubscriptionPromise) return ackSubscriptionPromise
  ackSubscriptionPromise = redisSub.subscribe(CH_WAKE_ACK).then(() => {
    ackSubscriptionActive = true
  }).catch((err) => {
    ackSubscriptionActive = false
    throw err
  }).finally(() => {
    ackSubscriptionPromise = null
  })
  return ackSubscriptionPromise
}

async function ensureAgentChannelActive(state: AgentChannelState): Promise<void> {
  // Serialize against ANY in-flight transition first — including the FINAL
  // UNSUBSCRIBE started by the previous last subscriber's cleanup. Returning
  // early just because `state.active` still reads true while that unsubscribe
  // is pending would let this attach write its `ready` frame onto a channel
  // that is about to lose its Redis subscription: the stream would then
  // silently miss every subsequent event. Await the transition, re-check,
  // and only then treat the channel as live.
  while (state.transition) {
    try { await state.transition } catch { /* failed transition left active untouched */ }
  }
  while (!state.active) {
    if (state.transition) {
      try { await state.transition } catch { /* failed transition left active untouched */ }
      continue
    }
    const transition = (async () => {
      await ensureAckSubscription()
      const channel = CH_WAKE_PREFIX + state.agentId
      if (wakeSubscribeOverride) await wakeSubscribeOverride(channel)
      else await redisSub.subscribe(channel).then(() => {})
      state.active = true
    })()
    state.transition = transition
    try {
      await transition
    } finally {
      if (state.transition === transition) state.transition = null
    }
  }
}

async function deactivateAgentChannelIfIdle(state: AgentChannelState): Promise<void> {
  while (state.subscribers.size === 0) {
    if (state.transition) {
      try { await state.transition } catch { /* setup already failed closed */ }
      continue
    }
    if (!state.active) {
      if (channels.get(state.agentId) === state) channels.delete(state.agentId)
      return
    }
    const channel = CH_WAKE_PREFIX + state.agentId
    const teardown = wakeUnsubscribeOverride
      ? wakeUnsubscribeOverride(channel)
      : redisSub.unsubscribe(channel).then(() => {})
    const transition = teardown.then(() => {
      state.active = false
    })
    state.transition = transition
    try {
      await transition
    } catch (err) {
      state.active = false
      // A new subscriber may have joined while the (failed) teardown was in
      // flight — its attach is parked in ensureAgentChannelActive awaiting
      // this transition. Deleting the channel state unconditionally would
      // orphan that attach: it would re-subscribe and signal ready on a state
      // the message handler can no longer find, so every later event would
      // ack zero while the pod believes its stream is live. Only drop the
      // state when it is still idle; otherwise keep it (active=false) so the
      // parked attach re-subscribes the SAME state deterministically.
      if (state.subscribers.size === 0 && channels.get(state.agentId) === state) {
        channels.delete(state.agentId)
      }
      console.warn(`[wake-bus] redis unsubscribe ${state.agentId} failed:`,
        err instanceof Error ? err.message : String(err))
      return
    } finally {
      if (state.transition === transition) state.transition = null
    }
  }
}

function installRedisSubscriber(): void {
  if (redisSubscriberInstalled) return
  redisSubscriberInstalled = true
  redisSub.on('message', (channel, raw) => {
    if (channel === CH_WAKE_ACK) {
      let ack: { eventId: string; brokerId: string; accepted: number }
      try { ack = JSON.parse(raw) as typeof ack } catch { return }
      if (
        typeof ack.eventId !== 'string'
        || typeof ack.brokerId !== 'string'
        || !Number.isSafeInteger(ack.accepted)
        || ack.accepted < 0
      ) return
      const pending = pendingDeliveryAcks.get(ack.eventId)
      if (!pending || pending.responders.has(ack.brokerId)) return
      pending.responders.add(ack.brokerId)
      pending.accepted += Math.max(0, Math.floor(ack.accepted))
      settlePendingAck(ack.eventId, pending)
      return
    }
    if (!channel.startsWith(CH_WAKE_PREFIX)) return
    const agentId = channel.slice(CH_WAKE_PREFIX.length)
    const state = channels.get(agentId)
    let evt: WakeEvent
    try { evt = JSON.parse(raw) as WakeEvent } catch { return }
    const deliveries = state
      ? [...state.subscribers].map((subscriber) => deliverTo(subscriber, evt))
      : []
    void Promise.all(deliveries).then(async (results) => {
      const accepted = results.filter(Boolean).length
      await redis.publish(CH_WAKE_ACK, JSON.stringify({
        eventId: evt.id,
        brokerId,
        accepted,
      }))
    }).catch(() => {
      // Missing acknowledgements make the publisher return zero after its
      // timeout, which safely falls through to pod creation.
    })
  })
}

/**
 * Mint a wake event for one agent and publish it to the cluster.
 * Returns the number of streams that accepted the event after revalidation.
 * Missing acknowledgements return zero so stale Redis subscriptions cannot
 * suppress pod creation.
 */
export async function deliver(agentId: string, partial: WakeEventPayload): Promise<number> {
  // `partial` is the discriminated union minus the base fields. The
  // explicit spread + assign keeps TypeScript happy across both
  // branches without a cast.
  const evt = { ...partial, id: `${partial.kind}-${randomUUID()}`, at: Date.now() } as WakeEvent
  installRedisSubscriber()
  await ensureAckSubscription()
  const accepted = new Promise<number>((resolve) => {
    const pending: PendingDeliveryAck = {
      expected: null,
      accepted: 0,
      responders: new Set(),
      timer: setTimeout(() => {
        pendingDeliveryAcks.delete(evt.id)
        resolve(0)
      }, DELIVERY_ACK_TIMEOUT_MS),
      resolve,
    }
    pendingDeliveryAcks.set(evt.id, pending)
  })
  try {
    const expected = await redis.publish(CH_WAKE_PREFIX + agentId, JSON.stringify(evt))
    const pending = pendingDeliveryAcks.get(evt.id)
    if (pending) {
      pending.expected = expected
      settlePendingAck(evt.id, pending)
    }
    return await accepted
  } catch (err) {
    const pending = pendingDeliveryAcks.get(evt.id)
    if (pending) {
      clearTimeout(pending.timer)
      pendingDeliveryAcks.delete(evt.id)
      pending.resolve(0)
    }
    throw err
  }
}

/** Steer-flavoured deliver — typed wrapper so callers don't have to
 *  spell `kind: 'steer'` and we get full payload type-checking. The
 *  pod's SSE handler routes events by their `event:` line (the SSE
 *  field that mirrors `evt.kind`), so a wake-subscribed pod
 *  automatically receives steer events too — no separate channel /
 *  subscriber needed. */
export async function deliverSteer(
  agentId: string,
  payload: Omit<Extract<WakeEvent, { kind: 'steer' }>, 'id' | 'at' | 'kind'>,
): Promise<number> {
  return deliver(agentId, { kind: 'steer', ...payload })
}

/** Diagnostic — agent ids with at least one LOCAL live subscriber.
 *  Other servers in the cluster may also have subs; this is local view. */
export function listLocalSubscribedAgents(): string[] {
  const out: string[] = []
  for (const [id, state] of channels) {
    for (const s of state.subscribers) if (!s.closed && s.ready) { out.push(id); break }
  }
  return out
}

/**
 * Hook a long-poll SSE response onto the bus. Sends an immediate
 * `ready` event so the Pod knows the stream is alive, then keeps the
 * response open. Subscribes to the per-agent Redis channel iff this
 * is the first local sub for that agent — and NEVER while a final
 * unsubscribe for that channel is still settling.
 */
export async function attachWakeStream(
  agentId: string,
  res: Response,
  auth: WakeStreamAuth,
): Promise<void> {
  installRedisSubscriber()

  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache, no-transform')
  res.setHeader('Connection', 'keep-alive')
  res.setHeader('X-Accel-Buffering', 'no')           // disable nginx buffering
  let state = channels.get(agentId)
  if (!state) {
    state = { agentId, subscribers: new Set(), active: false, transition: null }
    channels.set(agentId, state)
  }
  const s: Subscriber = {
    agentId,
    res,
    closed: false,
    ready: false,
    revalidate: auth.revalidate,
    expiresAtMs: auth.expiresAtMs,
    deliveryQueue: [],
    pendingDeliveries: 0,
    processingDelivery: false,
    close: () => {},
  }
  state.subscribers.add(s)

  let ping: ReturnType<typeof setInterval> | null = null
  let expiry: ReturnType<typeof setTimeout> | null = null
  let cleaned = false
  const cleanup = (): void => {
    if (cleaned) return
    cleaned = true
    s.closed = true
    if (ping) clearInterval(ping)
    if (expiry) clearTimeout(expiry)
    failQueuedDeliveries(s)
    state!.subscribers.delete(s)
    const last = state!.subscribers.size === 0
    console.log(`[wake-bus] -sub ${agentId} (${state!.subscribers.size} local · last=${last})`)
    if (last) void deactivateAgentChannelIfIdle(state!)
  }
  s.close = (): void => {
    if (cleaned) return
    cleanup()
    try { res.end() } catch { /* ignore */ }
  }
  res.on('close', cleanup)
  res.on('error', cleanup)

  // Subscribe to the Redis channel BEFORE we tell the Pod we're ready —
  // otherwise a wake that arrives during the gap is silently dropped
  // (the Pod's initial-drain catches it, but we'd rather not rely on
  // that fallback when avoidable).
  try {
    await ensureAgentChannelActive(state)
  } catch (err) {
    // Keep the HTTP response untouched so the route can still return a 500.
    cleanup()
    throw err
  }
  if (s.closed) return

  if (auth.expiresAtMs <= Date.now()) {
    s.close()
    return
  }
  try {
    res.flushHeaders?.()
    res.write(`event: ready\ndata: {"agentId":"${agentId}","at":${Date.now()}}\n\n`)
    s.ready = true
  } catch {
    s.close()
    return
  }
  console.log(`[wake-bus] +sub ${agentId} (${state.subscribers.size} local · redis-subscribed=true)`)

  // Close at the JWT's exact expiry. Assignment changes and revocation are
  // checked before each future event, so no polling interval or fixed sleep can
  // leave an invalid subscriber eligible for a wake.
  if (Number.isFinite(auth.expiresAtMs)) {
    expiry = setTimeout(s.close, Math.max(0, auth.expiresAtMs - Date.now()))
  }

  // SSE comment ping every 25s. Clients that idle out at 30s (some
  // proxies) stay alive.
  ping = setInterval(() => {
    if (s.closed) return
    try { res.write(`: ping ${Date.now()}\n\n`) } catch { s.close() }
  }, 25_000)
}
