/**
 * Minimal HS256 JWT for runtime-pod ↔ server auth.
 *
 * Server signs one token per runtime with `{ sub: agentId, companyId,
 * runtimeKind, computerId, assignmentVersion, scope: 'agent-runner',
 * iat, exp }`. The immutable runtimeKind distinguishes managed pods
 * from BYOA daemons even after reassignment; the computerId pins the
 * exact host that minted the credential; and the opaque persisted
 * assignmentVersion binds the token to the agent's assignment epoch so
 * a stale credential can never survive an effective host/engine change,
 * a departure/reactivation cycle, or an A→B→A return to the same host.
 * The runtime presents the token on every `/runtime/*` request. Server
 * verifies sig + exp + scope, pins the agentId to the token, and
 * revalidates the live assignment on every request and before every
 * outgoing wake event — endpoints never trust an agentId from the
 * request body.
 *
 * Why homegrown instead of `jose`: the surface area is small (sign +
 * verify, fixed alg, fixed claim shape), the secret never leaves the
 * server, and avoiding the dep keeps the agent-loop bundle smaller
 * (HttpRuntimeClient only needs the token string, not the lib).
 */
import { createHmac, timingSafeEqual } from 'node:crypto'
import { env } from '../../env.js'

export interface AgentRuntimeClaims {
  /** agent participant id — pinned at sign time, immutable for the token's life. */
  sub: string
  /** company id the agent belongs to. Pinned so requests can't cross tenants. */
  companyId: string | null
  /** Immutable origin of this runtime credential. */
  runtimeKind: 'managed' | 'byoa'
  /** The exact computer (cloud or paired host) this credential was minted
   *  for. Present on every token minted after assignment binding; legacy
   *  unbound tokens carry no computerId and fail the assignment check. */
  computerId?: string
  /** Opaque persisted assignment version observed at mint time. Rotates
   *  whenever the agent's effective host/engine assignment changes and
   *  across departure/reactivation — so returning to a previous host can
   *  never revive a credential minted there. Opaque by design: callers must
   *  never parse meaning out of it, only compare equality. */
  assignmentVersion?: string
  /** Fixed string; lets the server reject tokens minted for other purposes. */
  scope: 'agent-runner'
  /** issued-at, unix seconds. */
  iat: number
  /** expiry, unix seconds. Pod lifetimes are short — default 1h. */
  exp: number
}

const DEFAULT_TTL_SECONDS = 60 * 60

/** Legacy BYOA daemons minted two-hour tokens. Never let a hand-crafted
 * unpinned credential extend that upgrade window. Assignment auth still
 * rejects every unpinned token because it cannot prove which assignment
 * minted it; current daemons remint a bound token after the resulting 401. */
export const LEGACY_BYOA_MAX_TTL_SECONDS = 2 * 60 * 60

function b64url(buf: Buffer): string {
  return buf.toString('base64').replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_')
}

function fromB64url(s: string): Buffer {
  const pad = s.length % 4 === 0 ? '' : '='.repeat(4 - (s.length % 4))
  return Buffer.from(s.replace(/-/g, '+').replace(/_/g, '/') + pad, 'base64')
}

function sign(headerB64: string, payloadB64: string): string {
  const mac = createHmac('sha256', env.AGENT_RUNTIME_SECRET)
  mac.update(`${headerB64}.${payloadB64}`)
  return b64url(mac.digest())
}

/** Mint a token for a managed pod or paired BYOA daemon. Both kinds MUST
 *  be bound to their observed assignment: the minting computer and the
 *  opaque persisted assignment version. Unbound minting is refused so no
 *  new fail-open credential can enter circulation. */
export function signAgentToken(args: {
  agentId: string
  companyId: string | null
  runtimeKind?: 'managed' | 'byoa'
  /** Host identity the credential is bound to — required for BYOA (the
   *  paired device) and for managed pods (the cloud computer). */
  computerId?: string
  /** Opaque assignment version observed at mint time. Required. */
  assignmentVersion?: string
  ttlSeconds?: number
}): string {
  const runtimeKind = args.runtimeKind ?? 'managed'
  if (!args.computerId) {
    throw new Error('computerId is required for agent runtime tokens')
  }
  if (!args.assignmentVersion) {
    throw new Error('assignmentVersion is required for agent runtime tokens')
  }
  const now = Math.floor(Date.now() / 1000)
  const claims: AgentRuntimeClaims = {
    sub: args.agentId,
    companyId: args.companyId,
    runtimeKind,
    computerId: args.computerId,
    assignmentVersion: args.assignmentVersion,
    scope: 'agent-runner',
    iat: now,
    exp: now + (args.ttlSeconds ?? DEFAULT_TTL_SECONDS),
  }
  const headerB64 = b64url(Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })))
  const payloadB64 = b64url(Buffer.from(JSON.stringify(claims)))
  const sigB64 = sign(headerB64, payloadB64)
  return `${headerB64}.${payloadB64}.${sigB64}`
}

/** Verify + decode. Throws on any tampering, expiry, or scope mismatch. */
export function verifyAgentToken(token: string): AgentRuntimeClaims {
  const parts = token.split('.')
  if (parts.length !== 3) throw new Error('malformed token')
  const [headerB64, payloadB64, sigB64] = parts
  const expected = sign(headerB64, payloadB64)
  const a = Buffer.from(sigB64)
  const b = Buffer.from(expected)
  if (a.length !== b.length || !timingSafeEqual(a, b)) throw new Error('bad signature')
  const claims = JSON.parse(fromB64url(payloadB64).toString('utf8')) as AgentRuntimeClaims
  if (claims.scope !== 'agent-runner') throw new Error('bad scope')
  if (claims.runtimeKind !== 'managed' && claims.runtimeKind !== 'byoa') throw new Error('missing runtime kind')
  const now = Math.floor(Date.now() / 1000)
  if (typeof claims.iat !== 'number') throw new Error('missing iat')
  if (typeof claims.exp !== 'number' || claims.exp <= now) throw new Error('token expired')
  if (typeof claims.sub !== 'string' || !claims.sub) throw new Error('missing sub')
  if (claims.computerId !== undefined && (typeof claims.computerId !== 'string' || !claims.computerId)) {
    throw new Error('invalid computerId')
  }
  if (claims.assignmentVersion !== undefined
    && (typeof claims.assignmentVersion !== 'string' || !claims.assignmentVersion)) {
    throw new Error('invalid assignmentVersion')
  }
  if (claims.runtimeKind === 'byoa' && !claims.computerId && claims.exp - claims.iat > LEGACY_BYOA_MAX_TTL_SECONDS) {
    throw new Error('legacy BYOA token exceeds upgrade window')
  }
  return claims
}

/** BYOA-only deployments accept only credentials minted by a paired daemon.
 * This must use the immutable token claim, never the agent's current computer
 * assignment: a managed pod can retain its token after reassignment. */
export function runtimeAllowedWhenServerLlmDisabled(claims: AgentRuntimeClaims): boolean {
  return claims.runtimeKind === 'byoa'
}

/** The live assignment state a runtime credential must still match. */
export interface AgentRuntimeAssignment {
  /** The agent's current computer, or null when unassigned. */
  computerId: string | null
  /** Kind of that computer — 'cloud' hosts run managed pods, 'local'/'vps'
   *  hosts run BYOA daemons. */
  computerKind: 'cloud' | 'local' | 'vps' | null
  /** The agent's current engine. */
  engine: string | null
  /** Current opaque persisted assignment version. */
  assignmentVersion: string | null
  /** True when the assigned computer row is revoked. */
  computerRevoked: boolean
}

/** Ensure a runtime credential still names the agent's CURRENT assignment
 *  epoch. The persisted version rotates on every effective host/engine
 *  change, on departure and on reactivation, and at computer revocation —
 *  so a credential minted for any earlier assignment fails here even when
 *  the agent has since returned to the exact same host (A→B→A). Legacy
 *  unbound tokens (no computerId / no assignmentVersion) cannot prove they
 *  survived no reassignment and fail closed; the holder must remint. */
export function runtimeCredentialMatchesAssignment(
  claims: AgentRuntimeClaims,
  assignment: AgentRuntimeAssignment | null,
): boolean {
  if (!assignment || assignment.computerRevoked) return false
  if (typeof claims.assignmentVersion !== 'string' || !claims.assignmentVersion) return false
  if (claims.assignmentVersion !== assignment.assignmentVersion) return false
  if (typeof claims.computerId !== 'string' || !claims.computerId) return false
  if (claims.computerId !== assignment.computerId) return false
  if (claims.runtimeKind === 'managed') {
    return assignment.computerKind === 'cloud' && assignment.engine === 'managed'
  }
  return assignment.computerKind === 'local' || assignment.computerKind === 'vps'
}
