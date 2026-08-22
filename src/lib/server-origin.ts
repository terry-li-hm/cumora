/** Runtime key for an operator override of the API origin. */
export const SERVER_URL_KEY = 'cumora.serverUrl'

/**
 * Resolve the API origin.
 *
 * 1. localStorage['cumora.serverUrl'] — operator override.
 * 2. http(s) pages that are not the public app.cumora.ai host — empty
 *    string, meaning same-origin relative `/api`. Self-hosted and
 *    tailnet deploys serve the SPA from the API, so a production bake of
 *    `https://api.cumora.ai` must not steal those sessions.
 * 3. VITE_CUMORA_API_BASE — Electron (`app:`) and the public web host.
 */
function isLoopbackOrigin(origin: string): boolean {
  return /^https?:\/\/(localhost|127\.0\.0\.1)(:|$)/i.test(origin)
}

export function resolveServerOrigin(): string {
  if (typeof localStorage !== 'undefined') {
    const override = localStorage.getItem(SERVER_URL_KEY)
    if (override) {
      const cleaned = override.replace(/\/+$/, '')
      // A localhost override on a tailnet/remote page points at the
      // client device, not this API. Ignore it so Safari-on-phone does
      // not probe the phone's own :5181.
      const pageIsRemote = typeof location !== 'undefined'
        && !/^(localhost|127\.0\.0\.1)$/i.test(location.hostname)
      if (!(pageIsRemote && isLoopbackOrigin(cleaned))) {
        return cleaned
      }
    }
  }
  if (
    typeof location !== 'undefined'
    && (location.protocol === 'http:' || location.protocol === 'https:')
    && !/^app\./i.test(location.hostname)
    && !(typeof location.search === 'string' && location.search.includes('webonly=1'))
  ) {
    return ''
  }
  const baked = import.meta.env.VITE_CUMORA_API_BASE as string | undefined
  return (baked ?? '').replace(/\/+$/, '')
}
