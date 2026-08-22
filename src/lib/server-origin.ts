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
export function resolveServerOrigin(): string {
  if (typeof localStorage !== 'undefined') {
    const override = localStorage.getItem(SERVER_URL_KEY)
    if (override) return override.replace(/\/+$/, '')
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
