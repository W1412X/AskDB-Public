const env = (typeof import.meta !== 'undefined' && import.meta.env) ? import.meta.env : {}
const base = (env.VITE_API_BASE_URL || '/api').replace(/\/$/, '')

async function request(method, path, body) {
  const opts = { method, headers: {} }
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json'
    opts.body = JSON.stringify(body)
  }
  const r = await fetch(`${base}${path}`, opts)
  if (!r.ok) {
    const t = await r.text()
    let msg = t
    try {
      const j = JSON.parse(t)
      msg = j.detail || t
    } catch (_) {}
    throw new Error(msg)
  }
  return r.json()
}

export const configApi = {
  listFiles: () => request('GET', '/config/files'),
  get: (filename) => request('GET', `/config/${filename}`),
  put: (filename, data) => request('PUT', `/config/${filename}`, data),
  reload: () => request('POST', '/config/reload'),
}

export const initApi = {
  status: () => request('GET', '/init/status'),
  start: () => request('POST', '/init/start'),
}

export const queryApi = {
  run: (body) => request('POST', '/query/run', body),
  runAsync: (body) => request('POST', '/query/run/async', body),
  resume: (body) => request('POST', '/query/resume', body),
  resumeAsync: (body) => request('POST', '/query/resume/async', body),
  status: (workflowId) => request('GET', `/query/status/${encodeURIComponent(workflowId)}`),
}

/** Browser EventSource URL for SSE (same-origin; Vite dev proxy forwards /api). */
export function queryStreamUrl(workflowId) {
  const base = (env.VITE_API_BASE_URL || '/api').replace(/\/$/, '')
  const origin = typeof window !== 'undefined' ? window.location.origin : ''
  return `${origin}${base}/query/stream/${encodeURIComponent(workflowId)}`
}
