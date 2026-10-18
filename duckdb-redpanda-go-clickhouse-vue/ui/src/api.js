// HTTP history from the gateway and the live WebSocket feed.

export async function getJSON(path) {
  const res = await fetch(path)
  if (!res.ok) throw new Error(`${path}: ${res.status} ${await res.text()}`)
  return res.json()
}

// The gateway answers with ClickHouse's JSONColumns: { col: [values], ... }
export function rows(columns) {
  const keys = Object.keys(columns ?? {})
  const n = keys.length ? columns[keys[0]].length : 0
  const out = new Array(n)
  for (let i = 0; i < n; i++) {
    const row = {}
    for (const k of keys) row[k] = columns[k][i]
    out[i] = row
  }
  return out
}

// Live keeps one WebSocket open, reconnecting with backoff, and re-sends the
// subscription after every reconnect.
export class Live {
  constructor({ onFrame, onStatus }) {
    this.onFrame = onFrame
    this.onStatus = onStatus
    this.symbols = []
    this.delay = 500
    this.closed = false
    this.connect()
  }

  connect() {
    const scheme = location.protocol === 'https:' ? 'wss' : 'ws'
    const ws = new WebSocket(`${scheme}://${location.host}/ws`)
    this.ws = ws
    this.onStatus('connecting')
    ws.onopen = () => {
      this.delay = 500
      this.onStatus('live')
      this.send()
    }
    ws.onmessage = (e) => this.onFrame(JSON.parse(e.data))
    ws.onclose = () => {
      if (this.closed) return
      this.onStatus('reconnecting')
      setTimeout(() => this.connect(), this.delay)
      this.delay = Math.min(this.delay * 2, 10000)
    }
  }

  subscribe(symbols) {
    this.symbols = symbols
    this.send()
  }

  send() {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ subscribe: this.symbols }))
    }
  }

  close() {
    this.closed = true
    this.ws?.close()
  }
}
