package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Per-client limits on what waits for the next frame; past them the oldest
// entries are dropped and counted, so a slow browser cannot stall the hub.
const (
	maxPendingBars    = 20000
	maxPendingSignals = 5000
)

// client is one WebSocket connection. Bars reach it only for the symbols it
// subscribed to; every signal and stats update reaches every client.
type client struct {
	conn *websocket.Conn

	mu       sync.Mutex
	symbols  map[string]bool
	bars     []json.RawMessage
	signals  []json.RawMessage
	stats    json.RawMessage
	dropped  int
	notified chan struct{} // has something to send
}

func (c *client) push(bar bool, msg json.RawMessage) {
	c.mu.Lock()
	if bar {
		if len(c.bars) >= maxPendingBars {
			c.bars = c.bars[1:]
			c.dropped++
			wsDropped.Inc()
		}
		c.bars = append(c.bars, msg)
	} else {
		if len(c.signals) >= maxPendingSignals {
			c.signals = c.signals[1:]
			c.dropped++
			wsDropped.Inc()
		}
		c.signals = append(c.signals, msg)
	}
	c.mu.Unlock()
	c.notify()
}

func (c *client) notify() {
	select {
	case c.notified <- struct{}{}:
	default:
	}
}

// frame takes everything pending as one JSON message, or nil if there is none.
func (c *client) frame() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.bars) == 0 && len(c.signals) == 0 && c.stats == nil {
		return nil
	}
	var b bytes.Buffer
	b.WriteString(`{"bars":[`)
	writeJoined(&b, c.bars)
	b.WriteString(`],"signals":[`)
	writeJoined(&b, c.signals)
	b.WriteString(`],"dropped":`)
	b.WriteString(strconv.Itoa(c.dropped))
	if c.stats != nil {
		b.WriteString(`,"stats":`)
		b.Write(c.stats)
	}
	b.WriteByte('}')
	c.bars, c.signals, c.stats, c.dropped = c.bars[:0], c.signals[:0], nil, 0
	return b.Bytes()
}

func writeJoined(b *bytes.Buffer, msgs []json.RawMessage) {
	for i, m := range msgs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(m)
	}
}

// hub fans records out to clients, indexing bar subscriptions by symbol.
type hub struct {
	mu       sync.RWMutex
	clients  map[*client]bool
	bySymbol map[string]map[*client]bool
}

func newHub() *hub {
	return &hub{clients: make(map[*client]bool), bySymbol: make(map[string]map[*client]bool)}
}

func (h *hub) add(c *client) {
	h.mu.Lock()
	h.clients[c] = true
	h.mu.Unlock()
	wsClients.Inc()
}

func (h *hub) remove(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	for s := range c.symbols {
		delete(h.bySymbol[s], c)
	}
	h.mu.Unlock()
	wsClients.Dec()
}

// subscribe replaces the client's bar subscriptions.
func (h *hub) subscribe(c *client, symbols []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range c.symbols {
		delete(h.bySymbol[s], c)
	}
	c.mu.Lock()
	c.symbols = make(map[string]bool, len(symbols))
	for _, s := range symbols {
		c.symbols[s] = true
		if h.bySymbol[s] == nil {
			h.bySymbol[s] = make(map[*client]bool)
		}
		h.bySymbol[s][c] = true
	}
	c.mu.Unlock()
}

func (h *hub) bar(symbol string, msg json.RawMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.bySymbol[symbol] {
		c.push(true, msg)
	}
}

func (h *hub) signal(msg json.RawMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		c.push(false, msg)
	}
}

func (h *hub) stats(msg json.RawMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		c.mu.Lock()
		c.stats = msg
		c.mu.Unlock()
		c.notify()
	}
}

// writeLoop sends at most one frame per interval, so bursts are batched.
func (h *hub) writeLoop(ctx context.Context, c *client, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.notified:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		if f := c.frame(); f != nil {
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.conn.Write(wctx, websocket.MessageText, f)
			cancel()
			if err != nil {
				return err
			}
			wsFrames.Inc()
			wsBytes.Add(float64(len(f)))
		}
	}
}
