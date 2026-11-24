package main

import "math"

// windowMs is the length of a tumbling window. Berlin is a whole number of
// hours from UTC, so windows aligned on epoch milliseconds are also aligned
// on the Berlin clock.
const windowMs = 5 * 60 * 1000

// Smoothing factors, computed in float64 like ClickHouse computes 2 / 39 and
// 1 - a38, so the EMAs match oracle.sql bit for bit.
var (
	a38, a100 = 2 / float64(39), 2 / float64(101)
	b38, b100 = 1 - a38, 1 - a100
)

// tick is one record of the ticks topic. An empty symbol marks a watermark:
// no later record in the partition has an earlier event time than TsMs.
type tick struct {
	Symbol  string  `json:"symbol"`
	SecType string  `json:"sectype"`
	Last    float64 `json:"last"`
	TsMs    int64   `json:"ts_ms"`
	Seq     uint64  `json:"seq"`
	SentMs  int64   `json:"sent_ms"`
}

type bar struct {
	symbol                 string
	windowStart            int64
	open, high, low, close float64
	trades                 uint32
	ema38, ema100          float64
	triggerSentMs          int64 // sent_ms of the record that closed the window
}

type crossover struct {
	symbol        string
	windowStart   int64
	side          string // BUY or SELL
	close         float64
	ema38, ema100 float64
	triggerSentMs int64
}

// sink receives what closing a window produces.
type sink interface {
	bar(*bar)
	signal(*crossover)
}

// series is the state of one symbol: its open window and its EMAs.
type series struct {
	symbol string

	open              bool
	ws                int64 // start of the open window
	lastClosedWs      int64 // start of the last closed window
	o, h, l, c        float64
	firstTs, lastTs   int64
	firstSeq, lastSeq uint64
	trades            uint32
	seeded            bool
	ema38, ema100     float64
	rel               int8 // last non-zero sign(ema38 - ema100)
}

// partition holds every symbol of one ticks partition. Its records arrive in
// receipt order, roughly but not strictly by event time. The highest event
// time seen is the partition watermark; windows wait allowedLateness past it.
type partition struct {
	firstClose      bool // seed both EMAs with the first close (else with zero)
	allowedLateness int64
	syms            map[string]*series
	order           []*series // first-seen order, so watermark closes are deterministic
	wm              int64
	late            int64
	sentMs          int64 // sent_ms of the record being applied, for latency breakdowns
}

func newPartition(firstClose bool, allowedLateness ...int64) *partition {
	var lat int64
	if len(allowedLateness) > 0 {
		lat = allowedLateness[0]
	}
	return &partition{
		firstClose:      firstClose,
		allowedLateness: lat,
		syms:            make(map[string]*series),
		wm:              math.MinInt64,
	}
}

func windowStart(ts int64) int64 {
	return ts - ((ts%windowMs)+windowMs)%windowMs
}

// add applies one record. A window closes immediately when a later event arrives
// for its own symbol (own watermark), or at latest when the partition watermark
// passes the window's end + allowedLateness.
func (p *partition) add(t *tick, out sink) {
	p.sentMs = t.SentMs
	if t.Symbol == "" {
		p.advance(t.TsMs, out)
		return
	}
	ws := windowStart(t.TsMs)
	if ws+windowMs+p.allowedLateness <= p.wm {
		p.late++ // its window has already closed by partition timeout
		return
	}
	s := p.syms[t.Symbol]
	if s == nil {
		s = &series{symbol: t.Symbol}
		p.syms[t.Symbol] = s
		p.order = append(p.order, s)
	}

	// Check if this symbol has already closed this window
	if (s.open && ws < s.ws) || (!s.open && s.lastClosedWs != 0 && ws <= s.lastClosedWs) {
		p.late++
		return
	}

	// Own watermark fast path: stock's own trade in a later window closes its open window immediately
	if s.open && ws > s.ws {
		p.close(s, out)
	}

	if !s.open {
		*s = series{symbol: s.symbol, seeded: s.seeded, ema38: s.ema38, ema100: s.ema100, rel: s.rel,
			lastClosedWs: s.lastClosedWs,
			open: true, ws: ws, o: t.Last, h: t.Last, l: t.Last, c: t.Last,
			firstTs: t.TsMs, lastTs: t.TsMs, firstSeq: t.Seq, lastSeq: t.Seq, trades: 1}
	} else {
		s.h = max(s.h, t.Last)
		s.l = min(s.l, t.Last)
		s.trades++
		if t.TsMs < s.firstTs || (t.TsMs == s.firstTs && t.Seq < s.firstSeq) {
			s.o, s.firstTs, s.firstSeq = t.Last, t.TsMs, t.Seq
		}
		if t.TsMs > s.lastTs || (t.TsMs == s.lastTs && t.Seq > s.lastSeq) {
			s.c, s.lastTs, s.lastSeq = t.Last, t.TsMs, t.Seq
		}
	}
	p.advance(t.TsMs, out)
}

// advance moves the watermark to ts. Windows only end on 5-minute boundaries,
// so open windows are checked only when one is crossed (accounting for allowedLateness).
func (p *partition) advance(ts int64, out sink) {
	if ts <= p.wm {
		return
	}
	oldWm := p.wm
	p.wm = ts
	crossed := oldWm == math.MinInt64 || windowStart(ts-p.allowedLateness) != windowStart(oldWm-p.allowedLateness)
	if !crossed {
		return
	}
	for _, s := range p.order {
		if s.open && s.ws+windowMs+p.allowedLateness <= ts {
			p.close(s, out)
		}
	}
}

func (p *partition) close(s *series, out sink) {
	c := s.c
	switch {
	case s.seeded:
		// The conversions round each product, which keeps the compiler from
		// fusing them into one multiply-add (ClickHouse rounds each too)
		s.ema38 = float64(c*a38) + float64(s.ema38*b38)
		s.ema100 = float64(c*a100) + float64(s.ema100*b100)
	case p.firstClose:
		s.ema38, s.ema100 = c, c
	default:
		s.ema38, s.ema100 = float64(c*a38), float64(c*a100)
	}
	s.seeded = true
	s.lastClosedWs = s.ws
	s.open = false
	out.bar(&bar{symbol: s.symbol, windowStart: s.ws, open: s.o, high: s.h, low: s.l, close: c,
		trades: s.trades, ema38: s.ema38, ema100: s.ema100, triggerSentMs: p.sentMs})

	var rel int8
	switch {
	case s.ema38 > s.ema100:
		rel = 1
	case s.ema38 < s.ema100:
		rel = -1
	default:
		return
	}
	side := ""
	switch {
	case rel == 1 && s.rel == -1:
		side = "BUY"
	case rel == -1 && s.rel == 1:
		side = "SELL"
	}
	s.rel = rel
	if side != "" {
		out.signal(&crossover{symbol: s.symbol, windowStart: s.ws, side: side, close: c,
			ema38: s.ema38, ema100: s.ema100, triggerSentMs: p.sentMs})
	}
}
