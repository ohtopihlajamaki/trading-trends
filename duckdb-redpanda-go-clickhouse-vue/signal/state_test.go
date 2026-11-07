package main

import (
	"testing"
)

type recorder struct {
	bars    []bar
	signals []crossover
}

func (r *recorder) bar(b *bar)          { r.bars = append(r.bars, *b) }
func (r *recorder) signal(c *crossover) { r.signals = append(r.signals, *c) }

const t0 = int64(1636358400000) // 2021-11-08 09:00 Europe/Berlin

func min5(n int64) int64 { return t0 + n*windowMs }

func TestWindowClosesOnLaterEventOfSymbol(t *testing.T) {
	p, r := newPartition(true), &recorder{}
	p.add(&tick{Symbol: "A", Last: 10, TsMs: t0 + 1000, Seq: 1}, r)
	p.add(&tick{Symbol: "A", Last: 12, TsMs: t0 + 2000, Seq: 2}, r)
	p.add(&tick{Symbol: "A", Last: 9, TsMs: t0 + 3000, Seq: 3}, r)
	if len(r.bars) != 0 {
		t.Fatalf("window closed early: %+v", r.bars)
	}
	p.add(&tick{Symbol: "A", Last: 11, TsMs: min5(1), Seq: 4}, r)
	if len(r.bars) != 1 {
		t.Fatalf("want 1 bar, got %d", len(r.bars))
	}
	b := r.bars[0]
	want := bar{symbol: "A", windowStart: t0, open: 10, high: 12, low: 9, close: 9, trades: 3, ema38: 9, ema100: 9}
	if b != want {
		t.Fatalf("got %+v, want %+v", b, want)
	}
}

func TestCloseIsLastByTimeThenSeq(t *testing.T) {
	p, r := newPartition(true), &recorder{}
	p.add(&tick{Symbol: "A", Last: 1, TsMs: t0 + 5000, Seq: 7}, r)
	p.add(&tick{Symbol: "A", Last: 2, TsMs: t0 + 5000, Seq: 9}, r)
	p.add(&tick{Symbol: "A", Last: 3, TsMs: t0 + 5000, Seq: 8}, r)  // same time, lower seq
	p.add(&tick{Symbol: "A", Last: 4, TsMs: t0 + 4000, Seq: 99}, r) // earlier time
	p.add(&tick{Symbol: "", TsMs: min5(1)}, r)
	if len(r.bars) != 1 || r.bars[0].close != 2 || r.bars[0].open != 4 {
		t.Fatalf("got %+v, want open 4 close 2", r.bars)
	}
}

func TestWatermarkClosesOtherSymbols(t *testing.T) {
	p, r := newPartition(true), &recorder{}
	p.add(&tick{Symbol: "A", Last: 10, TsMs: t0 + 1000, Seq: 1}, r)
	p.add(&tick{Symbol: "B", Last: 20, TsMs: t0 + 2000, Seq: 2}, r)
	p.add(&tick{Symbol: "B", Last: 21, TsMs: min5(1) - 1, Seq: 3}, r)
	if len(r.bars) != 0 {
		t.Fatalf("closed before the window end: %+v", r.bars)
	}
	// A later event of B closes B's window, and its time passes the end of A's
	p.add(&tick{Symbol: "B", Last: 22, TsMs: min5(1), Seq: 4}, r)
	if len(r.bars) != 2 || r.bars[0].symbol != "B" || r.bars[1].symbol != "A" {
		t.Fatalf("want bars for B, then A, got %+v", r.bars)
	}
}

func TestWatermarkRecordClosesEverything(t *testing.T) {
	p, r := newPartition(true), &recorder{}
	p.add(&tick{Symbol: "A", Last: 10, TsMs: t0 + 1000, Seq: 1}, r)
	p.add(&tick{Symbol: "B", Last: 20, TsMs: min5(3), Seq: 2}, r)
	p.add(&tick{TsMs: min5(100)}, r)
	if len(r.bars) != 2 {
		t.Fatalf("want 2 bars, got %+v", r.bars)
	}
}

func TestLateTickIsDropped(t *testing.T) {
	p, r := newPartition(true), &recorder{}
	p.add(&tick{Symbol: "A", Last: 10, TsMs: min5(2), Seq: 1}, r)
	p.add(&tick{Symbol: "B", Last: 10, TsMs: min5(1), Seq: 2}, r)
	if p.late != 1 {
		t.Fatalf("want 1 late tick, got %d", p.late)
	}
}

func TestOwnWatermarkClosesImmediatelyRegardlessOfAllowedLateness(t *testing.T) {
	// Partition has 1000ms allowed lateness
	p, r := newPartition(true, 1000), &recorder{}
	p.add(&tick{Symbol: "A", Last: 10, TsMs: t0 + 1000, Seq: 1}, r)
	if len(r.bars) != 0 {
		t.Fatalf("window should be open")
	}
	// Stock A gets a trade in the next window (min5(1) + 50ms)
	p.add(&tick{Symbol: "A", Last: 12, TsMs: min5(1) + 50, Seq: 2}, r)
	// Window 0 for A must close immediately, even though partition watermark hasn't crossed min5(1) + 1000ms
	if len(r.bars) != 1 {
		t.Fatalf("want 1 bar closed immediately by own watermark, got %d", len(r.bars))
	}
	if r.bars[0].close != 10 {
		t.Fatalf("want close 10, got %v", r.bars[0].close)
	}
}

func TestOutOfOrderTickWithinGracePeriodIsAccepted(t *testing.T) {
	// 1000ms allowed lateness
	p, r := newPartition(true, 1000), &recorder{}
	// Stock A opens window 0
	p.add(&tick{Symbol: "A", Last: 10, TsMs: t0 + 1000, Seq: 1}, r)
	// Stock B makes a trade 10ms into window 1, advancing partition watermark to min5(1) + 10
	p.add(&tick{Symbol: "B", Last: 50, TsMs: min5(1) + 10, Seq: 2}, r)
	// Stock A receives a late trade stamped 5ms before window 0 ended (min5(1) - 5)
	p.add(&tick{Symbol: "A", Last: 11, TsMs: min5(1) - 5, Seq: 3}, r)
	// Must not be dropped as late!
	if p.late != 0 {
		t.Fatalf("want 0 late ticks within grace period, got %d", p.late)
	}
	// Watermark reaches min5(1) + 1000ms -> timeout closes window 0
	p.add(&tick{Symbol: "B", Last: 51, TsMs: min5(1) + 1000, Seq: 4}, r)
	if len(r.bars) < 1 {
		t.Fatalf("want bars closed, got %d", len(r.bars))
	}
	// The late trade's price 11 must be the closing price!
	if r.bars[0].close != 11 {
		t.Fatalf("want close 11 from the late trade, got %v", r.bars[0].close)
	}
}

func TestEMAFormula(t *testing.T) {
	p, r := newPartition(true), &recorder{}
	closes := []float64{100, 110, 105}
	for i, c := range closes {
		p.add(&tick{Symbol: "A", Last: c, TsMs: min5(int64(i)), Seq: uint64(i)}, r)
	}
	p.add(&tick{TsMs: min5(10)}, r)
	e38, e100 := 100.0, 100.0
	for i, c := range closes {
		if i > 0 {
			e38 = float64(c*a38) + float64(e38*b38)
			e100 = float64(c*a100) + float64(e100*b100)
		}
		if r.bars[i].ema38 != e38 || r.bars[i].ema100 != e100 {
			t.Fatalf("bar %d: got %v %v, want %v %v", i, r.bars[i].ema38, r.bars[i].ema100, e38, e100)
		}
	}
}

func TestZeroSeeding(t *testing.T) {
	p, r := newPartition(false), &recorder{}
	p.add(&tick{Symbol: "A", Last: 39, TsMs: t0, Seq: 1}, r)
	p.add(&tick{TsMs: min5(1)}, r)
	if r.bars[0].ema38 != 39*a38 || r.bars[0].ema100 != 39*a100 {
		t.Fatalf("got %+v", r.bars[0])
	}
}

// Rising closes put EMA38 above EMA100, falling closes below; the first
// non-zero relation only sets the baseline, and equal EMAs keep it.
func TestFlipRule(t *testing.T) {
	p, r := newPartition(true), &recorder{}
	closes := []float64{100, 101, 102, 103, 90, 80, 70, 120, 130}
	for i, c := range closes {
		p.add(&tick{Symbol: "A", Last: c, TsMs: min5(int64(i)), Seq: uint64(i)}, r)
	}
	p.add(&tick{TsMs: min5(20)}, r)
	var got []string
	for _, s := range r.signals {
		got = append(got, s.side)
	}
	// Window 0 seeds equal EMAs (no baseline); the rise at window 1 sets rel +1
	// as the baseline, the drop at window 4 crosses down and the jump back up
	// crosses at window 8 (worked out by hand). The closes move from the
	// second window on: two equal closes give EMAs that differ by rounding
	// alone, and that sign would become the baseline.
	want := []string{"SELL", "BUY"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
	if r.signals[0].windowStart != min5(4) || r.signals[1].windowStart != min5(8) {
		t.Fatalf("wrong windows: %+v", r.signals)
	}
}

func TestWindowStartIsClockAligned(t *testing.T) {
	for _, c := range []struct{ ts, ws int64 }{{t0, t0}, {t0 + 299999, t0}, {t0 + 300000, t0 + 300000}, {-1, -300000}} {
		if got := windowStart(c.ts); got != c.ws {
			t.Errorf("windowStart(%d) = %d, want %d", c.ts, got, c.ws)
		}
	}
}

func TestBarsCarryTheClosingRecordsSendTime(t *testing.T) {
	p, r := newPartition(true), &recorder{}
	p.add(&tick{Symbol: "A", Last: 10, TsMs: t0 + 1000, Seq: 1, SentMs: 100}, r)
	p.add(&tick{Symbol: "B", Last: 20, TsMs: t0 + 2000, Seq: 2, SentMs: 200}, r)
	// B's next tick crosses the boundary: it closes B, then the watermark closes A
	p.add(&tick{Symbol: "B", Last: 21, TsMs: min5(1), Seq: 3, SentMs: 300}, r)
	if len(r.bars) != 2 || r.bars[0].triggerSentMs != 300 || r.bars[1].triggerSentMs != 300 {
		t.Fatalf("got %+v, want both bars closed by the record sent at 300", r.bars)
	}
	p.add(&tick{Symbol: "", TsMs: min5(2), SentMs: 400}, r) // a watermark closes B again
	if len(r.bars) != 3 || r.bars[2].triggerSentMs != 400 {
		t.Fatalf("got %+v, want the watermark's send time", r.bars)
	}
}
