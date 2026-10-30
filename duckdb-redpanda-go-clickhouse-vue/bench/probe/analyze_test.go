package main

import (
	"strings"
	"testing"
)

const w0 = int64(1636358400000) // 2021-11-08 09:00 Europe/Berlin

func TestAnalyzeSplitsLatencyIntoStages(t *testing.T) {
	// The replay started at wall time 1_000_000 from event time w0 at 10x, so
	// the end of window w0 (5 min later) was due at 1_000_000 + 30_000
	a := args{startMs: 1_000_000, originMs: w0, speed: 10, fromMs: w0, toMs: w0 + windowMs}
	csv := strings.Join([]string{
		"kind,symbol,window_start_ms,side,trigger_sent_ms,emitted_ms,recv_ms",
		"bar,A,1636358400000,,1030020,1030025,1030200",
		"bar,B,1636358400000,,1030040,1030041,1030100",
		"signal,A,1636358400000,BUY,1030020,1030025,1030200",
		"bar,A,1636358700000,,1060000,1060001,1060002", // next window: outside the range
		"# dropped=3",
	}, "\n")
	r, err := analyze(strings.NewReader(csv), a)
	if err != nil {
		t.Fatal(err)
	}
	if r.bars != 2 || r.signals != 1 || r.dropped != 3 {
		t.Fatalf("got bars=%d signals=%d dropped=%d, want 2 1 3", r.bars, r.signals, r.dropped)
	}
	// Each bar's stages add up to its total
	for i := range r.total {
		if sum := r.wait[i] + r.service[i] + r.gateway[i]; sum != r.total[i] {
			t.Fatalf("bar %d: stages sum to %v, total %v", i, sum, r.total[i])
		}
	}
	out := r.String()
	for _, want := range []string{"total_p50_ms=100.0", "total_max_ms=200.0", "system_max_ms=180.0",
		"sig_total_max_ms=200.0", "wait_p50_ms=20.0", "service_p50_ms=1.0", "gateway_p50_ms=59.0"} {
		if !strings.Contains(out, want) {
			t.Errorf("%s missing from %q", want, out)
		}
	}
}

func TestAnalyzeEmptyRangeGivesNA(t *testing.T) {
	a := args{startMs: 0, originMs: 0, speed: 1, fromMs: 0, toMs: 1}
	r, err := analyze(strings.NewReader("kind,symbol,window_start_ms,side,trigger_sent_ms,emitted_ms,recv_ms\n"), a)
	if err != nil {
		t.Fatal(err)
	}
	if out := r.String(); !strings.Contains(out, "total_p50_ms=NA") || !strings.Contains(out, "bars=0") {
		t.Fatalf("got %q", out)
	}
}
