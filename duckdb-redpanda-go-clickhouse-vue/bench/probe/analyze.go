package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

const windowMs = 5 * 60 * 1000

// args are analyze's key=value arguments: the replayer's clock line
// (start_ms, origin_ms, speed) and the windows to measure, [from_ms, to_ms)
// by window start.
type args struct {
	in                string
	startMs, originMs int64
	speed             float64
	fromMs, toMs      int64
}

func parseArgs(kv []string) (args, error) {
	var a args
	var err error
	for _, s := range kv {
		k, v, ok := strings.Cut(s, "=")
		if !ok {
			return a, fmt.Errorf("argument %q is not key=value", s)
		}
		switch k {
		case "in":
			a.in = v
		case "start_ms":
			a.startMs, err = parseInt(v)
		case "origin_ms":
			a.originMs, err = parseInt(v)
		case "speed":
			a.speed, err = strconv.ParseFloat(v, 64)
		case "from_ms":
			a.fromMs, err = parseInt(v)
		case "to_ms":
			a.toMs, err = parseInt(v)
		default:
			return a, fmt.Errorf("unknown argument %q", k)
		}
		if err != nil {
			return a, fmt.Errorf("%s: %w", k, err)
		}
	}
	if a.in == "" || a.speed <= 0 || a.toMs <= a.fromMs {
		return a, fmt.Errorf("analyze needs in, start_ms, origin_ms, speed > 0 and from_ms < to_ms")
	}
	return a, nil
}

// dueMs is when the replayer's schedule reached event time ts.
func (a args) dueMs(ts int64) float64 {
	return float64(a.startMs) + float64(ts-a.originMs)/a.speed
}

type result struct {
	bars, signals int
	dropped       int64
	// Window end → received (total) and closing record sent → received
	// (system), over bars; total over signals; and the three stages over bars
	total, system, sigTotal, wait, service, gateway []float64
}

func (r result) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "bars=%d signals=%d dropped=%d", r.bars, r.signals, r.dropped)
	for _, s := range []struct {
		name string
		v    []float64
		qs   []float64
	}{
		{"total", r.total, []float64{50, 95, 99, 100}},
		{"system", r.system, []float64{50, 95, 99, 100}},
		{"sig_total", r.sigTotal, []float64{50, 95, 100}},
		{"wait", r.wait, []float64{50}},
		{"service", r.service, []float64{50}},
		{"gateway", r.gateway, []float64{50}},
	} {
		slices.Sort(s.v)
		for _, q := range s.qs {
			name := fmt.Sprintf("p%g", q)
			if q == 100 {
				name = "max"
			}
			fmt.Fprintf(&b, " %s_%s_ms=%s", s.name, name, fmtMs(percentile(s.v, q)))
		}
	}
	return b.String()
}

func fmtMs(v float64) string {
	if math.IsNaN(v) {
		return "NA"
	}
	return strconv.FormatFloat(v, 'f', 1, 64)
}

// percentile is the nearest-rank percentile of sorted v; NaN when v is empty.
func percentile(v []float64, q float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	i := int(math.Ceil(q/100*float64(len(v)))) - 1
	return v[max(0, min(i, len(v)-1))]
}

func analyze(in io.Reader, a args) (result, error) {
	var r result
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for line := 0; sc.Scan(); line++ {
		s := sc.Text()
		if d, ok := strings.CutPrefix(s, "# dropped="); ok {
			r.dropped, _ = parseInt(d)
			continue
		}
		if line == 0 || s == "" || s[0] == '#' {
			continue // header
		}
		f := strings.Split(s, ",")
		if len(f) != 7 {
			return r, fmt.Errorf("line %d: want 7 fields, got %d", line+1, len(f))
		}
		var ws, trigger, emitted, recv int64
		var err error
		for i, p := range []*int64{&ws, &trigger, &emitted, &recv} {
			if *p, err = parseInt(f[[]int{2, 4, 5, 6}[i]]); err != nil {
				return r, fmt.Errorf("line %d: %w", line+1, err)
			}
		}
		if ws < a.fromMs || ws >= a.toMs {
			continue
		}
		end := a.dueMs(ws + windowMs)
		total := float64(recv) - end
		switch f[0] {
		case "bar":
			r.bars++
			r.total = append(r.total, total)
			r.system = append(r.system, float64(recv-trigger))
			r.wait = append(r.wait, float64(trigger)-end)
			r.service = append(r.service, float64(emitted-trigger))
			r.gateway = append(r.gateway, float64(recv-emitted))
		case "signal":
			r.signals++
			r.sigTotal = append(r.sigTotal, total)
		}
	}
	return r, sc.Err()
}
