// Command replayer produces the Parquet buckets written by prep to the ticks
// topic: bucket k goes to partition k, in event order, keyed by symbol.
//
// Each of REPLAY_WORKERS goroutines owns the buckets with bucket % N == worker
// and merges them by event time, so pacing (REPLAY_SPEED) holds across
// buckets. After the last tick of each bucket-day the replayer writes a
// watermark record (empty symbol, ts_ms = midnight after that day) to the
// bucket's partition, which lets the signal service close the day's last
// windows.
//
// REPLAY_FROM=HH:MM[:SS] (Europe/Berlin) starts the replay at that time of the
// first day: earlier ticks are skipped, and a paced replay's clock starts at
// exactly that moment.
//
// REPLAY_THREADS caps the OS threads that run Go code at once (GOMAXPROCS);
// by default the replayer uses every CPU the container has.
package main

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	recordsProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "replayer_records_total", Help: "Records acknowledged by the broker.",
	}, []string{"kind"})
	produceErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "replayer_produce_errors_total", Help: "Records the broker did not acknowledge.",
	})
	produceLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "replayer_produce_latency_seconds", Help: "From Produce to broker acknowledgement.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
	})
	eventTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "replayer_event_time_seconds", Help: "Event time of the latest record handed to the producer.",
	})
	scheduleLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "replayer_schedule_lag_seconds", Help: "How far a paced replay runs behind its schedule.",
	})
)

type config struct {
	brokers     []string
	topic       string
	dir         string
	days        []string
	partitions  int
	workers     int
	speed       float64 // multiple of real time; 0 means as fast as possible
	from        string  // REPLAY_FROM as given, "" for the first tick
	fromMs      int64   // REPLAY_FROM on the first day, epoch ms; 0 if unset
	metricsAddr string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() (config, error) {
	c := config{
		brokers:     strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","),
		topic:       env("TICKS_TOPIC", "ticks"),
		dir:         env("TICKS_DIR", "/data/ticks"),
		days:        strings.Fields(env("DAYS", "081121")),
		metricsAddr: env("METRICS_ADDR", ":9100"),
	}
	var err error
	if c.partitions, err = strconv.Atoi(env("PARTITIONS", "32")); err != nil {
		return c, fmt.Errorf("PARTITIONS: %w", err)
	}
	if c.workers, err = strconv.Atoi(env("REPLAY_WORKERS", "4")); err != nil || c.workers < 1 {
		return c, fmt.Errorf("REPLAY_WORKERS must be a positive integer")
	}
	if s := os.Getenv("REPLAY_THREADS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			return c, fmt.Errorf("REPLAY_THREADS must be a positive integer, got %q", s)
		}
		runtime.GOMAXPROCS(n)
	}
	if s := env("REPLAY_SPEED", "max"); s != "max" {
		if c.speed, err = strconv.ParseFloat(s, 64); err != nil || c.speed <= 0 {
			return c, fmt.Errorf("REPLAY_SPEED must be max or a positive number, got %q", s)
		}
	}
	if len(c.days) == 0 {
		return c, fmt.Errorf("DAYS is empty")
	}
	if c.from = os.Getenv("REPLAY_FROM"); c.from != "" {
		hms := c.from
		if strings.Count(hms, ":") == 1 {
			hms += ":00"
		}
		berlin, err := time.LoadLocation("Europe/Berlin")
		if err != nil {
			return c, err
		}
		t, err := time.ParseInLocation("020106 15:04:05", c.days[0]+" "+hms, berlin)
		if err != nil {
			return c, fmt.Errorf("REPLAY_FROM must be HH:MM or HH:MM:SS, got %q", c.from)
		}
		c.fromMs = t.UnixMilli()
	}
	return c, nil
}

// row is one tick as prep writes it (all columns nullable in DuckDB's output).
type row struct {
	Symbol  string  `parquet:"symbol,optional"`
	SecType string  `parquet:"sectype,optional"`
	Last    float64 `parquet:"last,optional"`
	EventTS int64   `parquet:"event_ts,optional,timestamp(microsecond)"`
	Seq     int64   `parquet:"seq,optional"`
}

// item is a tick, or a watermark when symbol is empty.
type item struct {
	symbol, sectype string
	last            float64
	tsMs            int64
	seq             int64
}

// bucketReader yields one bucket's ticks for every day in order, with a
// watermark after each day.
type bucketReader struct {
	bucket  int
	files   []string
	dayEnds []int64
	fromMs  int64 // ticks before this are skipped

	file   int
	f      *os.File
	r      *parquet.GenericReader[row]
	buf    []row
	n, pos int
	cur    item
}

func newBucketReader(dir string, bucket int, days []string, fromMs int64) (*bucketReader, error) {
	b := &bucketReader{bucket: bucket, buf: make([]row, 8192), file: -1, fromMs: fromMs}
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		return nil, err
	}
	for _, day := range days {
		d, err := time.ParseInLocation("020106", day, berlin)
		if err != nil {
			return nil, fmt.Errorf("day %q is not ddmmyy: %w", day, err)
		}
		path := filepath.Join(dir, fmt.Sprintf("bucket=%02d", bucket), day+".parquet")
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("missing bucket file (run prep for %s): %w", day, err)
		}
		b.files = append(b.files, path)
		b.dayEnds = append(b.dayEnds, d.AddDate(0, 0, 1).UnixMilli())
	}
	return b, nil
}

// next advances cur; it returns false when every day has been read.
func (b *bucketReader) next() (bool, error) {
	for {
		if b.pos < b.n {
			r := &b.buf[b.pos]
			b.pos++
			if r.EventTS/1000 < b.fromMs {
				continue
			}
			b.cur = item{symbol: r.Symbol, sectype: r.SecType, last: r.Last,
				tsMs: r.EventTS / 1000, seq: r.Seq}
			return true, nil
		}
		if b.r != nil {
			n, err := b.r.Read(b.buf)
			b.n, b.pos = n, 0
			if n > 0 {
				continue
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return false, fmt.Errorf("%s: %w", b.files[b.file], err)
			}
			b.r.Close()
			b.f.Close()
			b.r, b.f = nil, nil
			b.cur = item{tsMs: b.dayEnds[b.file]} // this day is done
			return true, nil
		}
		if b.file+1 >= len(b.files) {
			return false, nil
		}
		b.file++
		f, err := os.Open(b.files[b.file])
		if err != nil {
			return false, err
		}
		b.f, b.r = f, parquet.NewGenericReader[row](f)
	}
}

// readerHeap orders bucket readers by the event time of their current item.
type readerHeap []*bucketReader

func (h readerHeap) Len() int { return len(h) }
func (h readerHeap) Less(i, j int) bool {
	if h[i].cur.tsMs != h[j].cur.tsMs {
		return h[i].cur.tsMs < h[j].cur.tsMs
	}
	return h[i].bucket < h[j].bucket
}
func (h readerHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *readerHeap) Push(x any)         { *h = append(*h, x.(*bucketReader)) }
func (h *readerHeap) Pop() any           { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }
func (h readerHeap) peek() *bucketReader { return h[0] }

// appendJSONString appends s as a JSON string. Symbols are plain ASCII, but
// quotes, backslashes and control characters are escaped all the same.
func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			b = append(b, '\\', c)
		case c < 0x20:
			b = append(b, `\u00`...)
			b = append(b, "0123456789abcdef"[c>>4], "0123456789abcdef"[c&0xf])
		default:
			b = append(b, c)
		}
	}
	return append(b, '"')
}

func encode(it *item, sentMs int64) []byte {
	b := make([]byte, 0, 128)
	b = append(b, `{"symbol":`...)
	b = appendJSONString(b, it.symbol)
	b = append(b, `,"sectype":`...)
	b = appendJSONString(b, it.sectype)
	b = append(b, `,"last":`...)
	b = strconv.AppendFloat(b, it.last, 'g', -1, 64)
	b = append(b, `,"ts_ms":`...)
	b = strconv.AppendInt(b, it.tsMs, 10)
	b = append(b, `,"seq":`...)
	b = strconv.AppendInt(b, it.seq, 10)
	b = append(b, `,"sent_ms":`...)
	b = strconv.AppendInt(b, sentMs, 10)
	return append(b, '}')
}

type stats struct {
	ticks, watermarks, errors atomic.Int64
	eventMs                   atomic.Int64 // event time of a recent record, for the progress log
	lastMs                    atomic.Int64 // latest event time handed to the producer
	maxLag                    atomic.Int64 // furthest a paced replay fell behind, in ns
	firstErr                  atomic.Value
}

// storeMax raises v to x if x is larger.
func storeMax(v *atomic.Int64, x int64) {
	for {
		cur := v.Load()
		if x <= cur || v.CompareAndSwap(cur, x) {
			return
		}
	}
}

// run produces everything in h; origin and start anchor the pacing schedule.
func run(ctx context.Context, cl *kgo.Client, cfg config, h readerHeap, origin int64, start time.Time, st *stats) error {
	heap.Init(&h)
	for h.Len() > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		b := h.peek()
		it := b.cur
		if cfg.speed > 0 {
			due := start.Add(time.Duration(float64(it.tsMs-origin) / cfg.speed * float64(time.Millisecond)))
			if wait := time.Until(due); wait > time.Millisecond {
				scheduleLag.Set(0)
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				scheduleLag.Set(-wait.Seconds())
				storeMax(&st.maxLag, int64(-wait))
			}
		}

		kind := "tick"
		if it.symbol == "" {
			kind = "watermark"
		}
		rec := &kgo.Record{
			Topic:     cfg.topic,
			Partition: int32(b.bucket),
			Key:       []byte(it.symbol),
			Value:     encode(&it, time.Now().UnixMilli()),
		}
		sent := time.Now()
		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				if st.errors.Add(1) == 1 {
					st.firstErr.Store(err)
				}
				produceErrors.Inc()
				return
			}
			produceLatency.Observe(time.Since(sent).Seconds())
			recordsProduced.WithLabelValues(kind).Inc()
		})
		if kind == "tick" {
			st.ticks.Add(1)
		} else {
			st.watermarks.Add(1)
		}
		eventTime.Set(float64(it.tsMs) / 1000)
		st.eventMs.Store(it.tsMs)
		storeMax(&st.lastMs, it.tsMs)

		ok, err := b.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println(http.ListenAndServe(cfg.metricsAddr, nil))
	}()

	// Open every bucket and read its first item, which fixes the replay origin
	heaps := make([]readerHeap, cfg.workers)
	origin := int64(1<<63 - 1)
	for k := 0; k < cfg.partitions; k++ {
		b, err := newBucketReader(cfg.dir, k, cfg.days, cfg.fromMs)
		if err != nil {
			log.Fatal(err)
		}
		ok, err := b.next()
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			continue
		}
		origin = min(origin, b.cur.tsMs)
		heaps[k%cfg.workers] = append(heaps[k%cfg.workers], b)
	}
	if cfg.fromMs > 0 {
		origin = cfg.fromMs // the clock starts at REPLAY_FROM, not at the next tick
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.MaxBufferedRecords(1<<20),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	speed := "max"
	if cfg.speed > 0 {
		speed = strconv.FormatFloat(cfg.speed, 'g', -1, 64) + "x"
	}
	startAt := "the first tick"
	if cfg.from != "" {
		startAt = cfg.from + " Europe/Berlin"
	}
	log.Printf("replaying days %s from %s, starting at %s: %d buckets, %d workers, %d threads, speed %s",
		strings.Join(cfg.days, " "), cfg.dir, startAt, cfg.partitions, cfg.workers, runtime.GOMAXPROCS(0), speed)

	var st stats
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, cfg.workers)
	for w := range heaps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := run(ctx, cl, cfg, heaps[w], origin, start, &st); err != nil {
				errs <- fmt.Errorf("worker %d: %w", w, err)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		last := int64(0)
		for {
			select {
			case <-done:
				return
			case <-t.C:
				n := st.ticks.Load()
				log.Printf("%d ticks sent (%.0f/s), event time %s", n, float64(n-last)/10,
					time.UnixMilli(st.eventMs.Load()).Format(time.DateTime))
				last = n
			}
		}
	}()
	wg.Wait()
	close(errs)
	if err := cl.Flush(context.Background()); err != nil {
		log.Printf("flush: %v", err)
	}
	close(done)

	elapsed := time.Since(start)
	n := st.ticks.Load()
	log.Printf("done: %d ticks and %d watermarks in %s (%.0f ticks/s), %d produce errors",
		n, st.watermarks.Load(), elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds(), st.errors.Load())
	var sched, lag time.Duration
	if cfg.speed > 0 {
		// How well a paced replay kept to its schedule; a lag of L seconds put
		// records L × speed seconds of market time behind
		sched = time.Duration(float64(st.lastMs.Load()-origin) / cfg.speed * float64(time.Millisecond))
		lag = time.Duration(st.maxLag.Load())
		log.Printf("schedule %s, max schedule lag %s (%s of market time)",
			sched.Round(time.Millisecond), lag.Round(time.Millisecond),
			time.Duration(float64(lag)*cfg.speed).Round(time.Second))
	}
	// The same in key=value form, for scripts (bench/replay-speeds.sh)
	log.Printf("summary speed=%s ticks=%d watermarks=%d errors=%d took_s=%.3f ticks_per_s=%.0f planned_s=%.3f max_lag_ms=%.1f",
		strings.TrimSuffix(speed, "x"), n, st.watermarks.Load(), st.errors.Load(), elapsed.Seconds(),
		float64(n)/elapsed.Seconds(), sched.Seconds(), float64(lag)/float64(time.Millisecond))
	failed := false
	for err := range errs {
		log.Print(err)
		failed = true
	}
	if e := st.firstErr.Load(); e != nil {
		log.Printf("first produce error: %v", e)
		failed = true
	}
	if failed {
		os.Exit(1)
	}
}
