// Command signal turns ticks into 5-minute bars with EMA(38) and EMA(100) and
// emits BUY/SELL crossovers. Instances share the ticks partitions as one
// consumer group; each keeps the state of its partitions' symbols in memory.
//
// Output for ticks partition p goes to partition p of bars_ema and signals.
// Offsets are committed only after the outputs of a batch are acknowledged.
// When an instance is assigned a partition that has a committed offset C, it
// first rebuilds the partition's state by reading offsets [0, C) without
// emitting, so a rebalance changes neither the EMAs nor the signals.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ticksIn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "signal_ticks_total", Help: "Ticks and watermarks processed (excluding state rebuilds).",
	})
	barsOut = promauto.NewCounter(prometheus.CounterOpts{
		Name: "signal_bars_total", Help: "Bars emitted to bars_ema.",
	})
	signalsOut = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "signal_signals_total", Help: "Crossovers emitted to signals.",
	}, []string{"side"})
	lateTicks = promauto.NewCounter(prometheus.CounterOpts{
		Name: "signal_late_ticks_total", Help: "Ticks dropped because their window had closed.",
	})
	decodeErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "signal_decode_errors_total", Help: "Records that are not valid tick JSON.",
	})
	partitionLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "signal_partition_lag_records", Help: "High watermark minus next offset, per assigned partition.",
	}, []string{"partition"})
	tickLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "signal_tick_latency_seconds", Help: "From the replayer sending a tick to this service processing it.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 18),
	})
	batchSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "signal_batch_seconds", Help: "Process, produce and commit one polled batch.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
	})
	rebuildRecords = promauto.NewCounter(prometheus.CounterOpts{
		Name: "signal_rebuild_records_total", Help: "Records replayed to rebuild state after an assignment.",
	})
	rebuildSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "signal_rebuild_seconds", Help: "Time to rebuild one partition's state.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
	})
	assignedPartitions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "signal_assigned_partitions", Help: "Ticks partitions this instance owns.",
	})
)

type config struct {
	brokers      []string
	group        string
	ticksTopic   string
	barsTopic    string
	signalsTopic string
	firstClose   bool
	pollRecords  int
	metricsAddr  string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() (config, error) {
	c := config{
		brokers:      strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","),
		group:        env("SIGNAL_GROUP", "signal"),
		ticksTopic:   env("TICKS_TOPIC", "ticks"),
		barsTopic:    env("BARS_TOPIC", "bars_ema"),
		signalsTopic: env("SIGNALS_TOPIC", "signals"),
		metricsAddr:  env("METRICS_ADDR", ":9100"),
	}
	switch init := env("EMA_INIT", "first_close"); init {
	case "first_close":
		c.firstClose = true
	case "zero":
	default:
		return c, fmt.Errorf("EMA_INIT must be first_close or zero, got %q", init)
	}
	var err error
	if c.pollRecords, err = strconv.Atoi(env("POLL_RECORDS", "20000")); err != nil || c.pollRecords < 1 {
		return c, fmt.Errorf("POLL_RECORDS must be a positive integer")
	}
	return c, nil
}

// emitter encodes what one partition's windows produce as output records.
type emitter struct {
	cfg       *config
	partition int32
	recs      []*kgo.Record
}

func (e *emitter) bar(b *bar) {
	v := make([]byte, 0, 192)
	v = append(v, `{"symbol":`...)
	v = appendJSONString(v, b.symbol)
	v = append(v, `,"window_start_ms":`...)
	v = strconv.AppendInt(v, b.windowStart, 10)
	v = appendFloatField(v, "open", b.open)
	v = appendFloatField(v, "high", b.high)
	v = appendFloatField(v, "low", b.low)
	v = appendFloatField(v, "close", b.close)
	v = append(v, `,"trades":`...)
	v = strconv.AppendUint(v, uint64(b.trades), 10)
	v = appendFloatField(v, "ema38", b.ema38)
	v = appendFloatField(v, "ema100", b.ema100)
	v = append(v, '}')
	e.recs = append(e.recs, &kgo.Record{Topic: e.cfg.barsTopic, Partition: e.partition, Key: []byte(b.symbol), Value: v})
	barsOut.Inc()
}

func (e *emitter) signal(c *crossover) {
	v := make([]byte, 0, 160)
	v = append(v, `{"symbol":`...)
	v = appendJSONString(v, c.symbol)
	v = append(v, `,"window_start_ms":`...)
	v = strconv.AppendInt(v, c.windowStart, 10)
	v = append(v, `,"side":"`...)
	v = append(v, c.side...)
	v = append(v, '"')
	v = appendFloatField(v, "close", c.close)
	v = appendFloatField(v, "ema38", c.ema38)
	v = appendFloatField(v, "ema100", c.ema100)
	v = append(v, `,"emitted_ms":`...)
	v = strconv.AppendInt(v, time.Now().UnixMilli(), 10)
	v = append(v, '}')
	e.recs = append(e.recs, &kgo.Record{Topic: e.cfg.signalsTopic, Partition: e.partition, Key: []byte(c.symbol), Value: v})
	signalsOut.WithLabelValues(c.side).Inc()
}

// discard drops the output of a state rebuild, which an earlier owner of the
// partition has already emitted.
type discard struct{}

func (discard) bar(*bar)          {}
func (discard) signal(*crossover) {}

// appendFloatField writes the shortest decimal that parses back to f exactly.
func appendFloatField(b []byte, name string, f float64) []byte {
	b = append(b, ',', '"')
	b = append(b, name...)
	b = append(b, '"', ':')
	return strconv.AppendFloat(b, f, 'g', -1, 64)
}

func appendJSONString(b []byte, s string) []byte {
	q, _ := json.Marshal(s)
	return append(b, q...)
}

// owned is a ticks partition this instance has been assigned.
type owned struct {
	*partition
	rebuilt bool // state covers every offset before the first one fetched
}

type app struct {
	cfg config
	cl  *kgo.Client

	mu    sync.Mutex
	parts map[int32]*owned
}

func (a *app) assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range assigned[a.cfg.ticksTopic] {
		a.parts[p] = &owned{partition: newPartition(a.cfg.firstClose)}
	}
	assignedPartitions.Set(float64(len(a.parts)))
	log.Printf("assigned %v, now own %d partitions", assigned[a.cfg.ticksTopic], len(a.parts))
}

func (a *app) revoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range revoked[a.cfg.ticksTopic] {
		delete(a.parts, p)
		partitionLag.DeleteLabelValues(strconv.Itoa(int(p)))
	}
	assignedPartitions.Set(float64(len(a.parts)))
	log.Printf("revoked %v, now own %d partitions", revoked[a.cfg.ticksTopic], len(a.parts))
}

// rebuild replays offsets [0, upTo) of a partition into its state, without
// emitting: the previous owner emitted everything up to its last commit.
func (a *app) rebuild(ctx context.Context, partition int32, st *owned, upTo int64) error {
	if upTo <= 0 {
		return nil
	}
	start := time.Now()
	rc, err := kgo.NewClient(
		kgo.SeedBrokers(a.cfg.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			a.cfg.ticksTopic: {partition: kgo.NewOffset().AtStart()},
		}),
		kgo.FetchMaxPartitionBytes(16<<20),
	)
	if err != nil {
		return err
	}
	defer rc.Close()
	var n int64
	var t tick
	for done := false; !done; {
		fetches := rc.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("rebuild partition %d: %w", partition, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if done || r.Offset >= upTo {
				done = true
				return
			}
			t = tick{}
			if json.Unmarshal(r.Value, &t) == nil {
				st.add(&t, discard{})
			}
			n++
			done = r.Offset == upTo-1
		})
	}
	rebuildRecords.Add(float64(n))
	rebuildSeconds.Observe(time.Since(start).Seconds())
	log.Printf("partition %d: rebuilt state from %d records (offsets 0..%d) in %s",
		partition, n, upTo-1, time.Since(start).Round(time.Millisecond))
	return nil
}

func (a *app) run(ctx context.Context) error {
	var t tick
	for {
		fetches := a.cl.PollRecords(ctx, a.cfg.pollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, p int32, err error) {
			log.Printf("fetch %s/%d: %v", topic, p, err)
		})
		start := time.Now()

		a.mu.Lock()
		var out []*kgo.Record
		var err error
		fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
			if err != nil || len(fp.Records) == 0 {
				return
			}
			st := a.parts[fp.Partition]
			if st == nil {
				err = fmt.Errorf("records for partition %d, which is not assigned", fp.Partition)
				return
			}
			if !st.rebuilt {
				if err = a.rebuild(ctx, fp.Partition, st, fp.Records[0].Offset); err != nil {
					return
				}
				st.rebuilt = true
			}
			em := &emitter{cfg: &a.cfg, partition: fp.Partition}
			late := st.late
			nowMs := time.Now().UnixMilli()
			for _, r := range fp.Records {
				t = tick{}
				if json.Unmarshal(r.Value, &t) != nil {
					decodeErrors.Inc()
					continue
				}
				st.add(&t, em)
				if t.SentMs > 0 {
					tickLatency.Observe(float64(nowMs-t.SentMs) / 1000)
				}
			}
			ticksIn.Add(float64(len(fp.Records)))
			lateTicks.Add(float64(st.late - late))
			last := fp.Records[len(fp.Records)-1].Offset
			partitionLag.WithLabelValues(strconv.Itoa(int(fp.Partition))).Set(float64(fp.HighWatermark - last - 1))
			out = append(out, em.recs...)
		})
		a.mu.Unlock()
		if err != nil {
			return err
		}

		// Outputs first, then offsets: a crash in between replays the batch,
		// and ClickHouse's ReplacingMergeTree drops the duplicates
		if len(out) > 0 {
			for _, r := range a.cl.ProduceSync(ctx, out...) {
				if r.Err != nil {
					return fmt.Errorf("produce: %w", r.Err)
				}
			}
		}
		if err := a.cl.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		batchSeconds.Observe(time.Since(start).Seconds())
		a.cl.AllowRebalance()
	}
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
		http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
		log.Println(http.ListenAndServe(cfg.metricsAddr, nil))
	}()

	a := &app{cfg: cfg, parts: make(map[int32]*owned)}
	a.cl, err = kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ConsumerGroup(cfg.group),
		kgo.ConsumeTopics(cfg.ticksTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(a.assigned),
		kgo.OnPartitionsRevoked(a.revoked),
		kgo.OnPartitionsLost(a.revoked),
		// A rebuild runs inside a poll iteration, so allow it time
		kgo.RebalanceTimeout(5*time.Minute),
		kgo.FetchMaxPartitionBytes(4<<20),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("consuming %s as group %s, EMA_INIT first_close=%v", cfg.ticksTopic, cfg.group, cfg.firstClose)

	err = a.run(ctx)
	// Leave the group cleanly so the partitions move without waiting for a timeout
	a.cl.AllowRebalance()
	a.cl.Close()
	if err != nil {
		log.Fatal(err)
	}
}
