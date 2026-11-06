// Command gateway pushes live bars and signals to browsers over WebSocket and
// serves their history from ClickHouse.
//
//	GET /ws              WebSocket; send {"subscribe":["SYM",...]} to receive
//	                     that symbol's bars. Signals and stats go to everyone.
//	GET /history?symbol= bars_ema and signals of one symbol (columnar JSON)
//	GET /signals?limit=  most recent signals (columnar JSON)
//	GET /symbols         symbols with bars, signal counts and last close
//	GET /metrics         Prometheus
//
// Every instance reads bars_ema and signals with its own consumer group
// (gateway-$HOSTNAME), so each sees every symbol and any instance can serve
// any client. It never commits: a (re)started instance begins at the live end,
// and clients load what came before from ClickHouse.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	recordsIn = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_records_total", Help: "Records consumed, by topic.",
	}, []string{"topic"})
	wsClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_ws_clients", Help: "Open WebSocket connections.",
	})
	wsFrames = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_ws_frames_total", Help: "WebSocket frames written.",
	})
	wsBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_ws_bytes_total", Help: "WebSocket payload bytes written.",
	})
	wsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_ws_dropped_total", Help: "Messages dropped for clients that fell behind.",
	})
	signalLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "gateway_signal_latency_seconds", Help: "From the signal service emitting a signal to the gateway receiving it.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 18),
	})
	e2eLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "gateway_e2e_latency_seconds", Help: "From the closing tick sent to the gateway receiving the signal.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 18),
	})
	historySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "gateway_http_seconds", Help: "HTTP endpoint latency, ClickHouse included.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"endpoint"})
)

type config struct {
	brokers       []string
	group         string
	barsTopic     string
	signalsTopic  string
	chURL         string
	chUser        string
	chPassword    string
	listen        string
	flushInterval time.Duration
	origins       []string
	instance      string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() (config, error) {
	host, _ := os.Hostname()
	c := config{
		brokers:      strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","),
		group:        env("GATEWAY_GROUP", "gateway-"+host),
		barsTopic:    env("BARS_TOPIC", "bars_ema"),
		signalsTopic: env("SIGNALS_TOPIC", "signals"),
		chURL:        env("CLICKHOUSE_URL", "http://localhost:8123"),
		chUser:       env("CLICKHOUSE_USER", "default"),
		chPassword:   os.Getenv("CLICKHOUSE_PASSWORD"),
		listen:       env("LISTEN_ADDR", ":8000"),
		origins:      strings.Split(env("ALLOWED_ORIGINS", "localhost:*,127.0.0.1:*"), ","),
		instance:     host,
	}
	ms, err := strconv.Atoi(env("FLUSH_MS", "250"))
	if err != nil || ms < 1 {
		return c, fmt.Errorf("FLUSH_MS must be a positive integer")
	}
	c.flushInterval = time.Duration(ms) * time.Millisecond
	// Set once here, before any client connects (hub.go)
	for _, l := range []struct {
		name string
		v    *int
	}{{"WS_MAX_PENDING_BARS", &maxPendingBars}, {"WS_MAX_PENDING_SIGNALS", &maxPendingSignals}} {
		if s := os.Getenv(l.name); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 1 {
				return c, fmt.Errorf("%s must be a positive integer", l.name)
			}
			*l.v = n
		}
	}
	return c, nil
}

type gateway struct {
	cfg config
	hub *hub
	ch  *http.Client

	bars, signals   atomic.Int64 // since the last stats tick
	windowMs        atomic.Int64 // latest window start seen
	latencyMaxUs    atomic.Int64 // largest signal latency since the last stats tick
	e2eLatencyMaxUs atomic.Int64 // largest end-to-end signal latency since the last stats tick
	ready           atomic.Bool  // partitions assigned: live records will arrive
}

// consume reads both topics from the end and fans records out to the hub.
func (g *gateway) consume(ctx context.Context) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(g.cfg.brokers...),
		kgo.ConsumerGroup(g.cfg.group),
		kgo.ConsumeTopics(g.cfg.barsTopic, g.cfg.signalsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		// A restarted instance rejoins its group; a short session lets the
		// group drop a member that died without leaving
		kgo.SessionTimeout(10*time.Second),
		kgo.HeartbeatInterval(2*time.Second),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { g.ready.Store(true) }),
		kgo.OnPartitionsRevoked(func(context.Context, *kgo.Client, map[string][]int32) { g.ready.Store(false) }),
		kgo.OnPartitionsLost(func(context.Context, *kgo.Client, map[string][]int32) { g.ready.Store(false) }),
	)
	if err != nil {
		return err
	}
	// Leaving the group on the way out lets a restarted instance join at once
	defer cl.Close()
	log.Printf("consuming %s and %s as group %s", g.cfg.barsTopic, g.cfg.signalsTopic, g.cfg.group)

	var head struct {
		Symbol        string `json:"symbol"`
		WindowStartMs int64  `json:"window_start_ms"`
		TriggerSentMs int64  `json:"trigger_sent_ms"`
		EmittedMs     int64  `json:"emitted_ms"`
	}
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) { log.Printf("fetch %s/%d: %v", t, p, err) })
		now := time.Now().UnixMilli()
		fetches.EachRecord(func(r *kgo.Record) {
			head.Symbol, head.WindowStartMs, head.TriggerSentMs, head.EmittedMs = "", 0, 0, 0
			if json.Unmarshal(r.Value, &head) != nil {
				return
			}
			recordsIn.WithLabelValues(r.Topic).Inc()
			if head.WindowStartMs > g.windowMs.Load() {
				g.windowMs.Store(head.WindowStartMs)
			}
			if r.Topic == g.cfg.barsTopic {
				g.bars.Add(1)
				g.hub.bar(head.Symbol, r.Value)
				return
			}
			g.signals.Add(1)
			if head.EmittedMs > 0 {
				lat := now - head.EmittedMs
				if lat > 0 {
					signalLatency.Observe(float64(lat) / 1000)
					if us := lat * 1000; us > g.latencyMaxUs.Load() {
						g.latencyMaxUs.Store(us)
					}
				}
			}
			if head.TriggerSentMs > 0 {
				e2e := now - head.TriggerSentMs
				if e2e > 0 {
					e2eLatency.Observe(float64(e2e) / 1000)
					if us := e2e * 1000; us > g.e2eLatencyMaxUs.Load() {
						g.e2eLatencyMaxUs.Store(us)
					}
				}
			}
			g.hub.signal(r.Value)
		})
	}
}

// statsLoop sends every client a summary once a second.
func (g *gateway) statsLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			secs := now.Sub(last).Seconds()
			last = now
			msg, _ := json.Marshal(map[string]any{
				"t_ms":           now.UnixMilli(),
				"bars_per_s":     math.Round(float64(g.bars.Swap(0)) / secs),
				"signals_per_s":  math.Round(float64(g.signals.Swap(0)) / secs),
				"window_ms":      g.windowMs.Load(),
				"latency_ms":     float64(g.latencyMaxUs.Swap(0)) / 1000,
				"e2e_latency_ms": float64(g.e2eLatencyMaxUs.Swap(0)) / 1000,
				"instance":       g.cfg.instance,
			})
			g.hub.stats(msg)
		}
	}
}

func (g *gateway) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: g.cfg.origins})
	if err != nil {
		return
	}
	// The largest message a client sends is its subscription. One to every
	// symbol takes about 15 bytes per symbol: 1.4 MB at REPLAY_COPIES=20, so
	// 16 MiB leaves room for about a million symbols
	conn.SetReadLimit(16 << 20)
	c := &client{conn: conn, symbols: map[string]bool{}, notified: make(chan struct{}, 1)}
	g.hub.add(c)
	defer g.hub.remove(c)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go func() {
		defer cancel()
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var msg struct {
				Subscribe []string `json:"subscribe"`
			}
			if json.Unmarshal(data, &msg) == nil && msg.Subscribe != nil {
				g.hub.subscribe(c, msg.Subscribe)
			}
		}
	}()
	err = g.hub.writeLoop(ctx, c, g.cfg.flushInterval)
	if errors.Is(err, context.Canceled) {
		conn.Close(websocket.StatusNormalClosure, "")
	} else {
		conn.CloseNow()
	}
}

// query runs sql on ClickHouse with {name:Type} parameters and copies the
// result to w.
func (g *gateway) query(ctx context.Context, w io.Writer, sql string, params map[string]string) error {
	u, err := url.Parse(g.cfg.chURL)
	if err != nil {
		return err
	}
	q := u.Query()
	for k, v := range params {
		q.Set("param_"+k, v)
	}
	q.Set("output_format_json_quote_64bit_integers", "0")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(sql))
	if err != nil {
		return err
	}
	req.SetBasicAuth(g.cfg.chUser, g.cfg.chPassword)
	resp, err := g.ch.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

// queryInto buffers the result, so an error can still become a 502.
func (g *gateway) queryInto(ctx context.Context, sql string, params map[string]string) ([]byte, error) {
	var b strings.Builder
	if err := g.query(ctx, &b, sql, params); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSpace(b.String())), nil
}

func (g *gateway) handle(name string, h func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if err := h(w, r); err != nil {
			log.Printf("%s: %v", name, err)
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
		historySeconds.WithLabelValues(name).Observe(time.Since(start).Seconds())
	}
}

const barsSQL = `
SELECT toUnixTimestamp(window_start) * 1000 AS t, open, high, low, close, trades, ema38, ema100
FROM bars_ema FINAL
WHERE symbol = {symbol:String}
  AND window_start >= toDateTime(intDiv({from:Int64}, 1000)) AND window_start < toDateTime(intDiv({to:Int64}, 1000))
ORDER BY window_start
FORMAT JSONColumns`

const symbolSignalsSQL = `
SELECT toUnixTimestamp(window_start) * 1000 AS window_start_ms, toString(side) AS side, close, ema38, ema100
FROM signals FINAL
WHERE symbol = {symbol:String}
  AND window_start >= toDateTime(intDiv({from:Int64}, 1000)) AND window_start < toDateTime(intDiv({to:Int64}, 1000))
ORDER BY window_start
FORMAT JSONColumns`

func (g *gateway) history(w http.ResponseWriter, r *http.Request) error {
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		http.Error(w, "symbol is required", http.StatusBadRequest)
		return nil
	}
	params := map[string]string{"symbol": symbol, "from": "0", "to": strconv.FormatInt(math.MaxInt32*1000, 10)}
	for _, k := range []string{"from", "to"} {
		if v := r.URL.Query().Get(k); v != "" {
			if _, err := strconv.ParseInt(v, 10, 64); err != nil {
				http.Error(w, k+" must be epoch milliseconds", http.StatusBadRequest)
				return nil
			}
			params[k] = v
		}
	}
	bars, err := g.queryInto(r.Context(), barsSQL, params)
	if err != nil {
		return err
	}
	sigs, err := g.queryInto(r.Context(), symbolSignalsSQL, params)
	if err != nil {
		return err
	}
	name, _ := json.Marshal(symbol)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"symbol":%s,"bars":%s,"signals":%s}`, name, bars, sigs)
	return nil
}

const recentSignalsSQL = `
SELECT symbol, toUnixTimestamp(window_start) * 1000 AS window_start_ms, toString(side) AS side,
       close, ema38, ema100, toUnixTimestamp64Milli(emitted_at) AS emitted_ms
FROM signals FINAL
ORDER BY window_start DESC, symbol
LIMIT {limit:UInt32}
FORMAT JSONColumns`

func (g *gateway) recentSignals(w http.ResponseWriter, r *http.Request) error {
	limit := r.URL.Query().Get("limit")
	if n, err := strconv.Atoi(limit); err != nil || n < 1 || n > 100000 {
		limit = "2000"
	}
	w.Header().Set("Content-Type", "application/json")
	return g.query(r.Context(), w, recentSignalsSQL, map[string]string{"limit": limit})
}

// Most traded first. A copy from REPLAY_COPIES (c7:SYMBOL) takes its
// original's row of instruments.
const symbolsSQL = `
SELECT b.symbol AS symbol, i.sectype AS sectype, i.exchange AS exchange, b.bars AS bars, b.trades AS trades,
       b.last_close AS last_close, b.last_ms AS last_ms, s.signals AS signals
FROM (SELECT symbol, replaceRegexpOne(symbol, '^c[0-9]+:', '') AS base, count() AS bars, sum(trades) AS trades,
             argMax(close, window_start) AS last_close, toUnixTimestamp(max(window_start)) * 1000 AS last_ms
      FROM bars_ema FINAL GROUP BY symbol) AS b
LEFT JOIN (SELECT symbol, sectype, exchange FROM instruments FINAL) AS i ON i.symbol = b.base
LEFT JOIN (SELECT symbol, uniqExact(window_start) AS signals FROM signals GROUP BY symbol) AS s ON s.symbol = b.symbol
ORDER BY trades DESC, symbol
FORMAT JSONColumns`

func (g *gateway) symbols(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "application/json")
	return g.query(r.Context(), w, symbolsSQL, nil)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g := &gateway{cfg: cfg, hub: newHub(), ch: &http.Client{Timeout: 30 * time.Second}}
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		if err := g.consume(ctx); err != nil {
			log.Fatal(err)
		}
	}()
	go g.statsLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", g.serveWS)
	mux.Handle("GET /history", g.handle("history", g.history))
	mux.Handle("GET /signals", g.handle("signals", g.recentSignals))
	mux.Handle("GET /symbols", g.handle("symbols", g.symbols))
	mux.Handle("GET /metrics", promhttp.Handler())
	// Healthy once partitions are assigned, so a client that waits for it sees
	// everything produced afterwards
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !g.ready.Load() {
			http.Error(w, "joining consumer group", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})

	srv := &http.Server{Addr: cfg.listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	log.Printf("listening on %s", cfg.listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-consumed
}
