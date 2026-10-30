// Command probe measures what a UI client sees, for bench/realtime.sh.
//
//	probe listen  connects to the gateway's WebSocket like the UI, subscribes
//	              to every symbol in SYMBOLS_FILE and appends one CSV line per
//	              bar and signal it receives, stamped on arrival
//	probe analyze reads that CSV and the replayer's clock line and prints the
//	              latencies of the windows in a range as one key=value line
//
// The probe runs in a container next to the replayer, the signal service and
// the gateway, so every timestamp it compares comes from the same clock.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		log.Fatal("usage: probe listen | probe analyze key=value ...")
	}
	var err error
	switch os.Args[1] {
	case "listen":
		err = listen(env("GATEWAY_WS", "ws://gateway:8000/ws"), env("SYMBOLS_FILE", ""), env("OUT", ""))
	case "analyze":
		var a args
		if a, err = parseArgs(os.Args[2:]); err == nil {
			var f *os.File
			if f, err = os.Open(a.in); err == nil {
				var r result
				if r, err = analyze(f, a); err == nil {
					fmt.Println(r.String())
				}
				f.Close()
			}
		}
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

// message is the part of a bar or signal the probe records.
type message struct {
	Symbol        string `json:"symbol"`
	WindowStartMs int64  `json:"window_start_ms"`
	Side          string `json:"side"`
	TriggerSentMs int64  `json:"trigger_sent_ms"`
	EmittedMs     int64  `json:"emitted_ms"`
}

type frame struct {
	Bars    []message `json:"bars"`
	Signals []message `json:"signals"`
	Dropped int64     `json:"dropped"`
}

// listen records until SIGTERM. It writes OUT.ready once subscribed, and a
// final "# dropped=N" line with the gateway's count of messages it dropped.
func listen(url, symbolsFile, out string) error {
	if symbolsFile == "" || out == "" {
		return fmt.Errorf("listen needs SYMBOLS_FILE and OUT")
	}
	symbols, err := os.ReadFile(symbolsFile)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 20)
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"subscribe":`+string(symbols)+`}`)); err != nil {
		return err
	}

	f, err := os.Create(out)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	fmt.Fprintln(w, "kind,symbol,window_start_ms,side,trigger_sent_ms,emitted_ms,recv_ms")
	if err := os.WriteFile(out+".ready", nil, 0o644); err != nil {
		return err
	}
	log.Printf("listening on %s for %d bytes of symbols", url, len(symbols))

	var dropped int64
	for {
		_, data, err := conn.Read(ctx)
		recv := time.Now().UnixMilli()
		if err != nil {
			break // SIGTERM, or the gateway went away
		}
		var fr frame
		if json.Unmarshal(data, &fr) != nil {
			continue
		}
		dropped = max(dropped, fr.Dropped)
		for _, m := range fr.Bars {
			fmt.Fprintf(w, "bar,%s,%d,,%d,%d,%d\n", m.Symbol, m.WindowStartMs, m.TriggerSentMs, m.EmittedMs, recv)
		}
		for _, m := range fr.Signals {
			fmt.Fprintf(w, "signal,%s,%d,%s,%d,%d,%d\n", m.Symbol, m.WindowStartMs, m.Side, m.TriggerSentMs, m.EmittedMs, recv)
		}
	}
	fmt.Fprintf(w, "# dropped=%d\n", dropped)
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Close()
}

func parseInt(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
