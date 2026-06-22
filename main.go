package main

import (
	"bufio"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	puts = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "broker_put_total", Help: "Messages enqueued."},
		[]string{"queue"},
	)
	gets = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "broker_get_total", Help: "GET requests by result."},
		[]string{"queue", "result"},
	)
	wait = prometheus.NewHistogram(
		prometheus.HistogramOpts{Name: "broker_get_wait_seconds", Help: "GET wait time.",
			Buckets: prometheus.DefBuckets},
	)
)

func init() { prometheus.MustRegister(puts, gets, wait) }

type queue struct {
	msgs    *list.List // FIFO of string
	waiters *list.List // FIFO of chan string (buffered 1)
}

type broker struct {
	mu  sync.Mutex
	qs  map[string]*queue
	log *os.File // append-only WAL; nil when persistence disabled
}

// JSON-lines WAL: one record per line, fsync after each.
type record struct {
	Op string `json:"op"`          // "P" = put, "G" = consumed from head
	Q  string `json:"q"`           // queue name
	V  string `json:"v,omitempty"` // payload (P only)
}

func newBroker(path string) (*broker, error) {
	b := &broker{qs: map[string]*queue{}}
	if path == "" {
		return b, nil
	}
	if err := b.replay(path); err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	b.log = f
	return b, nil
}

func (b *broker) close() error {
	if b.log == nil {
		return nil
	}
	return b.log.Close()
}

func (b *broker) replay(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for s.Scan() {
		var r record
		if err := json.Unmarshal(s.Bytes(), &r); err != nil {
			return err
		}
		q := b.queue(r.Q)
		switch r.Op {
		case "P":
			q.msgs.PushBack(r.V)
		case "G":
			if e := q.msgs.Front(); e != nil {
				q.msgs.Remove(e)
			}
		default:
			return fmt.Errorf("unknown op %q", r.Op)
		}
	}
	return s.Err()
}

// appendLog must be called with b.mu held. Returns error if fsync fails.
func (b *broker) appendLog(op, q, v string) error {
	if b.log == nil {
		return nil
	}
	data, _ := json.Marshal(record{Op: op, Q: q, V: v})
	if _, err := b.log.Write(append(data, '\n')); err != nil {
		return err
	}
	return b.log.Sync()
}

func (b *broker) queue(name string) *queue {
	q, ok := b.qs[name]
	if !ok {
		q = &queue{msgs: list.New(), waiters: list.New()}
		b.qs[name] = q
	}
	return q
}

func (b *broker) put(name, msg string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.appendLog("P", name, msg); err != nil {
		return err
	}
	q := b.queue(name)
	if e := q.waiters.Front(); e != nil {
		w := e.Value.(chan string)
		q.waiters.Remove(e)
		w <- msg
		return nil
	}
	q.msgs.PushBack(msg)
	return nil
}

func (b *broker) take(ctx context.Context, name string, timeout time.Duration) (string, bool, error) {
	b.mu.Lock()
	if q := b.qs[name]; q != nil {
		if e := q.msgs.Front(); e != nil {
			msg := e.Value.(string)
			if err := b.appendLog("G", name, ""); err != nil {
				b.mu.Unlock()
				return "", false, err
			}
			q.msgs.Remove(e)
			b.mu.Unlock()
			return msg, true, nil
		}
	}
	if timeout <= 0 {
		b.mu.Unlock()
		return "", false, nil
	}
	q := b.queue(name)
	w := make(chan string, 1)
	we := q.waiters.PushBack(w)
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg := <-w:
		b.mu.Lock()
		err := b.appendLog("G", name, "")
		b.mu.Unlock()
		return msg, true, err
	case <-timer.C:
	case <-ctx.Done():
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case msg := <-w:
		if err := b.appendLog("G", name, ""); err != nil {
			return "", false, err
		}
		return msg, true, nil
	default:
		q.waiters.Remove(we)
		return "", false, nil
	}
}

func (b *broker) handle(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" || strings.Contains(name, "/") {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPut:
		v := r.URL.Query().Get("v")
		if v == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := b.put(name, v); err != nil {
			log.Error().Err(err).Str("queue", name).Msg("put")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		puts.WithLabelValues(name).Inc()
		log.Info().Str("queue", name).Str("msg", v).Msg("put")
	case http.MethodGet:
		var timeout time.Duration
		if s := r.URL.Query().Get("timeout"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			timeout = time.Duration(n) * time.Second
		}
		start := time.Now()
		msg, ok, err := b.take(r.Context(), name, timeout)
		wait.Observe(time.Since(start).Seconds())
		if err != nil {
			log.Error().Err(err).Str("queue", name).Msg("get")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !ok {
			gets.WithLabelValues(name, "miss").Inc()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gets.WithLabelValues(name, "hit").Inc()
		log.Info().Str("queue", name).Str("msg", msg).Msg("get")
		_, _ = w.Write([]byte(msg))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	port := flag.Int("port", 8080, "TCP port to listen on")
	data := flag.String("data", "", "persistence log file path (empty disables persistence)")
	flag.Parse()
	zerolog.TimeFieldFormat = time.RFC3339Nano

	b, err := newBroker(*data)
	if err != nil {
		log.Fatal().Err(err).Msg("init")
	}
	defer b.close()

	mux := http.NewServeMux()
	mux.Handle("/debug/metrics", promhttp.Handler())
	mux.HandleFunc("/", b.handle)

	base, stop := context.WithCancel(context.Background())
	srv := &http.Server{
		Addr:        ":" + strconv.Itoa(*port),
		Handler:     mux,
		BaseContext: func(net.Listener) context.Context { return base },
	}

	go func() {
		log.Info().Int("port", *port).Str("data", *data).Msg("listening")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("listen")
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
