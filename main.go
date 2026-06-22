package main

import (
	"container/list"
	"context"
	"errors"
	"flag"
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
	mu sync.Mutex
	qs map[string]*queue
}

func newBroker() *broker { return &broker{qs: map[string]*queue{}} }

func (b *broker) queue(name string) *queue {
	q, ok := b.qs[name]
	if !ok {
		q = &queue{msgs: list.New(), waiters: list.New()}
		b.qs[name] = q
	}
	return q
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queue(name)
	if e := q.waiters.Front(); e != nil {
		w := e.Value.(chan string)
		q.waiters.Remove(e)
		w <- msg
		return
	}
	q.msgs.PushBack(msg)
}

func (b *broker) take(ctx context.Context, name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	if q := b.qs[name]; q != nil {
		if e := q.msgs.Front(); e != nil {
			msg := e.Value.(string)
			q.msgs.Remove(e)
			b.mu.Unlock()
			return msg, true
		}
	}
	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}
	q := b.queue(name)
	w := make(chan string, 1)
	we := q.waiters.PushBack(w)
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg := <-w:
		return msg, true
	case <-timer.C:
	case <-ctx.Done():
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case msg := <-w:
		return msg, true
	default:
		q.waiters.Remove(we)
		return "", false
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
		b.put(name, v)
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
		msg, ok := b.take(r.Context(), name, timeout)
		wait.Observe(time.Since(start).Seconds())
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
	flag.Parse()
	zerolog.TimeFieldFormat = time.RFC3339Nano

	b := newBroker()
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
		log.Info().Int("port", *port).Msg("listening")
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
