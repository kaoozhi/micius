// bench/load is a synthetic gRPC load generator for the Micius storage engine.
//
// It generates DataPoints with real wall-clock timestamps — no file, no stale
// data, no deduplication at the memtable. Each goroutine sends AppendRequests
// in a tight loop for the configured duration, then reports latency statistics.
//
// Usage:
//
//	go run . [flags]
//
// Flags:
//
//	--addr        gRPC server address (default localhost:50051)
//	--workers     concurrent gRPC senders (default 50)
//	--batch       DataPoints per AppendRequest (default 100)
//	--duration    how long to run (default 30s)
//	--series      tag cardinality — unique (metric, tags) combinations (default 100000)
//	--rps         target requests/sec across all workers, 0 = unlimited (default 0)
//
// Example — 30-second throughput ceiling run:
//
//	go run . --workers 50 --batch 100 --duration 30s
//
// Example — rate-limited steady-state latency profile:
//
//	go run . --workers 10 --batch 100 --duration 60s --rps 2000
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	storagev1 "github.com/kaoozhi/micius/gen/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ── Tag vocabulary ────────────────────────────────────────────────────────────

var (
	metrics = []string{
		"cpu.load",
		"mem.used_bytes",
		"disk.io_bytes",
		"net.rx_bytes",
		"gc.pause_ms",
	}
	regions = []string{
		"us-east-1",
		"eu-west-1",
		"ap-southeast-1",
	}
	services = []string{
		"api",
		"cache",
		"queue",
		"storage",
		"auth",
	}
)

// seriesVocab is a pre-generated fixed-cardinality set of (metric, tags)
// combinations. Workers pick entries at random so cardinality stays bounded
// while timestamps remain unique (wall clock).
type seriesEntry struct {
	metric string
	tags   map[string]string
}

func buildVocab(n int, rng *rand.Rand) []seriesEntry {
	vocab := make([]seriesEntry, n)
	for i := range vocab {
		region := regions[rng.Intn(len(regions))]
		service := services[rng.Intn(len(services))]
		vocab[i] = seriesEntry{
			metric: metrics[rng.Intn(len(metrics))],
			tags: map[string]string{
				"host":    fmt.Sprintf("%s-%s-%06d", region, service, rng.Intn(1_000_000)),
				"region":  region,
				"service": service,
			},
		}
	}
	return vocab
}

// ── Stats collector ───────────────────────────────────────────────────────────

type stats struct {
	latencies []int64 // nanoseconds, protected by mu
	mu        sync.Mutex
	errors    atomic.Int64
	total     atomic.Int64
}

func (s *stats) record(d time.Duration) {
	s.mu.Lock()
	s.latencies = append(s.latencies, d.Nanoseconds())
	s.mu.Unlock()
	s.total.Add(1)
}

func (s *stats) percentile(p float64) time.Duration {
	s.mu.Lock()
	lats := make([]int64, len(s.latencies))
	copy(lats, s.latencies)
	s.mu.Unlock()
	if len(lats) == 0 {
		return 0
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	idx := int(float64(len(lats)-1) * p / 100)
	return time.Duration(lats[idx])
}

func (s *stats) report(elapsed time.Duration, batchSize int) {
	total := s.total.Load()
	errs := s.errors.Load()
	rps := float64(total) / elapsed.Seconds()
	pps := rps * float64(batchSize)

	fmt.Println()
	fmt.Println("─────────────────────────────────────────────────")
	fmt.Printf("  Duration:       %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Requests:       %d  (errors: %d)\n", total, errs)
	fmt.Printf("  Throughput:     %.0f req/s  →  %.0f pts/s\n", rps, pps)
	fmt.Println()
	fmt.Printf("  Latency P50:    %v\n", s.percentile(50).Round(time.Microsecond))
	fmt.Printf("  Latency P90:    %v\n", s.percentile(90).Round(time.Microsecond))
	fmt.Printf("  Latency P95:    %v\n", s.percentile(95).Round(time.Microsecond))
	fmt.Printf("  Latency P99:    %v\n", s.percentile(99).Round(time.Microsecond))
	fmt.Println("─────────────────────────────────────────────────")
}

// ── Worker ────────────────────────────────────────────────────────────────────

func worker(
	ctx context.Context,
	client storagev1.StorageServiceClient,
	vocab []seriesEntry,
	batchSize int,
	rateLimiter <-chan struct{}, // nil = unlimited
	st *stats,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec

	for {
		// Respect rate limit if configured.
		if rateLimiter != nil {
			select {
			case <-ctx.Done():
				return
			case <-rateLimiter:
			}
		} else {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}

		// Build batch with real wall-clock timestamps.
		// Each request picks batchSize *distinct* series by striding through the
		// vocab from a random start — guarantees no two points share a series key,
		// maximising shard spread for WAL sharding benchmarks.
		points := make([]*storagev1.DataPoint, batchSize)
		now := time.Now().UnixNano()
		vocabStart := rng.Intn(len(vocab))
		for i := range points {
			s := vocab[(vocabStart+i)%len(vocab)]
			points[i] = &storagev1.DataPoint{
				MetricName:  s.metric,
				Tags:        s.tags,
				TimestampNs: now + int64(i), // +i ns to guarantee uniqueness within batch
				Value:       rng.Float64() * 100,
			}
		}

		start := time.Now()
		_, err := client.Append(ctx, &storagev1.AppendRequest{Points: points})
		elapsed := time.Since(start)

		if err != nil {
			st.errors.Add(1)
		} else {
			st.record(elapsed)
		}
	}
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC server address")
	workers := flag.Int("workers", 50, "concurrent gRPC senders")
	batchSize := flag.Int("batch", 100, "DataPoints per AppendRequest")
	duration := flag.Duration("duration", 30*time.Second, "how long to run")
	seriesN := flag.Int("series", 100_000, "unique (metric, tags) cardinality")
	rps := flag.Int("rps", 0, "target requests/sec across all workers (0 = unlimited)")
	flag.Parse()

	if *workers <= 0 || *batchSize <= 0 || *seriesN <= 0 {
		fmt.Fprintln(os.Stderr, "load: --workers, --batch, --series must be > 0")
		os.Exit(1)
	}

	// Pre-generate series vocabulary — shared read-only across all workers.
	vocab := buildVocab(*seriesN, rand.New(rand.NewSource(42))) //nolint:gosec

	// Connect to gRPC server.
	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load: dial %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := storagev1.NewStorageServiceClient(conn)

	// Rate limiter — a token bucket implemented as a buffered channel fed by a ticker.
	var rateLimiter <-chan struct{}
	if *rps > 0 {
		ch := make(chan struct{}, *rps)
		ticker := time.NewTicker(time.Second / time.Duration(*rps))
		go func() {
			for range ticker.C {
				select {
				case ch <- struct{}{}:
				default: // drop token if workers can't keep up
				}
			}
		}()
		rateLimiter = ch
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// Catch Ctrl+C for early termination with stats.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	fmt.Printf("→ load: addr=%s workers=%d batch=%d series=%d duration=%v",
		*addr, *workers, *batchSize, *seriesN, *duration)
	if *rps > 0 {
		fmt.Printf(" rps=%d", *rps)
	}
	fmt.Println()

	st := &stats{}
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(ctx, client, vocab, *batchSize, rateLimiter, st, &wg)
	}

	wg.Wait()
	st.report(time.Since(start), *batchSize)
}
