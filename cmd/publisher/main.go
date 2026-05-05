// Publisher CLI: generates fake push notifications and publishes them to Kafka.
//
// The producer config (single vs async, linger, compression, acks, ...) lives
// in a YAML profile under configs/. The CLI flags are limited to runtime
// concerns (--profile, --count, --rate, --report) so two runs with the same
// profile are directly comparable.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/quangdangfit/kafkaforge/internal/config"
	"github.com/quangdangfit/kafkaforge/internal/gen"
	"github.com/quangdangfit/kafkaforge/internal/metrics"
)

const sentAtHeader = "sent_at_ns"

type runFlags struct {
	profilePath string
	producers   int
	pprofAddr   string
	count       int64
	duration    time.Duration
	rate        int
	users       int64
	report      time.Duration
}

func parseFlags() runFlags {
	var f runFlags
	flag.StringVar(&f.profilePath, "profile", "configs/baseline.yaml", "path to YAML tuning profile")
	flag.IntVar(&f.producers, "producers", 1, "number of concurrent producer goroutines (share one kgo.Client)")
	flag.Int64Var(&f.count, "count", 0, "stop after N messages across all producers (0 = no limit)")
	flag.DurationVar(&f.duration, "duration", 0, "stop after this much wall-clock time (0 = no limit)")
	flag.IntVar(&f.rate, "rate", 0, "global max messages per second across all producers (0 = unlimited)")
	flag.Int64Var(&f.users, "users", 1_000_000, "size of the synthetic user space")
	flag.DurationVar(&f.report, "report", time.Second, "interval between metric log lines")
	flag.StringVar(&f.pprofAddr, "pprof", "", "if non-empty, serve net/http/pprof on this address (e.g. :6060)")
	flag.Parse()
	if f.producers < 1 {
		f.producers = 1
	}
	return f
}

func main() {
	flags := parseFlags()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err := run(flags); err != nil {
		slog.Error("publisher failed", "err", err)
		os.Exit(1)
	}
}

func run(flags runFlags) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if flags.pprofAddr != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		go func() {
			slog.Info("pprof", "addr", flags.pprofAddr)
			if err := http.ListenAndServe(flags.pprofAddr, nil); err != nil {
				slog.Warn("pprof server", "err", err)
			}
		}()
	}
	if flags.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, flags.duration)
		defer cancel()
	}

	profile, err := config.Load(flags.profilePath)
	if err != nil {
		return err
	}

	cl, err := newClient(profile)
	if err != nil {
		return err
	}
	defer cl.Close()

	if err := cl.Ping(ctx); err != nil {
		return fmt.Errorf("ping brokers: %w", err)
	}
	slog.Info("connected",
		"brokers", profile.Brokers,
		"topic", profile.Topic,
		"mode", profile.Producer.Mode,
		"acks", profile.Producer.Acks,
		"compression", profile.Producer.Compression,
		"linger", profile.Producer.Linger.String(),
		"producers", flags.producers,
	)

	rec := metrics.New()
	rep := metrics.NewReporter(rec)

	go func() {
		t := time.NewTicker(flags.report)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				slog.Info(rep.Tick("publish"))
			}
		}
	}()

	produce := chooseProducer(cl, profile, rec)

	var inflight sync.WaitGroup // tracks async produce callbacks
	var produced atomic.Int64   // shared count across producer goroutines

	// When --rate is set we spread it evenly across producers; the "+ N - 1"
	// rounds up so the global rate is respected even with awkward divisions.
	var perWorkerInterval time.Duration
	if flags.rate > 0 {
		perWorker := flags.rate / flags.producers
		if perWorker < 1 {
			perWorker = 1
		}
		perWorkerInterval = time.Second / time.Duration(perWorker)
	}

	var workers sync.WaitGroup
	for i := 0; i < flags.producers; i++ {
		workers.Add(1)
		// Each goroutine gets its own Generator so there is no shared rand state.
		seed := uint64(time.Now().UnixNano()) ^ uint64(i+1)*0x9E3779B97F4A7C15
		go func(workerID int, seed uint64) {
			defer workers.Done()
			g := gen.NewGenerator(seed, flags.users)
			var ticker *time.Ticker
			if perWorkerInterval > 0 {
				ticker = time.NewTicker(perWorkerInterval)
				defer ticker.Stop()
			}
			for {
				if ctx.Err() != nil {
					return
				}
				if flags.count > 0 && produced.Load() >= flags.count {
					return
				}
				if ticker != nil {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}
				}

				n := g.Next()
				key, val, err := n.Marshal()
				if err != nil {
					slog.Warn("marshal", "err", err, "worker", workerID)
					continue
				}
				hdr := []kgo.RecordHeader{{Key: sentAtHeader, Value: nanosToBytes(n.SentAt.UnixNano())}}
				r := &kgo.Record{Topic: profile.Topic, Key: key, Value: val, Headers: hdr}

				produce(ctx, r, &inflight)
				produced.Add(1)
			}
		}(i, seed)
	}

	workers.Wait()
	inflight.Wait()
	if err := cl.Flush(context.Background()); err != nil {
		slog.Warn("flush", "err", err)
	}
	fmt.Print(rec.Summary("publisher"))
	return nil
}

// chooseProducer returns either a synchronous (one round-trip per record) or
// asynchronous (callback-driven, batching) produce function based on profile.
func chooseProducer(cl *kgo.Client, p *config.Profile, rec *metrics.Recorder) func(context.Context, *kgo.Record, *sync.WaitGroup) {
	// Log only the first few produce errors so a misconfigured cluster doesn't
	// flood the log with millions of identical lines.
	var logged atomic.Int32
	const maxLogged = 5
	report := func(err error) {
		if logged.Load() < maxLogged && logged.Add(1) <= maxLogged {
			slog.Warn("produce error", "err", err)
		}
	}

	switch strings.ToLower(p.Producer.Mode) {
	case "single":
		return func(ctx context.Context, r *kgo.Record, _ *sync.WaitGroup) {
			start := time.Now()
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				rec.Fail()
				report(err)
				return
			}
			rec.Observe(time.Since(start))
			rec.Inc(len(r.Key) + len(r.Value))
		}
	default:
		return func(ctx context.Context, r *kgo.Record, wg *sync.WaitGroup) {
			wg.Add(1)
			start := time.Now()
			cl.Produce(ctx, r, func(rr *kgo.Record, err error) {
				defer wg.Done()
				if err != nil {
					rec.Fail()
					report(err)
					return
				}
				rec.Observe(time.Since(start))
				rec.Inc(len(rr.Key) + len(rr.Value))
			})
		}
	}
}

func newClient(p *config.Profile) (*kgo.Client, error) {
	acks, err := acksFromString(p.Producer.Acks)
	if err != nil {
		return nil, err
	}
	codec, err := compressionFromString(p.Producer.Compression)
	if err != nil {
		return nil, err
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(p.Brokers, ",")...),
		kgo.DefaultProduceTopic(p.Topic),
		kgo.ClientID("kafkaforge-publisher"),
		kgo.RequiredAcks(acks),
		kgo.ProducerBatchCompression(codec...),
	}

	if strings.ToLower(p.Producer.Mode) == "single" {
		// Force one record per batch so each Produce really is a round-trip.
		opts = append(opts, kgo.MaxBufferedRecords(1))
	} else {
		if p.Producer.Linger > 0 {
			opts = append(opts, kgo.ProducerLinger(p.Producer.Linger))
		}
		if p.Producer.BatchMaxBytes > 0 {
			opts = append(opts, kgo.ProducerBatchMaxBytes(int32(p.Producer.BatchMaxBytes)))
		}
		if p.Producer.MaxBuffered > 0 {
			opts = append(opts, kgo.MaxBufferedRecords(p.Producer.MaxBuffered))
		}
	}

	if !p.Producer.Idempotent {
		// franz-go enables idempotency by default; turning it off lets us
		// benchmark acks=0/1 paths cleanly.
		if acks != kgo.AllISRAcks() {
			opts = append(opts, kgo.DisableIdempotentWrite())
		}
	}

	return kgo.NewClient(opts...)
}

func acksFromString(s string) (kgo.Acks, error) {
	switch strings.ToLower(s) {
	case "all", "-1", "":
		return kgo.AllISRAcks(), nil
	case "leader", "1":
		return kgo.LeaderAck(), nil
	case "none", "0":
		return kgo.NoAck(), nil
	default:
		return kgo.Acks{}, fmt.Errorf("invalid acks %q", s)
	}
}

func compressionFromString(s string) ([]kgo.CompressionCodec, error) {
	switch strings.ToLower(s) {
	case "", "none":
		return []kgo.CompressionCodec{kgo.NoCompression()}, nil
	case "lz4":
		return []kgo.CompressionCodec{kgo.Lz4Compression()}, nil
	case "zstd":
		return []kgo.CompressionCodec{kgo.ZstdCompression()}, nil
	case "snappy":
		return []kgo.CompressionCodec{kgo.SnappyCompression()}, nil
	case "gzip":
		return []kgo.CompressionCodec{kgo.GzipCompression()}, nil
	default:
		return nil, fmt.Errorf("invalid compression %q", s)
	}
}

func nanosToBytes(ns int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(ns))
	return b
}
