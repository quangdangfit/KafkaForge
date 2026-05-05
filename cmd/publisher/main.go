// Publisher CLI: generates fake push notifications and publishes them to Kafka.
//
// In P2 the focus is correctness and a baseline RPS number, so each message is
// produced synchronously (one round-trip per message). Batching, linger and
// compression land in P4.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/quangdangfit/kafkaforge/internal/gen"
)

type config struct {
	brokers     string
	topic       string
	count       int64
	rateLimit   int
	users       int64
	reportEvery time.Duration
	clientID    string
	acks        string
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.brokers, "brokers", "localhost:9092,localhost:9094,localhost:9095", "comma-separated bootstrap brokers")
	flag.StringVar(&c.topic, "topic", "notifications", "topic to publish to")
	flag.Int64Var(&c.count, "count", 0, "stop after N messages (0 = run until interrupted)")
	flag.IntVar(&c.rateLimit, "rate", 0, "max messages per second (0 = unlimited)")
	flag.Int64Var(&c.users, "users", 1_000_000, "size of the synthetic user space")
	flag.DurationVar(&c.reportEvery, "report", time.Second, "interval between RPS log lines")
	flag.StringVar(&c.clientID, "client-id", "kafkaforge-publisher", "Kafka client.id")
	flag.StringVar(&c.acks, "acks", "all", "ack mode: all | leader | none")
	flag.Parse()
	return c
}

func acksFromString(s string) (kgo.Acks, error) {
	switch strings.ToLower(s) {
	case "all", "-1":
		return kgo.AllISRAcks(), nil
	case "leader", "1":
		return kgo.LeaderAck(), nil
	case "none", "0":
		return kgo.NoAck(), nil
	default:
		return kgo.Acks{}, fmt.Errorf("invalid acks %q (want all|leader|none)", s)
	}
}

func main() {
	cfg := parseFlags()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(cfg); err != nil {
		logger.Error("publisher failed", "err", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	acks, err := acksFromString(cfg.acks)
	if err != nil {
		return err
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.DefaultProduceTopic(cfg.topic),
		kgo.ClientID(cfg.clientID),
		kgo.RequiredAcks(acks),
		// P2: keep batching disabled so each Produce is a real single round-trip.
		// franz-go has no "disable batching" knob, but a 1-record max effectively
		// flushes per message; combined with ProduceSync below we get single-shot
		// publishing semantics.
		kgo.MaxBufferedRecords(1),
	)
	if err != nil {
		return fmt.Errorf("kgo client: %w", err)
	}
	defer cl.Close()

	if err := cl.Ping(ctx); err != nil {
		return fmt.Errorf("ping brokers: %w", err)
	}
	slog.Info("connected", "brokers", cfg.brokers, "topic", cfg.topic, "acks", cfg.acks)

	g := gen.NewGenerator(uint64(time.Now().UnixNano()), cfg.users)

	var sent, failed atomic.Int64
	go reportLoop(ctx, cfg.reportEvery, &sent, &failed)

	var ticker *time.Ticker
	if cfg.rateLimit > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(cfg.rateLimit))
		defer ticker.Stop()
	}

	for {
		if ctx.Err() != nil {
			break
		}
		if cfg.count > 0 && sent.Load()+failed.Load() >= cfg.count {
			break
		}
		if ticker != nil {
			select {
			case <-ctx.Done():
				break
			case <-ticker.C:
			}
		}

		key, val, err := g.NextJSON()
		if err != nil {
			return err
		}
		rec := &kgo.Record{Key: key, Value: val}

		// ProduceSync = the single-publish baseline for P2.
		if r := cl.ProduceSync(ctx, rec).FirstErr(); r != nil {
			failed.Add(1)
			slog.Warn("produce failed", "err", r)
			continue
		}
		sent.Add(1)
	}

	slog.Info("shutting down", "sent", sent.Load(), "failed", failed.Load())
	return cl.Flush(context.Background())
}

func reportLoop(ctx context.Context, every time.Duration, sent, failed *atomic.Int64) {
	t := time.NewTicker(every)
	defer t.Stop()

	var lastSent, lastFailed int64
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s, f := sent.Load(), failed.Load()
			elapsed := now.Sub(last).Seconds()
			rps := float64(s-lastSent) / elapsed
			fps := float64(f-lastFailed) / elapsed
			slog.Info("rps",
				"sent_total", s,
				"failed_total", f,
				"sent_per_sec", fmt.Sprintf("%.1f", rps),
				"failed_per_sec", fmt.Sprintf("%.1f", fps),
			)
			lastSent, lastFailed, last = s, f, now
		}
	}
}
