// Consumer CLI: joins a consumer group, fetches in batches, records latency,
// and forwards records to a sink (discard or ClickHouse).
//
// End-to-end latency is computed from the "sent_at_ns" record header set by
// the publisher (see cmd/publisher), so it reflects time-on-wire-plus-broker
// rather than just consumer-side processing.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/quangdangfit/kafkaforge/internal/config"
	"github.com/quangdangfit/kafkaforge/internal/metrics"
	"github.com/quangdangfit/kafkaforge/internal/sink"
)

const sentAtHeader = "sent_at_ns"

type runFlags struct {
	profilePath string
	report      time.Duration
}

func parseFlags() runFlags {
	var f runFlags
	flag.StringVar(&f.profilePath, "profile", "configs/baseline.yaml", "path to YAML tuning profile")
	flag.DurationVar(&f.report, "report", time.Second, "interval between metric log lines")
	flag.Parse()
	return f
}

func main() {
	flags := parseFlags()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err := run(flags); err != nil {
		slog.Error("consumer failed", "err", err)
		os.Exit(1)
	}
}

func run(flags runFlags) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	profile, err := config.Load(flags.profilePath)
	if err != nil {
		return err
	}

	out, err := newSink(ctx, profile)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	cl, err := newClient(profile)
	if err != nil {
		return err
	}
	defer cl.Close()

	slog.Info("connected",
		"brokers", profile.Brokers,
		"topic", profile.Topic,
		"group", profile.Consumer.GroupID,
		"sink", profile.Consumer.Sink,
		"protocol", profile.Consumer.GroupProtocol,
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
				slog.Info(rep.Tick("consume"))
			}
		}
	}()

	flushEvery := profile.Consumer.SinkFlushPeriod
	maxBatch := profile.Consumer.SinkBatchSize
	buf := make([]sink.Record, 0, maxBatch)
	flushTimer := time.NewTimer(flushEvery)
	defer flushTimer.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := out.Write(ctx, buf); err != nil {
			slog.Warn("sink write", "err", err)
		}
		buf = buf[:0]
	}

	for {
		if ctx.Err() != nil {
			break
		}

		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if ctx.Err() != nil {
					break
				}
				slog.Warn("fetch error", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
		}

		now := time.Now().UTC()
		fetches.EachRecord(func(r *kgo.Record) {
			sentAt := readSentAt(r)
			latency := time.Duration(0)
			if !sentAt.IsZero() {
				latency = now.Sub(sentAt)
			}
			rec.Inc(len(r.Key) + len(r.Value))
			rec.Observe(latency)
			buf = append(buf, sink.Record{
				Key: r.Key, Value: r.Value,
				Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
				SentAt: sentAt, RecvAt: now, Latency: latency,
			})
			if len(buf) >= maxBatch {
				flush()
			}
		})

		// Drain timer non-blockingly and flush on its tick.
		select {
		case <-flushTimer.C:
			flush()
			flushTimer.Reset(flushEvery)
		default:
		}
	}

	flush()
	slog.Info("done", "summary", rep.Tick("consume.final"))
	return nil
}

func newClient(p *config.Profile) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(p.Brokers, ",")...),
		kgo.ConsumeTopics(p.Topic),
		kgo.ConsumerGroup(p.Consumer.GroupID),
		kgo.ClientID("kafkaforge-consumer"),
		kgo.DisableAutoCommit(),
		kgo.AutoCommitMarks(),
	}
	if p.Consumer.FetchMinBytes > 0 {
		opts = append(opts, kgo.FetchMinBytes(int32(p.Consumer.FetchMinBytes)))
	}
	if p.Consumer.FetchMaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(int32(p.Consumer.FetchMaxBytes)))
	}
	if p.Consumer.FetchMaxWait > 0 {
		opts = append(opts, kgo.FetchMaxWait(p.Consumer.FetchMaxWait))
	}
	return kgo.NewClient(opts...)
}

func newSink(ctx context.Context, p *config.Profile) (sink.Sink, error) {
	switch strings.ToLower(p.Consumer.Sink) {
	case "", "discard", "stdout":
		return sink.NewDiscard(), nil
	case "clickhouse":
		dsn := p.Consumer.ClickhouseDSN
		if dsn == "" {
			dsn = "clickhouse://default:@localhost:9000/default"
		}
		return sink.NewClickhouse(ctx, dsn)
	default:
		return nil, fmt.Errorf("unknown sink %q", p.Consumer.Sink)
	}
}

func readSentAt(r *kgo.Record) time.Time {
	for _, h := range r.Headers {
		if h.Key == sentAtHeader && len(h.Value) == 8 {
			ns := int64(binary.BigEndian.Uint64(h.Value))
			return time.Unix(0, ns).UTC()
		}
	}
	return time.Time{}
}
