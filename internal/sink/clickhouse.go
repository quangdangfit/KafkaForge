package sink

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const ddl = `
CREATE TABLE IF NOT EXISTS kafkaforge.notifications_consumed (
    recv_at        DateTime64(3, 'UTC'),
    sent_at        DateTime64(3, 'UTC'),
    latency_ms     Float64,
    topic          LowCardinality(String),
    partition      Int32,
    offset         Int64,
    key            String,
    value          String
) ENGINE = MergeTree
ORDER BY (recv_at, partition, offset)
TTL toDateTime(recv_at) + INTERVAL 7 DAY
`

type Clickhouse struct {
	conn driver.Conn
}

// NewClickhouse opens a connection, ensures the schema exists, and returns a
// Sink that batches inserts via PrepareBatch.
func NewClickhouse(ctx context.Context, dsn string) (*Clickhouse, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS kafkaforge"); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ensure database: %w", err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ensure schema: %w", err)
	}
	return &Clickhouse{conn: conn}, nil
}

func (c *Clickhouse) Write(ctx context.Context, rs []Record) error {
	if len(rs) == 0 {
		return nil
	}
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO kafkaforge.notifications_consumed")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for _, r := range rs {
		if err := batch.Append(
			r.RecvAt, r.SentAt, float64(r.Latency)/float64(1e6),
			r.Topic, r.Partition, r.Offset, string(r.Key), string(r.Value),
		); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}
	return batch.Send()
}

func (c *Clickhouse) Close() error { return c.conn.Close() }
