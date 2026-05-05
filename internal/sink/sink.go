// Package sink consumes batches of decoded records into a downstream store.
//
// Discard is the default — useful when a benchmark only cares about throughput
// and the in-process metrics. Clickhouse persists every record so latency and
// throughput can be analysed offline with SQL.
package sink

import (
	"context"
	"time"
)

type Record struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
	SentAt    time.Time
	RecvAt    time.Time
	Latency   time.Duration
}

type Sink interface {
	Write(ctx context.Context, rs []Record) error
	Close() error
}

type Discard struct{}

func NewDiscard() Discard                                          { return Discard{} }
func (Discard) Write(context.Context, []Record) error              { return nil }
func (Discard) Close() error                                       { return nil }
