// Package config loads YAML tuning profiles. A profile fully describes a
// publisher/consumer run so benchmarks are reproducible from a single file.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Profile struct {
	Brokers  string      `yaml:"brokers"`
	Topic    string      `yaml:"topic"`
	Producer ProducerCfg `yaml:"producer"`
	Consumer ConsumerCfg `yaml:"consumer"`
}

type ProducerCfg struct {
	// Mode is "single" (one synchronous round-trip per record, used as the
	// latency baseline) or "async" (batched with linger + compression).
	Mode          string        `yaml:"mode"`
	Acks          string        `yaml:"acks"`
	Linger        time.Duration `yaml:"linger"`
	BatchMaxBytes int           `yaml:"batch_max_bytes"`
	Compression   string        `yaml:"compression"`
	MaxBuffered   int           `yaml:"max_buffered"`
	Idempotent    bool          `yaml:"idempotent"`
}

type ConsumerCfg struct {
	GroupID         string        `yaml:"group_id"`
	GroupProtocol   string        `yaml:"group_protocol"` // "" (default) | "classic" | "consumer"
	FetchMinBytes   int           `yaml:"fetch_min_bytes"`
	FetchMaxBytes   int           `yaml:"fetch_max_bytes"`
	FetchMaxWait    time.Duration `yaml:"fetch_max_wait"`
	Sink            string        `yaml:"sink"` // "" or "discard" | "clickhouse"
	ClickhouseDSN   string        `yaml:"clickhouse_dsn"`
	SinkBatchSize   int           `yaml:"sink_batch_size"`
	SinkFlushPeriod time.Duration `yaml:"sink_flush_period"`
}

func Load(path string) (*Profile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read profile: %w", err)
	}
	p := &Profile{}
	if err := yaml.Unmarshal(b, p); err != nil {
		return nil, fmt.Errorf("parse profile: %w", err)
	}
	p.applyDefaults()
	return p, nil
}

func (p *Profile) applyDefaults() {
	if p.Brokers == "" {
		p.Brokers = "localhost:9092,localhost:9094,localhost:9095"
	}
	if p.Topic == "" {
		p.Topic = "notifications"
	}
	if p.Producer.Mode == "" {
		p.Producer.Mode = "async"
	}
	if p.Producer.Acks == "" {
		p.Producer.Acks = "all"
	}
	if p.Producer.MaxBuffered == 0 {
		p.Producer.MaxBuffered = 10_000
	}
	if p.Consumer.GroupID == "" {
		p.Consumer.GroupID = "kafkaforge"
	}
	if p.Consumer.Sink == "" {
		p.Consumer.Sink = "discard"
	}
	if p.Consumer.SinkBatchSize == 0 {
		p.Consumer.SinkBatchSize = 1000
	}
	if p.Consumer.SinkFlushPeriod == 0 {
		p.Consumer.SinkFlushPeriod = time.Second
	}
}
