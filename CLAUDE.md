# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Goal

A sandbox for tuning Kafka traffic: measure publisher & consumer throughput/latency under different configs (batch size, linger, compression, partitions, acks, fetch size, ...).

## Stack

- Go 1.25+
- Kafka cluster of 3–4 brokers (KRaft mode, no Zookeeper) via `docker-compose`
- ClickHouse (optional sink for metrics analysis)
- Client library: `github.com/twmb/franz-go` (better perf and finer control than sarama)

## Layout

```
cmd/publisher/     # publisher CLI
cmd/consumer/      # consumer CLI
internal/gen/      # data generator (push notification payloads)
internal/metrics/  # RPS, latency, p50/p95/p99
internal/sink/     # stdout, clickhouse
deploy/            # docker-compose for kafka + clickhouse
configs/           # YAML/env profiles per tuning scenario
```

## Status

| Phase | Scope | State |
|-------|-------|-------|
| P1 | 3-broker KRaft cluster + topic init | done |
| P2 | Single-publish baseline + generator | done |
| P3 | Consumer group with RPS report | done |
| P4 | Batching (linger, compression, fetch tuning) via profiles | done |
| P5 | HDR-histogram metrics: p50/p95/p99 end-to-end via `sent_at_ns` header | done |
| P6 | ClickHouse sink (`kafkaforge.notifications_consumed`) | done |
| P7 | YAML tuning profiles + matrix runner | done |
| P8 | Chaos script (`scripts/chaos.sh`); Grafana dashboard not yet built | partial |

## Architecture

End-to-end latency is propagated via a record header `sent_at_ns` (big-endian
int64 unix-nanos) set by the publisher and read by the consumer; this avoids
JSON parsing on the hot path and works regardless of payload format.

The producer mode is selected per profile (`single` vs `async`):
- `single` forces `MaxBufferedRecords=1` + `ProduceSync` so each record is one
  round-trip — the latency baseline.
- `async` enables `Linger` + `BatchMaxBytes` + compression + buffered produce
  with callbacks; this is what real workloads use.

`internal/metrics` wraps an HDR histogram (1ns–60s, 3 sig figs). The
`Reporter` prints rolling RPS / MB-per-sec / p50/p95/p99 at a configurable
interval; the histogram resets each tick so the percentiles describe the
*latest* window rather than the full run.

`internal/sink` is the consumer's downstream store. `Discard` is a no-op for
pure throughput tests; `Clickhouse` ensures the schema and inserts via
`PrepareBatch`. The consumer flushes when the buffer hits `sink_batch_size`
or every `sink_flush_period`.

## Conventions

- All code, comments, and docs in English
- Every I/O function takes `context.Context` as the first argument
- Log with `slog` (JSON handler)
- Timestamps are UTC, type `time.Time`
- Bench-relevant config goes in `configs/*.yaml`; CLI flags only cover runtime
  knobs (`--count`, `--rate`, `--report`) so two runs with the same profile
  are directly comparable

## Commands

```
make up                        # bring up kafka cluster + clickhouse + UI
make topic                     # create the notifications topic
make publish PROFILE=configs/batch-lz4.yaml COUNT=200000
make consume PROFILE=configs/batch-lz4.yaml
make matrix                    # publisher sweep over configs/*.yaml
make chaos                     # kill a random broker for DOWN_FOR seconds
make build vet test            # Go checks
make clean                     # down -v (drops volumes)
```

Profiles live in `configs/`: `baseline.yaml`, `batch-lz4.yaml`,
`batch-zstd.yaml`, `clickhouse-sink.yaml`. Add a new YAML to extend the
tuning matrix; `scripts/run-matrix.sh` picks them up by filename.

## Commit Convention

```
feat(payment): add Stripe Checkout Session creation endpoint
fix(webhook): handle out-of-order payment_intent.succeeded after charge.refunded
perf(redis): batch idempotency checks
test(e2e): add full payment flow integration test
```

Types: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `chore`, `ci`
Scope: `payment`, `webhook`, `blockchain`, `kafka`, `redis`, `mysql`, `api`, `config`, `bench`

**Author:** commit as configured `git config user.name` / `user.email`. Do **not** append a `Co-Authored-By: Claude …` trailer — repo history has none, keep it that way.
