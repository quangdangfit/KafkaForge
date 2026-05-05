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

## Roadmap (simple → advanced)

**P1 — Bootstrap**
- `docker-compose` with 3 Kafka brokers in KRaft mode, ports 9092/9093/9094
- Init script to create topic `notifications` (partitions=6, RF=3)

**P2 — Basic publisher**
- Generator producing fake push notifications (user_id, title, body, ts UTC)
- Single publish, log messages/sec

**P3 — Basic consumer**
- Consumer group, print RPS every second (sliding window)

**P4 — Batching**
- Publisher: batch + linger + compression (lz4/zstd)
- Consumer: fetch in batches, measure throughput vs latency

**P5 — Metrics**
- `internal/metrics`: RPS, p50/p95/p99 end-to-end (publish ts → consume ts)
- Print at interval

**P6 — ClickHouse sink**
- Consumer writes messages + latency to ClickHouse (async insert / buffer table)
- Sample queries for analysis

**P7 — Tuning matrix**
- YAML profiles: partitions, acks, batch.size, linger.ms, fetch.min.bytes, compression, ...
- Runner that sweeps profiles and prints a comparison table

**P8 — Advanced**
- Multi-publisher / multi-consumer scale-out
- Chaos: kill a broker, measure recovery
- Dashboard (Grafana + ClickHouse or Prometheus)

## Conventions

- All code, comments, and docs in English
- Every I/O function takes `context.Context` as the first argument
- Log with `slog` (JSON handler when benchmarking)
- Timestamps are UTC, type `time.Time`
- Config via env + flags (flags take precedence so benchmarks can override easily)

## Commands (filled in as code lands)

- `make up` / `make down` — start/stop kafka + clickhouse
- `make topic` — create topics
- `go run ./cmd/publisher --profile=configs/p4-batch.yaml`
- `go run ./cmd/consumer --profile=configs/p4-batch.yaml`

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
