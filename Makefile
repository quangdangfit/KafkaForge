COMPOSE  := docker compose -f deploy/docker-compose.yml
PROFILE  ?= configs/baseline.yaml
COUNT    ?= 0

.PHONY: up down restart ps logs topic clean publish consume matrix chaos build vet test

up:
	$(COMPOSE) up -d
	@echo "waiting for brokers to become healthy..."
	@until $(COMPOSE) ps --format json | grep -q '"Health":"healthy"'; do sleep 1; done
	@echo "kafka cluster ready (UI: http://localhost:8080, ClickHouse: http://localhost:8123)"

down:
	$(COMPOSE) down

restart: down up

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

topic:
	bash deploy/create-topics.sh

clean:
	$(COMPOSE) down -v

publish:
	go run ./cmd/publisher --profile $(PROFILE) --count $(COUNT)

consume:
	go run ./cmd/consumer --profile $(PROFILE)

matrix:
	bash scripts/run-matrix.sh

chaos:
	bash scripts/chaos.sh

build:
	go build ./...

vet:
	go vet ./...

test:
	go test ./...
