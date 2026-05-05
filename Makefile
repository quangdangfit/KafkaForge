COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: up down restart ps logs topic clean

up:
	$(COMPOSE) up -d
	@echo "waiting for brokers to become healthy..."
	@until $(COMPOSE) ps --format json | grep -q '"Health":"healthy"'; do sleep 1; done
	@echo "kafka cluster ready (UI: http://localhost:8080)"

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
