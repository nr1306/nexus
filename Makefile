COMPOSE := docker compose -f deploy/compose/docker-compose.yml

.PHONY: up down clean ps logs connectors build test

up:            ## start local stack (Kafka, Postgres, Redis, Debezium Connect)
	$(COMPOSE) up -d --wait

down:          ## stop local stack (keeps data volume)
	$(COMPOSE) down

clean:         ## stop local stack and delete data volume
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

connectors:    ## register Debezium outbox connectors (each service must have run its migrations once)
	./deploy/connect/register.sh

build:
	./gradlew build -x test

test:          ## all unit + integration tests (Go gateway added in Phase 3)
	./gradlew test
