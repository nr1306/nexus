COMPOSE := docker compose -f deploy/compose/docker-compose.yml

.PHONY: up down clean ps logs connectors build test e2e phase1-check

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

e2e:           ## end-to-end saga tests: real services as containers + Debezium (slow; needs Docker)
	./gradlew :tests:e2e:test -Pe2e -Pe2eTests='*SagaE2eIT'

phase1-check:  ## Phase 1 done check: 1,000 mixed orders, 0 stock drift, 0 double charges; writes bench/results/
	./gradlew :tests:e2e:test -Pe2e -Pe2eTests='*PhaseOneDoneCheckIT' -PdoneCheckOrders=1000 -PresultsDir=$(CURDIR)/bench/results
