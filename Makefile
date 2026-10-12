COMPOSE := docker compose -f deploy/compose/docker-compose.yml

.PHONY: up down clean ps logs connectors build test e2e phase1-check phase2-check s6-check stripe-check

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

phase2-check:  ## Phase 2 done check: kill each service mid-load, no order lost; writes bench/results/
	./gradlew :tests:e2e:test -Pe2e -Pe2eTests='*PhaseTwoDoneCheckIT' -PresultsDir=$(CURDIR)/bench/results

stripe-check:  ## Stripe adapter against the real test-mode API (needs STRIPE_API_KEY=sk_test_...); fails if the live test is skipped
	@echo "$$STRIPE_API_KEY" | grep -Eq '^(sk|rk)_test_.+' || (echo 'Set STRIPE_API_KEY to a Stripe test-mode key (sk_test_... or rk_test_...)'; exit 1)
	./gradlew :services:payment:test --rerun --tests '*StripeLiveIT' --tests '*StripePaymentGatewayTest'
	@grep -q 'skipped="0"' services/payment/build/test-results/test/TEST-com.nexus.payment.gateway.stripe.StripeLiveIT.xml \
		|| (echo 'StripeLiveIT was skipped: the live Stripe API was not exercised'; exit 1)
	@echo 'StripeLiveIT ran against the Stripe test API'

s6-check:      ## S6 / Phase 3 reconciliation check: no false positives, every injected inconsistency detected; writes bench/results/
	./gradlew :tests:e2e:test -Pe2e -Pe2eTests='*ReconciliationCheckIT' -PresultsDir=$(CURDIR)/bench/results
