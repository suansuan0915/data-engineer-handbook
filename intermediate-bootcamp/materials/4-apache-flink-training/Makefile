include flink-env.env

PLATFORM ?= linux/amd64

# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)


TARGET_MAX_CHAR_NUM=20

## Show help with `make help`
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.PHONY: build
## Builds the Flink base image with pyFlink and connectors installed
build:
	docker build --platform ${PLATFORM} -t ${IMAGE_NAME} .

.PHONY: up
## Builds the base Docker image and starts Flink cluster
up:
	docker compose --env-file flink-env.env up --build --remove-orphans  -d

.PHONY: down
## Shuts down the Flink cluster
down:
	docker compose down --remove-orphans

# .PHONY: job
## Submit the Flink job
# job:
# 	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/start_job.py --pyFiles /opt/src -d

# aggregation_job:
# 	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/aggregation_job.py --pyFiles /opt/src -d

.PHONY: hw_sessionization_job

hw_sessionization_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/hw_sessionization_job.py --pyFiles /opt/src -d

.PHONY: stop
## Stops all services in Docker compose
stop:
	docker compose stop

.PHONY: start
## Starts all services in Docker compose
start:
	docker compose start

.PHONY: clean
## Stops and removes the Docker container as well as images with tag `<none>`
clean:
	docker compose stop
	docker ps -a --format '{{.Names}}' | grep "^${CONTAINER_PREFIX}" | xargs -I {} docker rm {}
	docker images | grep "<none>" | awk '{print $3}' | xargs -r docker rmi
	# Uncomment line `docker rmi` if you want to remove the Docker image from this set up too
	# docker rmi ${IMAGE_NAME}

## Hoework: sample data and testing
.PHONY: produce_sample
## Produce sample JSONL events into Kafka topic (via kcat docker)
produce_sample:
	@echo "${YELLOW}Producing sample events to Kafka topic ${GREEN}${KAFKA_TOPIC}${RESET}${RESET}"
	@test -n "$(KAFKA_URL)" || (echo "Missing KAFKA_URL in flink-env.env"; exit 1)
	@test -n "$(KAFKA_TOPIC)" || (echo "Missing KAFKA_TOPIC in flink-env.env"; exit 1)
	@test -n "$(KAFKA_WEB_TRAFFIC_KEY)" || (echo "Missing KAFKA_WEB_TRAFFIC_KEY in flink-env.env"; exit 1)
	@test -n "$(KAFKA_WEB_TRAFFIC_SECRET)" || (echo "Missing KAFKA_WEB_TRAFFIC_SECRET in flink-env.env"; exit 1)
	@test -f data/sample_data.jsonl || (echo "Missing data/sample_data.jsonl"; exit 1)
	@cat data/sample_data.jsonl | docker run -i --rm edenhill/kcat:1.7.1 \
		-P -b "$(KAFKA_URL)" -t "$(KAFKA_TOPIC)" \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=PLAIN \
		-X sasl.username="$(KAFKA_WEB_TRAFFIC_KEY)" \
		-X sasl.password="$(KAFKA_WEB_TRAFFIC_SECRET)"
	@echo "${GREEN}Done producing sample.${RESET}"

## Initialize Postgres tables using hw_tables.sql
PG_HOST ?= localhost
PG_PORT ?= 5432
PG_DB   ?= $(POSTGRES_DB)

psql_cmd = PGPASSWORD="$(POSTGRES_PASSWORD)" psql -h "$(PG_HOST)" -p "$(PG_PORT)" -U "$(POSTGRES_USER)" -d "$(PG_DB)"

.PHONY: pg_init_host
## Initialize Postgres tables on host using hw_tables.sql (requires psql installed locally)
pg_init_host:
	@echo "${YELLOW}Applying hw_tables.sql to host Postgres...${RESET}"
	@$(psql_cmd) -f sql/hw_tables.sql
	@echo "${GREEN}Done.${RESET}"

.PHONY: pg_show_host
## Show sample outputs in host Postgres
pg_show_host:
	@$(psql_cmd) -c "TABLE processed_events_aggregated LIMIT 20;"
	@$(psql_cmd) -c "TABLE processed_session_events_aggregated_tc;"
	@$(psql_cmd) -c "TABLE processed_session_events_aggregated_zachtc;"
	@$(psql_cmd) -c "TABLE processed_session_events_aggregated_zacht;"
	@$(psql_cmd) -c "TABLE processed_session_events_aggregated_lulu;"

.PHONY: pg_init_docker
pg_init_docker:
	@docker run --rm -i \
		-e PGPASSWORD="$(POSTGRES_PASSWORD)" \
		-v "$(PWD)/job:/sql" \
		postgres:15 \
		psql -h "$(PG_HOST)" -p "$(PG_PORT)" -U "$(POSTGRES_USER)" -d "$(PG_DB)" \
		-f /sql/hw_tables.sql

.PHONY: test
## Test end-to-end: up -> init pg -> submit job -> produce sample -> show results
test:
	@$(MAKE) up
	@$(MAKE) pg_init_host
	@$(MAKE) hw_sessionization_job
	@sleep 20
	@$(MAKE) produce_sample
	@sleep 20
	@$(MAKE) pg_show_host


