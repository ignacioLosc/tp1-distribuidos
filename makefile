NETWORK=steam_analyzer_net
COMPOSE_FILE=docker-compose.yaml

up: compose
	docker compose -f ${COMPOSE_FILE} up -d --build
.PHONY: run

remove_old:
	docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`

down:
	docker compose -f ${COMPOSE_FILE} stop -t 1
	docker compose -f ${COMPOSE_FILE} down
.PHONY: down

logs:
	docker compose -f ${COMPOSE_FILE} logs -f
.PHONY: logs

compose:
	python3 generate-compose.py
.PHONY: compose
