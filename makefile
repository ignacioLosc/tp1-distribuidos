NETWORK=steam_analyzer_net
COMPOSE_FILE=docker-compose.yaml

up:
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

network-create:
	docker network inspect ${NETWORK} >/dev/null 2>&1 || docker network create --subnet 172.255.125.0/24 ${NETWORK}
.PHONY: network-create

network-remove:
	docker network rm ${NETWORK}
.PHONY: network-remove

rabbit-up:
	docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 --network ${NETWORK} -e RABBITMQ_DEFAULT_USER=guest -e RABBITMQ_DEFAULT_PASS=guest rabbitmq:3.9.16-management-alpine
.PHONY: rabbit-up

rabbit-down:
	docker stop rabbitmq -t 1
	docker rm rabbitmq
.PHONY: rabbit-down
