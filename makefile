up:
	docker compose -f docker-compose.yaml up -d --build
.PHONY: run

remove_old:
	docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`

down:
	docker compose -f docker-compose.yaml stop -t 3
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: logs

network-create:
	docker network inspect testing_net_tp1 >/dev/null 2>&1 || docker network create --subnet 172.255.125.0/24 testing_net_tp1
.PHONY: network-create

network-remove:
	docker network rm testing_net_tp1
.PHONY: network-remove

rabbit-up:
	docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 --network testing_net_tp1 -e RABBITMQ_DEFAULT_USER=guest -e RABBITMQ_DEFAULT_PASS=guest rabbitmq:3.9.16-management-alpine
.PHONY: rabbit-up

rabbit-down:
	docker stop rabbitmq
	docker rm rabbitmq
.PHONY: rabbit-down
