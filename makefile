system-up: 
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-system-up

system-down:
	docker compose -f docker-compose-dev.yaml stop -t 3
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-system-down

network-create:
	docker network create --subnet 172.255.125.0/24 testing_net_tp1
.PHONY: network-create

network-remove:
	docker network rm testing_net_tp1
.PHONY: network-remove

rabbit-up:
	docker compose -f docker-compose-rabbit.yaml up -d --build
.PHONY: docker-rabbit-up

rabbit-down:
	docker compose -f docker-compose-rabbit.yaml stop -t 10
	docker compose -f docker-compose-rabbit.yaml down
.PHONY: docker-rabbit-down