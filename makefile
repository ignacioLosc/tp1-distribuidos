
create_base_images:
	docker build -f base-images/go-system.dockerfile -t golang-rabbit:1.0 system/
	docker build -f base-images/ZQ_client_golang.dockerfile -t golang-client:1.0 .
	docker build -f base-images/ZQ_system_golang.dockerfile -t golang-system-zq:1.0 .
	docker build -f base-images/ZQ-ready-run.dockerfile -t debian-zq:1.0 .
.PHONY: basic_images

remove_old : 
	docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`

create_images:
	docker build -f system/workers/gateway/Dockerfile -t gateway:latest system/
	docker build -f system/workers/input_controller/Dockerfile -t input_controller:latest system/
	docker build -f system/workers/platform_counter/Dockerfile -t platform_counter:latest system/
	docker build -f system/workers/platform_accumulator/Dockerfile -t platform_accumulator:latest system/
	docker build -f system/workers/genre_filter/Dockerfile -t genre_filter:latest system/
	docker build -f client/Dockerfile -t client:latest client/

system-up: create_base_images create_images network-create
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: system-up


system-down:
	docker compose -f docker-compose-dev.yaml stop -t 3
	docker compose -f docker-compose-dev.yaml down
.PHONY: system-down

network-create:
	docker network inspect testing_net_tp1 >/dev/null 2>&1 || docker network create --subnet 172.255.125.0/24 testing_net_tp1
.PHONY: network-create

logs :
	docker compose -f docker-compose-dev.yaml logs -f 
.PHONY: logs

network-remove:
	docker network rm testing_net_tp1
.PHONY: network-remove

rabbit-up:
	docker compose -f docker-compose-rabbit.yaml up -d --build
.PHONY: rabbit-up

rabbit-down:
	docker compose -f docker-compose-rabbit.yaml stop -t 1
	docker compose -f docker-compose-rabbit.yaml down
.PHONY: rabbit-down
