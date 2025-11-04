default: build

#make docker-image-<servicio> Construye solo UNA imagen específica
docker-image-%:
	@echo "Building Docker image: $*"
	docker build -f "./$*/Dockerfile" -t "$*:latest" .
.PHONY: docker-image-%


docker-image:
    docker build -f ./client/Dockerfile -t "client:latest" ./client
    docker build -f ./server/gateway/Dockerfile -t "gateway:latest" ./server/gateway
    docker build -f ./server/filter/Dockerfile -t "filter:latest" ./server/filter
.PHONY: docker-image

#construye todas las imagenes de docker
build: docker-image
.PHONY: build

#Levanta todo el sistema CON logs visibles en la terminal
start: docker-image
	docker compose -f docker-compose.yaml up --build -d
.PHONY: docker-compose-up

stop:
	docker compose -f docker-compose.yaml stop -t 30
	docker compose -f docker-compose.yaml down
	sudo cp server/client_logs.txt server/clients_logs_backup.txt || true
	sudo cp server/logs.txt server/logs_backup.txt || true
	sudo cp server/eof_logs.txt server/eof_logs_backup.txt || true
	echo -n > server/client_logs.txt
	echo -n > server/logs.txt
	echo -n > server/eof_logs.txt
.PHONY: stop

# Limpieza profunda (usar solo cuando realmente quieras limpiar todo)
clean-all: stop
	docker compose -f docker-compose.yaml down -v
	docker images --format "{{.Repository}}:{{.Tag}}" | grep -v "python" | xargs -r docker rmi -f || true
	docker container prune -f
	docker network prune -f
	docker builder prune -f
	docker volume prune -f
	sudo rm -rf report*
.PHONY: clean-all


logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

# Show logs for specific service
logs-%:
	docker compose -f docker-compose.yaml logs -f $*
.PHONY: logs-%


status:
	docker compose -f docker-compose.yaml ps
.PHONY: status

# Clean containers and images
clean:
	docker compose -f docker-compose.yaml down -v --remove-orphans
	docker system prune -f
.PHONY: clean

# Restart specific service
restart-%:
	docker compose -f docker-compose.yaml stop $*
	sleep 2
	docker compose -f docker-compose.yaml start $*
.PHONY: restart-%