PROTO_SRC := datatypes/proto
PROTO_OUT := datatypes/proto-generated
COMPOSE_FILE ?= etcd.yaml
BINARY := kafka-clone-server

HOST ?= 127.0.0.1
ETCD_ENDPOINTS ?= 127.0.0.1:2379
CORS_ORIGINS ?= http://127.0.0.1:8090,http://127.0.0.1:8091,http://127.0.0.1:8092

.PHONY: all dirs build server-build build-all run restart stop clean \
	proto proto-clean etcd etcd-stop test fmt-check vet check

all: run

# Перегенерировать *.pb.go / *_grpc.pb.go из datatypes/proto/*.proto.
# Генерируемый код лежит отдельно от написанной вручную бизнес-логики.
# Требует protoc и плагинов protoc-gen-go / protoc-gen-go-grpc в PATH.
proto:
	@mkdir -p $(PROTO_OUT)
	protoc -I $(PROTO_SRC) \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_SRC)/*.proto

proto-clean:
	@rm -f $(PROTO_OUT)/*.pb.go

dirs:
	@mkdir -p node1 node2 node3

# Локальный бинарник нужен для запуска трёх нод из отдельных рабочих каталогов.
build server-build: dirs
	@go build -o $(BINARY) .

# Проверяет сборку всех серверных пакетов, не затрагивая сломанный client/.
build-all:
	@cd .. && go build ./server/...

run: build
	@$(MAKE) --no-print-directory stop
	@cd node1; ../$(BINARY) -id 1 -tcp $(HOST):5090 -grpc $(HOST):6090 -control $(HOST):7090 -http $(HOST):8090 -etcd $(ETCD_ENDPOINTS) -cors-origins "$(CORS_ORIGINS)" & echo $$! > server.pid
	@cd node2; ../$(BINARY) -id 2 -tcp $(HOST):5091 -grpc $(HOST):6091 -control $(HOST):7091 -http $(HOST):8091 -etcd $(ETCD_ENDPOINTS) -cors-origins "$(CORS_ORIGINS)" & echo $$! > server.pid
	@cd node3; ../$(BINARY) -id 3 -tcp $(HOST):5092 -grpc $(HOST):6092 -control $(HOST):7092 -http $(HOST):8092 -etcd $(ETCD_ENDPOINTS) -cors-origins "$(CORS_ORIGINS)" & echo $$! > server.pid
	@echo "Брокеры запущены: $$(cat node1/server.pid) $$(cat node2/server.pid) $$(cat node3/server.pid)"
	@echo "Админка: http://$(HOST):8090"

restart: run

stop:
	@for dir in node1 node2 node3; do \
		pid_file="$$dir/server.pid"; \
		if [ -f "$$pid_file" ]; then \
			pid=$$(cat "$$pid_file"); \
			if kill -0 "$$pid" 2>/dev/null; then kill "$$pid" 2>/dev/null || true; fi; \
			rm -f "$$pid_file"; \
		fi; \
	done

clean: stop
	@rm -f $(BINARY)

etcd:
	docker compose -f $(COMPOSE_FILE) up -d --wait
	@echo "Etcd запущен."

etcd-stop:
	docker compose -f $(COMPOSE_FILE) down

test:
	@cd .. && go test -count=1 -race ./server/...

fmt-check:
	@cd .. && files=$$(gofmt -l server/); \
		if [ -n "$$files" ]; then echo "Требуется gofmt:"; echo "$$files"; exit 1; fi

vet:
	@cd .. && go vet ./server/...

check: build-all test fmt-check vet
