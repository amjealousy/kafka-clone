PROTO_SRC := datatypes/proto
PROTO_OUT := datatypes/proto-generated
COMPOSE_FILE ?= etcd.yaml

.PHONY: all dirs run stop proto proto-clean

all: run

# Перегенерировать *.pb.go / *_grpc.pb.go из datatypes/proto/*.proto.
# Генерируемый код кладём в datatypes/protogenerated — отдельно от
# datatypes/*.go, написанного вручную (encode.go, broker.go, peer.go и т.д.),
# чтобы не путать sgenerated-код с бизнес-логикой.
#
# Требует protoc и плагины protoc-gen-go / protoc-gen-go-grpc:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# (плагины ставятся в $(go env GOPATH)/bin — он должен быть в $PATH)
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

server-build: dirs
	@go build -o server ../server

run: server-build stop
	@cd node1; ../server -id 1 -tcp 127.0.0.1:5090 -grpc 127.0.0.1:6090 -control 127.0.0.1:7090 -etcd 127.0.0.1:2379 & echo $$! > server.pid
	@cd node2; ../server -id 2 -tcp 127.0.0.1:5091 -grpc 127.0.0.1:6091 -control 127.0.0.1:7091 -etcd 127.0.0.1:2379 & echo $$! > server.pid
	@cd node3; ../server -id 3 -tcp 127.0.0.1:5092 -grpc 127.0.0.1:6092 -control 127.0.0.1:7092 -etcd 127.0.0.1:2379 & echo $$! > server.pid
	@echo "Started brokers: $$(cat node1/server.pid) $$(cat node2/server.pid) $$(cat node3/server.pid)"

stop:
	@for dir in node1 node2 node3; do \
		pid_file="$$dir/server.pid"; \
		if [ -f "$$pid_file" ]; then \
			pid=$$(cat "$$pid_file"); \
			if kill -0 "$$pid" 2>/dev/null; then kill "$$pid" 2>/dev/null || true; fi; \
			rm -f "$$pid_file"; \
		fi; \
	done
	@pkill -u "$$(id -u)" -x server 2>/dev/null || true

etcd:
	docker compose -f $(COMPOSE_FILE) up -d --wait
	@echo "Etcd была успешно развернута."

