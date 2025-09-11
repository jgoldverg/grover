# Makefile

PROTO_DIR := proto
OUT_DIR := pb
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

SERVER_DIR := cmd/groverd
CLIENT_DIR := cmd/grover

BIN_DIR := bin
SERVER_BIN := $(BIN_DIR)/groverd
CLIENT_BIN := $(BIN_DIR)/grover

.PHONY: all proto server client clean test

all: proto server client

proto:
	@echo "Generating protobuf code..."
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(OUT_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(OUT_DIR) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)
	@echo "Protobuf generation complete."

server:
	@mkdir -p $(BIN_DIR)
	@echo "Building server binary..."
	go build -o $(SERVER_BIN) ./$(SERVER_DIR)
	@echo "Built server: $(SERVER_BIN)"

client:
	@mkdir -p $(BIN_DIR)
	@echo "Building client binary..."
	go build -o $(CLIENT_BIN) ./$(CLIENT_DIR)
	@echo "Built client: $(CLIENT_BIN)"

clean:
	@echo "Cleaning generated files and binaries..."
	rm -rf $(OUT_DIR)/*.pb.go
	rm -rf $(BIN_DIR)
	@echo "Clean complete."

test:
	@echo "Running all Go tests..."
	go test ./...
