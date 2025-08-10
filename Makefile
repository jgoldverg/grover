# Makefile

PROTO_DIR := proto
OUT_DIR := pb
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

SERVER_DIR := server
CLIENT_DIR := client

BIN_DIR := bin
SERVER_BIN := $(BIN_DIR)/go-rover
CLIENT_BIN := $(BIN_DIR)/go-rover

.PHONY: all proto build-server build-client clean test

all: proto build-server build-client

proto:
	@echo "Generating protobuf code..."
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(OUT_DIR) \
		--go-grpc_out=$(OUT_DIR) \
		$(PROTO_FILES)
	@echo "Protobuf generation complete."

build-server:
	@mkdir -p $(BIN_DIR)
	@echo "Building server binary..."
	go build -o $(SERVER_BIN) ./$(SERVER_DIR)
	@echo "Built server: $(SERVER_BIN)"

build-client:
	@mkdir -p $(BIN_DIR)
	@echo "Building client binary..."
	go build -o $(CLIENT_BIN) ./$(CLIENT_DIR)
	@echo "Built client: $(CLIENT_BIN)"

clean:
	@echo "Cleaning generated files and binaries..."
	rm -f $(OUT_DIR)/*.pb.go
	rm -f $(SERVER_BIN) $(CLIENT_BIN)
	@echo "Clean complete."

test:
	@echo "Running all Go tests..."
	go test ./...

