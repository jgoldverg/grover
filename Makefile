# Makefile

## Directories
BIN_DIR    := bin
PROTO_SRC  := api/proto
PB_OUT     := pkg/groverpb

## Binaries
SERVER_DIR := cmd/groverd
CLIENT_DIR := cmd/grover

SERVER_BIN := $(BIN_DIR)/groverd
CLIENT_BIN := $(BIN_DIR)/grover

## Proto files (flat layout)
PROTO_FILES := \
  $(PROTO_SRC)/grover.proto \
  $(PROTO_SRC)/grover_udp.proto

## Discover module path for protoc-gen-go (trims output dirs correctly)
MODULE := $(shell go list -m)

## Go flags
LDFLAGS := -s -w
GOFLAGS :=
BUILD_TAGS :=

.PHONY: all proto server client clean test fmt vet tools proto-clean

all: proto server client

tools:
	@command -v protoc >/dev/null 2>&1 || { echo "Error: protoc not found. Install protoc first."; exit 1; }
	@command -v protoc-gen-go >/dev/null 2>&1 || { \
		echo "Installing protoc-gen-go..."; \
		GOBIN=$$(go env GOPATH)/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@latest; }
	@command -v protoc-gen-go-grpc >/dev/null 2>&1 || { \
		echo "Installing protoc-gen-go-grpc..."; \
		GOBIN=$$(go env GOPATH)/bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest; }

PB_OUT := .

proto: tools
	@echo "Generating protobuf Go code..."
	PATH="$$(go env GOPATH)/bin:$$PATH" \
	protoc -I $(PROTO_SRC) \
	  --go_out=$(PB_OUT)      --go_opt=module=$(MODULE) \
	  --go-grpc_out=$(PB_OUT) --go-grpc_opt=module=$(MODULE) \
	  $(PROTO_FILES)
	@echo "Protobuf generation complete."


server:
	@mkdir -p $(BIN_DIR)
	@echo "Building server binary -> $(SERVER_BIN)"
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(SERVER_BIN) ./$(SERVER_DIR)

client:
	@mkdir -p $(BIN_DIR)
	@echo "Building client binary -> $(CLIENT_BIN)"
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(CLIENT_BIN) ./$(CLIENT_DIR)

fmt:
	@echo "Formatting code..."
	go fmt ./...

vet:
	@echo "Running go vet..."
	go vet ./...

test:
	@echo "Running all Go tests..."
	go test $(GOFLAGS) ./...

proto-clean:
	@echo "Cleaning generated protobuf files..."
	find $(PB_OUT) -name '*.pb.go' -type f -delete
	@echo "Proto clean complete."

clean: proto-clean
	@echo "Cleaning binaries..."
	rm -rf $(BIN_DIR)
	@echo "Clean complete."
