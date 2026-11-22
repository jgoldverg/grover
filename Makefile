# Grover build. `make` builds both binaries into bin/ (not committed).
BIN      := bin
LDFLAGS  := -s -w
PROTO    := api/proto/grover.proto
MODULE   := github.com/jgoldverg/grover

.PHONY: all build grover groverd test test-race lint fmt vet proto clean linux

all: build

build: grover groverd

grover:
	go build -ldflags '$(LDFLAGS)' -o $(BIN)/grover ./cmd/grover

groverd:
	go build -ldflags '$(LDFLAGS)' -o $(BIN)/groverd ./cmd/groverd

# Cross-compile for Linux nodes (FABRIC, TACC, UC, ...).
linux:
	GOOS=linux GOARCH=amd64 go build -ldflags '$(LDFLAGS)' -o $(BIN)/linux-amd64/grover ./cmd/grover
	GOOS=linux GOARCH=amd64 go build -ldflags '$(LDFLAGS)' -o $(BIN)/linux-amd64/groverd ./cmd/groverd

# Unit + in-process integration tests (they bind loopback sockets).
test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w cmd internal integration

lint:
	golangci-lint run ./...

# Regenerate internal/pb from api/proto/grover.proto.
# Needs protoc, protoc-gen-go and protoc-gen-go-grpc on PATH.
proto:
	protoc -I api/proto \
	  --go_out=. --go_opt=module=$(MODULE) \
	  --go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
	  $(PROTO)

clean:
	rm -rf $(BIN)
