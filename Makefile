.PHONY: help build run echo test race vet fmt lint smoke verify up down logs migrate clean proto proto-lint proto-check

GO      ?= go
BIN     ?= ./bin/gateway
CONFIG  ?= ./configs/local.yaml
NODE    ?= gateway-1

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Compile the gateway
	$(GO) build -o $(BIN) ./cmd/gateway

run: ## Run a gateway node against configs/local.yaml
	$(GO) run ./cmd/gateway --node=$(NODE) --config=$(CONFIG)

echo: ## Run the echo upstream the default route points at
	$(GO) run ./cmd/echo --addr=127.0.0.1:9000

proto: ## Regenerate the gRPC code from api/proto
	buf generate

proto-lint: ## Lint the protobuf definitions
	buf lint

proto-check: proto-lint ## Fail if the committed generated code is not current
	buf generate
	git diff --exit-code -- api/gen

test: ## Unit tests with the race detector
	$(GO) test ./... -race -count=1

vet: ## go vet
	$(GO) vet ./...

fmt: ## Format, and fail if anything was unformatted
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi

smoke: ## Start a real gateway and exercise it over a socket
	./scripts/gateway-smoke.sh

verify: fmt vet test smoke ## Everything CI runs (add proto-check with buf installed)

up: ## Start the data plane (redis, postgres)
	docker compose up -d redis postgres

down: ## Stop the data plane
	docker compose down

logs: ## Follow the data plane logs
	docker compose logs -f

migrate: ## Apply the policy-store schema
	$(GO) run ./cmd/gatewayctl migrate up --config $(CONFIG)

migrate-down: ## Roll back one migration
	$(GO) run ./cmd/gatewayctl migrate down 1 --config $(CONFIG)

migrate-version: ## Print the applied schema version
	$(GO) run ./cmd/gatewayctl migrate version --config $(CONFIG)

clean: ## Remove build output
	rm -rf ./bin
