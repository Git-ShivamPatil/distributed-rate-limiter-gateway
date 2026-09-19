.PHONY: help build run test race vet fmt lint smoke verify up down logs migrate clean

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

test: ## Unit tests with the race detector
	$(GO) test ./... -race -count=1

vet: ## go vet
	$(GO) vet ./...

fmt: ## Format, and fail if anything was unformatted
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi

smoke: ## Start a real gateway and exercise it over a socket
	./scripts/gateway-smoke.sh

verify: fmt vet test smoke ## Everything CI runs

up: ## Start the data plane (redis, postgres)
	docker compose up -d redis postgres

down: ## Stop the data plane
	docker compose down

logs: ## Follow the data plane logs
	docker compose logs -f

migrate: ## Apply the policy-store schema (milestone 3)
	@echo "migrate: the policy store lands in milestone 3; nothing to apply yet" >&2
	@exit 1

clean: ## Remove build output
	rm -rf ./bin
