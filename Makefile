.PHONY: test run docker-up docker-down

test:
	go test ./... -v

run:
	go run ./cmd/gateway

docker-up:
	docker compose up --build

docker-down:
	docker compose down
