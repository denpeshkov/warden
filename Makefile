POSTGRES_URL ?= postgres://postgres:postgres@localhost:5432/warden?sslmode=disable
REDIS_URL ?= localhost:6379

.DEFAULT_GOAL := help

.PHONY: help
help: ## Display this help screen
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_\/-]+:.*##/ {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: lint
lint: ## Run linter
	@go mod tidy -v && go mod verify
	@golangci-lint run --fix -v -c .golangci.yaml

.PHONY: test
test: ## Run tests
	@POSTGRES_URL="$(POSTGRES_URL)" \
	REDIS_URL="$(REDIS_URL)" \
	go test -race -count=1 -shuffle=on -timeout=2m ./...

.PHONY: test/cover
test/cover: ## Run tests with coverage
	@POSTGRES_URL="$(POSTGRES_URL)" \
	REDIS_URL="$(REDIS_URL)" \
	go test -race -count=1 -shuffle=on -timeout=2m ./... -coverprofile=/tmp/cover.out ./...
	@go tool cover -func=/tmp/cover.out
