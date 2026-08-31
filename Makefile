.DEFAULT_GOAL := help
BIN := mesh
CMD := ./cmd/mesh

.PHONY: help
help: ## показать список команд
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## собрать бинарник под текущую платформу
	go build -o bin/$(BIN) $(CMD)

.PHONY: test
test: ## прогнать тесты
	go test ./...

.PHONY: lint
lint: ## golangci-lint
	golangci-lint run

.PHONY: cross
cross: ## собрать бинарники под известные машины сети (Pi и маки)
	GOOS=linux  GOARCH=arm64 go build -o bin/$(BIN)-linux-arm64  $(CMD)
	GOOS=darwin GOARCH=arm64 go build -o bin/$(BIN)-darwin-arm64 $(CMD)

# Сборка под произвольную цель: GOOS и GOARCH задаёт вызывающий.
#
# Отдельно от cross потому, что архитектура VPS неизвестна. Обещать здесь
# linux/amd64 было бы догадкой: runbook уже однажды предлагал ставить
# bin/mesh-linux-amd64, которого не существовало ни в одной цели, — и
# инструкция была невыполнима буквально.
#
# Пример: make target GOOS=linux GOARCH=amd64
.PHONY: target
target: ## собрать под заданные GOOS/GOARCH (см. uname -m на целевой машине)
	@test -n "$(GOOS)" -a -n "$(GOARCH)" || \
		{ echo "нужны GOOS и GOARCH: make target GOOS=linux GOARCH=amd64"; exit 1; }
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o bin/$(BIN)-$(GOOS)-$(GOARCH) $(CMD)
	@echo "собрано: bin/$(BIN)-$(GOOS)-$(GOARCH)"

.PHONY: hub-check
hub-check: ## проверить ШАБЛОН hub.conf (боевой конфиг проверяйте nats-server -t на VPS)
	go test -count=1 ./internal/hubcheck/
