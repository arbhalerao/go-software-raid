.DEFAULT_GOAL := help

.PHONY: all build test help

all: help

build:
	go build -v ./...

test:
	go test -v ./...

help:
	@echo "Usage:"
	@echo "  make build   - Build the project"
	@echo "  make test    - Run tests"
	@echo "  make help    - Show this help message"
