.PHONY: all build test lint fmt clean examples

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
GOFMT=gofmt
GOMOD=$(GOCMD) mod

# Build targets
all: fmt lint test build

build:
	$(GOBUILD) -v ./...

test:
	$(GOTEST) -v -race -coverprofile=coverage.out ./...

test-short:
	$(GOTEST) -v -short ./...

coverage: test
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

lint:
	$(GOVET) ./...
	@if command -v staticcheck > /dev/null; then \
		staticcheck ./...; \
	fi

fmt:
	$(GOFMT) -s -w .

tidy:
	$(GOMOD) tidy

clean:
	rm -f coverage.out coverage.html
	$(GOCMD) clean

# Examples
examples: build
	@echo "Building examples..."
	$(GOBUILD) -v ./examples/basic
	$(GOBUILD) -v ./examples/workflow

run-basic:
	$(GOCMD) run ./examples/basic

run-workflow:
	$(GOCMD) run ./examples/workflow

# Development
dev-deps:
	go install honnef.co/go/tools/cmd/staticcheck@latest

# Documentation
docs:
	@echo "Opening documentation..."
	$(GOCMD) doc -all .

# Benchmarks
bench:
	$(GOTEST) -bench=. -benchmem ./...

# Release helpers
version:
	@git describe --tags --always --dirty

changelog:
	@git log --oneline --decorate $(shell git describe --tags --abbrev=0)..HEAD
