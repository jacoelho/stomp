.PHONY: test test-unit test-integration test-all build clean lint fmt vet staticcheck help proofs fuzz tla

# Default target
help:
	@echo "Available targets:"
	@echo "  test          - Run unit tests only"
	@echo "  test-unit     - Run unit tests only (same as test)"
	@echo "  test-integration - Run integration tests (requires Apache ActiveMQ Artemis)"
	@echo "  test-all      - Run all tests (unit + integration)"
	@echo "  build         - Build all packages"
	@echo "  clean         - Clean build artifacts"
	@echo "  lint          - Run linters (golangci-lint)"
	@echo "  fmt           - Format code"
	@echo "  vet           - Run go vet"
	@echo "  staticcheck   - Run staticcheck"
	@echo ""
	@echo "Environment variables:"
	@echo "  STOMP_TEST_SERVER - Override default server address for integration tests (default: localhost:61613)"
	@echo ""
	@echo "Examples:"
	@echo "  make test                                    # Run unit tests"
	@echo "  make test-integration                        # Run integration tests with localhost:61613"
	@echo "  STOMP_TEST_SERVER=192.168.1.100:61613 make test-integration  # Run with custom server"
	@echo "  make test-all                                # Run all tests"
	@echo ""
	@echo "Integration test setup:"
	@echo "  ./scripts/run-integration-tests.sh full     # Complete test cycle with Docker"
	@echo "  docker compose -f docker-compose.test.yml up -d  # Start Artemis manually"

# Run unit tests only (default)
test: test-unit

# Run unit tests only
test-unit:
	@echo "Running unit tests..."
	go test -race ./...

# Run integration tests (requires Apache ActiveMQ Artemis)
test-integration:
	@echo "Running integration tests..."
	@echo "Using server: $${STOMP_TEST_SERVER:-localhost:61613}"
	@echo "Note: This requires a running Apache ActiveMQ Artemis server."
	@echo "You can set STOMP_TEST_SERVER environment variable to use a different server."
	@echo ""
	go test -race -tags=integration ./...

# Run all tests (unit + integration)
test-all:
	@echo "Running all tests (unit + integration)..."
	@echo "Using server: $${STOMP_TEST_SERVER:-localhost:61613}"
	@echo "Note: Integration tests require a running Apache ActiveMQ Artemis server."
	@echo ""
	go test -race -tags=integration ./...

# Build all packages
build:
	@echo "Building all packages..."
	go build ./...

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	go clean ./...
	go clean -testcache

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Run go vet
vet:
	@echo "Running go vet..."
	go vet ./...

# Run staticcheck
staticcheck:
	@echo "Running staticcheck..."
	staticcheck ./...

# Run golangci-lint
lint:
	@echo "Running golangci-lint..."
	golangci-lint run

# Comprehensive check (format, vet, staticcheck, test)
check: fmt vet staticcheck test
	@echo "All checks passed!"

# CI target for continuous integration
ci: fmt vet staticcheck test-all
	@echo "CI checks completed!" 
# Lean proofs and the differential oracle tests
proofs:
	cd proofs && lake build
	go test -count=1 -tags lean ./stomptext ./internal/session

# Run every fuzz target for FUZZTIME each (default 30s)
FUZZTIME ?= 30s
fuzz:
	go test -run '^$$' -fuzz '^FuzzDecode$$' -fuzztime $(FUZZTIME) ./stomptext
	go test -run '^$$' -fuzz '^FuzzEncode$$' -fuzztime $(FUZZTIME) ./stomptext
	go test -run '^$$' -fuzz '^FuzzDecodeErrorsAndLimits$$' -fuzztime $(FUZZTIME) ./stomptext
	go test -run '^$$' -fuzz '^FuzzSession$$' -fuzztime $(FUZZTIME) ./internal/session
	go test -run '^$$' -fuzz '^FuzzClient$$' -fuzztime $(FUZZTIME) .

# Model-check the client's concurrency with TLC (TLA2TOOLS=path/to/tla2tools.jar)
tla:
	proofs/tla/check.sh
