#!/bin/bash

# STOMP Integration Test Runner
# This script helps set up and run integration tests with Apache ActiveMQ Artemis

set -e

# Configuration
COMPOSE_FILE="docker-compose.test.yml"
TIMEOUT=60
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
STOMP Integration Test Runner

Usage: $0 [OPTIONS] [COMMAND]

Commands:
    setup       Set up test environment (start Artemis STOMP server)
    test        Run integration tests
    cleanup     Stop and cleanup test environment
    full        Run complete test cycle (setup + test + cleanup)

Options:
    -t, --timeout SECONDS  Timeout for server startup [default: 60]
    -v, --verbose          Enable verbose output
    -h, --help             Show this help message

Examples:
    $0 full                    # Run complete test cycle
    $0 setup                   # Just start the test server
    $0 test                    # Run tests (assumes server is running)
    $0 cleanup                 # Stop and cleanup

Environment Variables:
    STOMP_TEST_SERVER          Override server address for tests (default: localhost:61613)

EOF
}

# Function to detect Docker Compose command
get_docker_compose_cmd() {
    if command -v "docker" &> /dev/null && docker compose version &> /dev/null; then
        echo "docker compose"
    elif command -v "docker-compose" &> /dev/null; then
        echo "docker-compose"
    else
        return 1
    fi
}

# Function to get STOMP server address
get_stomp_server_address() {
    echo "${STOMP_TEST_SERVER:-localhost:61613}"
}

# Function to check dependencies
check_dependencies() {
    local missing_deps=()
    
    if ! command -v docker &> /dev/null; then
        missing_deps+=("docker")
    fi
    
    if ! get_docker_compose_cmd &> /dev/null; then
        missing_deps+=("docker-compose or docker compose")
    fi
    
    if ! command -v go &> /dev/null; then
        missing_deps+=("go")
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        print_error "Missing dependencies: ${missing_deps[*]}"
        print_error "Please install the missing dependencies and try again."
        exit 1
    fi
}

# Function to wait for server to be ready
wait_for_server() {
    local server_address="$1"
    local timeout="$2"
    local start_time=$(date +%s)
    
    print_status "Waiting for Artemis STOMP server at $server_address (timeout: ${timeout}s)..."
    
    while true; do
        # Simple connectivity test using Docker's health check
        if docker ps --filter "name=stomp-test-artemis" --filter "health=healthy" | grep -q "stomp-test-artemis"; then
            print_success "Artemis STOMP server is ready!"
            return 0
        fi
        
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ $elapsed -ge $timeout ]; then
            print_error "Timeout waiting for Artemis STOMP server to be ready"
            print_error "Check container status and logs:"
            print_error "  docker ps -a | grep artemis"
            print_error "  docker logs stomp-test-artemis"
            return 1
        fi
        
        sleep 2
    done
}

# Function to setup test environment
setup_environment() {
    print_status "Setting up test environment with Apache ActiveMQ Artemis..."
    
    # Check if compose file exists
    if [ ! -f "$COMPOSE_FILE" ]; then
        print_error "Docker compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    # Get Docker Compose command
    local compose_cmd
    compose_cmd=$(get_docker_compose_cmd)
    if [ $? -ne 0 ]; then
        print_error "Docker Compose not available"
        exit 1
    fi
    
    # Get server address
    local server_address
    server_address=$(get_stomp_server_address)
    
    # Start Artemis
    print_status "Starting Apache ActiveMQ Artemis..."
    $compose_cmd -f "$COMPOSE_FILE" up -d artemis
    
    # Wait for server to be ready
    wait_for_server "$server_address" "$TIMEOUT"
    export STOMP_TEST_SERVER="$server_address"
    
    print_success "Test environment setup complete!"
    print_status "STOMP server: $STOMP_TEST_SERVER"
    print_status "Management console: http://localhost:8161/console/ (admin/admin)"
}

# Function to run integration tests
run_tests() {
    print_status "Running integration tests..."
    
    # Set server address if not already set
    if [ -z "$STOMP_TEST_SERVER" ]; then
        export STOMP_TEST_SERVER=$(get_stomp_server_address)
    fi
    
    local test_cmd="make test-integration"
    if [ "$VERBOSE" = true ]; then
        test_cmd="go test -v -tags=integration ./..."
    fi
    
    print_status "Test server: $STOMP_TEST_SERVER"
    print_status "Command: $test_cmd"
    
    if $test_cmd; then
        print_success "All integration tests passed!"
        return 0
    else
        print_error "Integration tests failed!"
        return 1
    fi
}

# Function to cleanup test environment
cleanup_environment() {
    print_status "Cleaning up test environment..."
    
    # Get Docker Compose command
    local compose_cmd
    compose_cmd=$(get_docker_compose_cmd)
    if [ $? -ne 0 ]; then
        print_warning "Docker Compose not available for cleanup"
        return 0
    fi
    
    # Stop all services
    $compose_cmd -f "$COMPOSE_FILE" down 2>/dev/null || true
    
    # Clean up volumes if they exist
    $compose_cmd -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    
    print_success "Cleanup complete!"
}

# Function to run full test cycle
run_full_cycle() {
    local exit_code=0
    
    print_status "Starting full integration test cycle with Apache ActiveMQ Artemis..."
    
    # Setup
    setup_environment || exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        # Run tests
        run_tests || exit_code=$?
    fi
    
    # Always cleanup
    cleanup_environment
    
    if [ $exit_code -eq 0 ]; then
        print_success "Full integration test cycle completed successfully!"
    else
        print_error "Integration test cycle failed!"
    fi
    
    return $exit_code
}

# Parse command line arguments
COMMAND=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        setup|test|cleanup|full)
            COMMAND="$1"
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Default command
if [ -z "$COMMAND" ]; then
    COMMAND="full"
fi

# Check dependencies
check_dependencies

# Execute command
case $COMMAND in
    setup)
        setup_environment
        ;;
    test)
        run_tests
        ;;
    cleanup)
        cleanup_environment
        ;;
    full)
        run_full_cycle
        ;;
    *)
        print_error "Unknown command: $COMMAND"
        show_usage
        exit 1
        ;;
esac 