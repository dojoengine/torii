#!/bin/bash

# Usage:
# ./scripts/rebuild_test_artifacts.sh [--sozo path/to/sozo] [--katana path/to/katana]
# If no paths provided, tools will be used from PATH

# Default to PATH tools, which will be defined by the `.tools-versions` file.
sozo_path="sozo"
katana_path="katana"

# Global variables
KATANA_PID=""
KATANA_PORT=5050
KATANA_HOST="0.0.0.0"

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --sozo PATH     Path to sozo binary (default: use from PATH)"
    echo "  --katana PATH   Path to katana binary (default: use from PATH)"
    echo "  -h, --help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Use tools from PATH"
    echo "  $0 --sozo /path/to/sozo              # Use specific sozo, katana from PATH"
    echo "  $0 --katana /path/to/katana          # Use specific katana, sozo from PATH"
    echo "  $0 --sozo /path/to/sozo --katana /path/to/katana  # Use both specific paths"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --sozo)
            sozo_path="$2"
            shift 2
            ;;
        --katana)
            katana_path="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Function to display script info
display_info() {
    echo "=========================================="
    echo "Torii Test Artifacts Rebuild Script"
    echo "=========================================="
    echo "Using sozo: $sozo_path"
    echo "Using katana: $katana_path"
    echo "=========================================="
    echo ""
}

# Display info and check tools
display_info

# Verify that the tools exist
if ! command -v "$sozo_path" &> /dev/null; then
    echo "Error: sozo not found at '$sozo_path'"
    echo ""
    show_usage
    exit 1
fi

if ! command -v "$katana_path" &> /dev/null; then
    echo "Error: katana not found at '$katana_path'"
    echo ""
    show_usage
    exit 1
fi

# Function to cleanup on exit
cleanup() {
    echo "Cleaning up..."
    stop_katana
    # Clean up any remaining Katana processes
    pkill -f "katana" 2>/dev/null || true
}

# Function to start Katana
start_katana() {
    local db_dir="$1"
    
    echo "Starting Katana on port $KATANA_PORT with db_dir: $db_dir..."
    "$katana_path" --dev --db-dir "$db_dir" > /tmp/katana.log 2>&1 &
    KATANA_PID=$!
    
    # Wait for Katana to be ready
    echo "Waiting for Katana to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:$KATANA_PORT > /dev/null 2>&1; then
            echo "Katana is ready!"
            return 0
        fi
        if [ $i -eq 30 ]; then
            echo "Katana failed to start within 30 seconds"
            return 1
        fi
        sleep 1
    done
}

# Function to stop Katana
stop_katana() {
    if [ ! -z "$KATANA_PID" ]; then
        echo "Stopping Katana (PID: $KATANA_PID)..."
        kill $KATANA_PID 2>/dev/null || true
        wait $KATANA_PID 2>/dev/null || true
        KATANA_PID=""
    fi
}

# Function to migrate a project
migrate_project() {
    local manifest_path="$1"
    local project_name="$2"
    local db_dir="./${project_name}-db"
    
    echo "Migrating $project_name..."
    
    # Create database directory for this project
    mkdir -p "$db_dir"
    
    start_katana "$db_dir" || return 1
    
    echo "Running migration for $project_name..."
    if ! "$sozo_path" migrate --manifest-path "$manifest_path"; then
        echo "Migration failed for $project_name"
        stop_katana
        return 1
    fi
    
    echo "Migration completed for $project_name"
    stop_katana
    return 0
}

# Function to cleanup old artifacts
cleanup_artifacts() {
    echo "Cleaning up old artifacts..."
    rm -rf examples/spawn-and-move/target
    rm -rf crates/types-test/target
    rm -rf ./spawn-and-move-db
    rm -rf ./types-test-db
}

# Function to compress database
compress_database() {
    local db_path="$1"
    local compressed_path="$2"
    
    echo "Compressing database: $db_path -> $compressed_path"
    
    # Remove existing compressed file if it exists
    rm -f "$compressed_path"
    
    # Compress the database directory
    if tar -czf "$compressed_path" -C "." "$(basename "$db_path")"; then
        echo "Successfully compressed $db_path to $compressed_path"
        return 0
    else
        echo "Failed to compress $db_path"
        return 1
    fi
}

# Set up trap to cleanup on exit.
trap cleanup EXIT INT TERM

main() {
    echo "Starting test artifacts rebuild..."

    cleanup_artifacts

    echo "Building Cairo projects..."
    "$sozo_path" build --manifest-path examples/spawn-and-move/Scarb.toml
    "$sozo_path" build --manifest-path crates/types-test/Scarb.toml

    migrate_project "examples/spawn-and-move/Scarb.toml" "spawn-and-move" || exit 1
    migrate_project "crates/types-test/Scarb.toml" "types-test" || exit 1
    
    compress_database "./spawn-and-move-db" "./spawn-and-move-db.tar.gz" || exit 1
    compress_database "./types-test-db" "./types-test-db.tar.gz" || exit 1
    
    echo "Extracting test database..."
    bash ./scripts/extract_test_db.sh

    echo "Test artifacts rebuilt successfully!"
}

main