#!/bin/bash

# Selective testing script for Torii workspace
# This script detects which crates have changes and runs tests only for those crates
# Usage: ./scripts/selective_test.sh [base_branch] [--dry-run] [--force-all]

set -e

# Default values
BASE_BRANCH="${1:-main}"
DRY_RUN=false
FORCE_ALL=false
NEXTEST_ARGS="--all-features --build-jobs 20"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --force-all)
            FORCE_ALL=true
            shift
            ;;
        --nextest-args)
            NEXTEST_ARGS="$2"
            shift 2
            ;;
        *)
            if [[ -z "$BASE_BRANCH" ]]; then
                BASE_BRANCH="$1"
            fi
            shift
            ;;
    esac
done

echo "🔍 Selective testing for Torii workspace"
echo "Base branch: $BASE_BRANCH"
echo "Dry run: $DRY_RUN"
echo "Force all: $FORCE_ALL"
echo ""

# Function to get all workspace members
get_workspace_members() {
    cargo metadata --format-version 1 --no-deps | jq -r '.workspace_members[]' | sed 's/ .*//'
}

# Function to map file paths to crate names
get_crate_for_file() {
    local file_path="$1"

    # Handle root workspace files
    if [[ "$file_path" == "Cargo.toml" ]] || [[ "$file_path" == "Cargo.lock" ]]; then
        echo "workspace-root"
        return
    fi

    # Handle bin directory
    if [[ "$file_path" == bin/* ]]; then
        echo "torii"
        return
    fi

    # Handle crates directory
    if [[ "$file_path" == crates/* ]]; then
        # Extract crate name from path like crates/sqlite/sqlite/src/lib.rs -> sqlite
        # or crates/graphql/src/lib.rs -> graphql
        local crate_path=$(echo "$file_path" | cut -d'/' -f2-)
        local crate_name=$(echo "$crate_path" | cut -d'/' -f1)

        # Handle nested crates like sqlite/sqlite -> sqlite
        if [[ -d "crates/$crate_name/$crate_name" ]]; then
            echo "$crate_name"
        else
            echo "$crate_name"
        fi
        return
    fi

    # Handle examples directory
    if [[ "$file_path" == examples/* ]]; then
        echo "examples"
        return
    fi

    # Handle other directories that might affect all crates
    if [[ "$file_path" == scripts/* ]] || [[ "$file_path" == .github/* ]] || [[ "$file_path" == migrations/* ]]; then
        echo "workspace-root"
        return
    fi

    echo "unknown"
}

# Function to get package name for cargo commands
get_package_name() {
    local crate_name="$1"

    case "$crate_name" in
        "sqlite")
            echo "torii-sqlite"
            ;;
        "graphql")
            echo "torii-graphql"
            ;;
        "server")
            echo "torii-server"
            ;;
        "cli")
            echo "torii-cli"
            ;;
        "broker")
            echo "torii-broker"
            ;;
        "client")
            echo "torii-client"
            ;;
        "messaging")
            echo "torii-messaging"
            ;;
        "storage")
            echo "torii-storage"
            ;;
        "cache")
            echo "torii-cache"
            ;;
        "math")
            echo "torii-math"
            ;;
        "controllers")
            echo "torii-controllers"
            ;;
        "processors")
            echo "torii-processors"
            ;;
        "proto")
            echo "torii-proto"
            ;;
        "typed-data")
            echo "torii-typed-data"
            ;;
        "adigraphmap")
            echo "torii-adigraphmap"
            ;;
        "task-network")
            echo "torii-task-network"
            ;;
        *)
            echo "$crate_name"
            ;;
    esac
}

# Get changed files
if command -v git >/dev/null 2>&1 && git rev-parse --git-dir >/dev/null 2>&1; then
    echo "📋 Getting changed files since $BASE_BRANCH..."

    # Try to get the merge base, fallback to direct comparison
    if git merge-base "$BASE_BRANCH" HEAD >/dev/null 2>&1; then
        MERGE_BASE=$(git merge-base "$BASE_BRANCH" HEAD)
        CHANGED_FILES=$(git diff --name-only "$MERGE_BASE"..HEAD)
    else
        echo "⚠️  Could not find merge base with $BASE_BRANCH, comparing directly"
        CHANGED_FILES=$(git diff --name-only "$BASE_BRANCH"..HEAD 2>/dev/null || git diff --name-only --cached)
    fi
else
    echo "⚠️  Not in a git repository or git not available, running all tests"
    FORCE_ALL=true
fi

if [[ "$FORCE_ALL" == "true" ]]; then
    echo "🚀 Running all tests (forced or no git info available)"
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "Would run: cargo nextest run $NEXTEST_ARGS --workspace"
    else
        cargo nextest run $NEXTEST_ARGS --workspace
    fi
    exit 0
fi

if [[ -z "$CHANGED_FILES" ]]; then
    echo "✅ No changes detected, skipping tests"
    exit 0
fi

echo "📝 Changed files:"
echo "$CHANGED_FILES" | sed 's/^/  /'
echo ""

# Determine affected crates
declare -A affected_crates
workspace_root_changed=false

while IFS= read -r file; do
    if [[ -n "$file" ]]; then
        crate=$(get_crate_for_file "$file")
        if [[ "$crate" == "workspace-root" ]]; then
            workspace_root_changed=true
        elif [[ "$crate" != "unknown" ]] && [[ "$crate" != "examples" ]]; then
            affected_crates["$crate"]=1
        fi
    fi
done <<< "$CHANGED_FILES"

# If workspace root files changed, test everything
if [[ "$workspace_root_changed" == "true" ]]; then
    echo "🌍 Workspace root files changed, running all tests"
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "Would run: cargo nextest run $NEXTEST_ARGS --workspace"
    else
        cargo nextest run $NEXTEST_ARGS --workspace
    fi
    exit 0
fi

# If no specific crates affected, run all tests
if [[ ${#affected_crates[@]} -eq 0 ]]; then
    echo "🤷 No specific crates identified, running all tests as fallback"
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "Would run: cargo nextest run $NEXTEST_ARGS --workspace"
    else
        cargo nextest run $NEXTEST_ARGS --workspace
    fi
    exit 0
fi

echo "🎯 Affected crates:"
for crate in "${!affected_crates[@]}"; do
    package_name=$(get_package_name "$crate")
    echo "  - $crate (package: $package_name)"
done
echo ""

# Add dependent crates based on common dependencies
declare -A final_crates
for crate in "${!affected_crates[@]}"; do
    final_crates["$crate"]=1

    # Add common dependents
    case "$crate" in
        "sqlite")
            final_crates["server"]=1
            final_crates["graphql"]=1
            ;;
        "processors")
            final_crates["server"]=1
            ;;
        "storage")
            final_crates["server"]=1
            final_crates["sqlite"]=1
            ;;
        "types")
            # types changes affect almost everything
            echo "🌐 Types changed, running all tests due to widespread impact"
            if [[ "$DRY_RUN" == "true" ]]; then
                echo "Would run: cargo nextest run $NEXTEST_ARGS --workspace"
            else
                cargo nextest run $NEXTEST_ARGS --workspace
            fi
            exit 0
            ;;
    esac
done

echo "🧪 Final test plan (including dependents):"
test_packages=()
for crate in "${!final_crates[@]}"; do
    package_name=$(get_package_name "$crate")
    echo "  - $package_name"
    test_packages+=("-p" "$package_name")
done
echo ""

# Run tests for affected crates
if [[ "$DRY_RUN" == "true" ]]; then
    echo "Would run: cargo nextest run $NEXTEST_ARGS ${test_packages[*]}"
else
    echo "🚀 Running tests for affected crates..."
    cargo nextest run $NEXTEST_ARGS "${test_packages[@]}"
fi

echo "✅ Selective testing completed!"
