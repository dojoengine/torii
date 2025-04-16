#!/bin/bash

# Usage:
# ./scripts/rebuild_test_artifacts.sh /path/to/sozo /path/to/katana

# When tests are run, the `build.rs` of `dojo-test-utils` is re-building the
# cairo artifacts ONLY if they don't exist.
# This script gives an easy way to remove those artifacts.

# Ensure to use the correct version of Sozo and Katana.
# TODO: when dojoup will be available with the new components, it will be
# easy to choose the toolchain version.
# Using also asdf with .tool-versions file could help when running this script
# from torii directory.

# For now, we use sozo and katana from input arguments.
sozo_path="$1"
katana_path="$2"

# Check that sozo path and katana path are provided.
if [ -z "$sozo_path" ] || [ -z "$katana_path" ]; then
    echo "Usage: ./scripts/rebuild_test_artifacts.sh /path/to/sozo /path/to/katana"
    exit 1
fi

# Manual forced cleanup.
rm -rf examples/spawn-and-move/target
rm -rf crates/types-test/target

# Re-run the minimal tests, this will re-build the projects + generate the build artifacts.
"$sozo_path" build --manifest-path examples/spawn-and-move/Scarb.toml
"$sozo_path" build --manifest-path crates/types-test/Scarb.toml

# Generates the database for testing by migrating the spawn and move example.
KATANA_RUNNER_BIN="$katana_path" cargo generate-test-db

# Extracts the database for testing.
bash ./scripts/extract_test_db.sh
