# Torii - Automatic Indexer for Dojo

Torii is a specialized indexer designed for Dojo games running on Starknet. It indexes and tracks onchain data changes, providing a comprehensive view of game state and history.

## Building

```bash
# Torii binary.
cargo build --bin torii

# Workspace.
cargo build --workspace
```

## Running Tests

Currently, the tests are based on pre-built Katana database to avoid
migrating the example each time (which takes a long time).

If you just pulled the repo, the pre-built databases are present, and you can uncompress them by running:

```bash
bash scripts/extract_test_db.sh
```

This will copy the databases in the `tmp` directory, ready to be used by the tests.

If you are working on Torii, then you will want to update the databases everytime you actually change the example cairo contracts. If you use `asdf`, the `.tool-versions` file will automatically install the correct version of Sozo and Katana. Soon the `dojoup` script will be updated to easily choose the toolchain version and the one to use too.

To rebuild the test artifacts, you can use the following script:

```bash
bash scripts/rebuild_test_artifacts.sh /path/to/sozo /path/to/katana
```

Finally, you can run the tests with:

```bash
# Use nextest to run the tests, adapt to your computer's resources.
KATANA_RUNNER_BIN=katana cargo nextest run --all-features --workspace --build-jobs 12
```

## Linting

To check linting, several scripts are available. They also usually have a `--fix` option to automatically fix the linting errors.

```bash
bash scripts/rust_fmt.sh (--fix)

bash scripts/clippy.sh
```


## Documentation

For more detailed documentation, visit the [Dojo Book](https://book.dojoengine.org/toolchain/torii).

## License

This project is licensed under the Apache-2.0 License - see the [LICENSE](LICENSE) file for details.
