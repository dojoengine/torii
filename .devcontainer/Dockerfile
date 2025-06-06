ARG RUST_VERSION=1.85.0
FROM ubuntu:24.04
ARG RUST_VERSION

# Install additional packages
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends protobuf-compiler libprotobuf-dev libclang-dev libzstd-dev make pkg-config libssl-dev gcc

RUN apt-get install -y gh libgmp3-dev software-properties-common curl git unzip

# Install rust.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

# To allow independent workflow of the container, the rust-toolchain is explicitely given.
RUN echo ${RUST_VERSION} > rust_toolchain_version
# Make sure to sync the nightly version with the scripts in ./scripts
RUN echo "nightly-2025-02-20" > nightly_rust_toolchain_version

# Install cargo-binstall
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

RUN rustup toolchain install $(cat rust_toolchain_version) && \
    rustup default $(cat rust_toolchain_version) && \
    rustup component add clippy && \
    rustup component add rustfmt

RUN rustup toolchain install $(cat nightly_rust_toolchain_version) && \
        rustup component add rustfmt clippy --toolchain $(cat nightly_rust_toolchain_version)

RUN rustup target add x86_64-pc-windows-msvc && \
    rustup target add wasm32-unknown-unknown

RUN cargo binstall cargo-get cargo-nextest cargo-llvm-cov cargo-release --secure -y

# Platform specific tooling
ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ] ; then \
    curl -L https://github.com/Orange-OpenSource/hurl/releases/download/4.1.0/hurl-4.1.0-aarch64-unknown-linux-gnu.tar.gz -o hurl.tar.gz && \
    tar -xzf hurl.tar.gz && \
    mv hurl-4.1.0-aarch64-unknown-linux-gnu/hurl /usr/local/bin/ && \
    rm -r hurl-4.1.0-aarch64-unknown-linux-gnu && \
    rm hurl.tar.gz && \
    rustup component add llvm-tools-preview --toolchain $(cat rust_toolchain_version)-aarch64-unknown-linux-gnu; \
    curl -L https://github.com/asdf-vm/asdf/releases/download/v0.16.7/asdf-v0.16.7-linux-arm64.tar.gz -o asdf.tar.gz; \
    elif [ "$TARGETPLATFORM" = "linux/amd64" ] ; then \
    curl -L https://github.com/Orange-OpenSource/hurl/releases/download/4.1.0/hurl-4.1.0-x86_64-unknown-linux-gnu.tar.gz -o hurl.tar.gz && \
    tar -xzf hurl.tar.gz && \
    mv hurl-4.1.0-x86_64-unknown-linux-gnu/hurl /usr/local/bin/ && \
    rm -r hurl-4.1.0-x86_64-unknown-linux-gnu && \
    rm hurl.tar.gz && \
    rustup component add llvm-tools-preview --toolchain $(cat rust_toolchain_version)-x86_64-unknown-linux-gnu && \
    rustup target add x86_64-fortanix-unknown-sgx --toolchain $(cat nightly_rust_toolchain_version); \
    curl -L https://github.com/asdf-vm/asdf/releases/download/v0.16.7/asdf-v0.16.7-linux-amd64.tar.gz -o asdf.tar.gz; \
    fi

# For asdf to be installed for the current platform automatically.
RUN tar -C /usr/local/bin -xzf asdf.tar.gz

ENV PATH="/root/.asdf/shims:$PATH"

RUN asdf plugin add scarb
RUN asdf install scarb 2.9.4
