#!/bin/bash


option="--check"

if [ "$1" == "--fix" ]; then
    option=""
    shift
fi

cargo +nightly-2025-02-20 fmt $option --all -- "$@"
