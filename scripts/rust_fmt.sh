#!/bin/bash


option="--check"

if [ "$1" == "--fix" ]; then
    option=""
    shift
fi

cargo +nightly-2025-05-01 fmt $option --all -- "$@"
