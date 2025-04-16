#!/bin/bash

option="--check"

if [ "$1" == "--fix" ]; then
    option=""
fi

scarb --manifest-path examples/spawn-and-move/Scarb.toml fmt $option
