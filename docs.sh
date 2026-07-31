#!/bin/sh

set -e

# Open docs locally.

# future todo: 
# remove '--no-deps' - currently there's a bug in dependency preventing this from working
RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --workspace --all-features --open --no-deps
