#!/bin/sh

set -e

# Open book locally.

cd book
mdbook serve --port 8500 --open
cd ..
