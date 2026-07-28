#!/bin/bash

set -Eeuo pipefail
trap 'echo "Failed at line $LINENO"; exit 1' ERR

if ! cmp -s README.md dnet/README.md; then
    echo "ERROR: README.md and dnet/README.md differ"
    exit 1
fi

echo Starting tests server...
cargo run -p tests-server &

clean_up () {
    kill $!
} 
trap clean_up EXIT

sleep .5

echo Running native tests...
cargo test

echo Building tests webworker...
cd tests-webworker
wasm-pack build --release --target web
cargo run
cd ..

echo Running web tests...
cd dnet
wasm-pack test --firefox --headless
cd ..

cd dnet-utils
wasm-pack test --firefox --headless
cd ..

cd dnet-js
wasm-pack test --firefox --headless
cd ..

cd dnet-rpc
wasm-pack test --firefox --headless
cd ..

echo Tests passed successfully!
exit 0
