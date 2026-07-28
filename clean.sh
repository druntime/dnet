#!/bin/bash

set -Eeuo pipefail
trap 'echo "Failed at line $LINENO"; exit 1' ERR

cargo clean

cd tests-webworker
cargo clean
rm -rf pkg
cd ..

cd dnet/examples

cd message_port
rm -rf pkg
cd www
rm -f script.js
rm -f script_bg.wasm
cd ..
cd ..

cd transferable
rm -rf pkg
cd www
rm -f script.js
rm -f script_bg.wasm
cd ..
cd ..

cd transferable_rpc
rm -rf pkg
cd www
rm -f script.js
rm -f script_bg.wasm
cd ..
cd ..

cd webworker
rm -rf pkg
cd www
rm -f script.js
rm -f script_bg.wasm
cd ..
cd ..

cd websocket
rm -rf pkg
cd www
rm -f script.js
rm -f script_bg.wasm
cd ..
cd ..

cd ../..
