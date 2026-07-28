#!/bin/sh
set -e

cargo build

wasm-pack build --release --target web --out-name script
mv pkg/script_bg.wasm www/script_bg.wasm
mv pkg/script.js www/script.js
