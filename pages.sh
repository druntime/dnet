#!/bin/sh
set -e

# Generate GitHub Pages content for this project.
# Currently this script builds example WebAssembly sites and copies their output
# into docs/examples. In the future it will also generate the project book.

ROOT="$(cd "$(dirname "$0")" && pwd)"
EXAMPLES="message_port transferable transferable_rpc webworker"

for example in $EXAMPLES; do
  echo "Building $example..."
  cd "$ROOT/dnet/examples/$example"
  ./build.sh
done

for example in $EXAMPLES; do
  src="$ROOT/dnet/examples/$example/www"
  dst="$ROOT/docs/examples/$example"

  echo "Copying $example/www to docs/examples/$example..."
  rm -rf "$dst"
  mkdir -p "$dst"
  cp -R "$src/." "$dst/"
done

echo Generated GitHub pages successfully!
exit 0
