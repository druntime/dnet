#!/bin/sh
set -e

# Generate GitHub Pages content for this project.
# Currently this script builds example WebAssembly sites and copies their output
# into docs/examples. In the future it will also generate the project book.

ROOT="$(cd "$(dirname "$0")" && pwd)"

echo Generating online example demos...
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

echo Generating book...
cd "$ROOT/book"
mdbook build

echo Copying generated book to docs/book...
rm -rf "$ROOT/docs/book"
mkdir -p "$ROOT/docs/book"
cp -R "$ROOT/book/book/." "$ROOT/docs/book/"

echo Generated GitHub pages successfully!
exit 0
