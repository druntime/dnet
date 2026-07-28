# TCP dnet example

Simple chat application.

## Usage

Navigate to `dnet` root directory.

First, start the server:
```bash
cargo run --example tcp -- --server
```

Start the client (in another terminal window):
```bash
cargo run --example tcp
```

## See also

[rustyline-async](https://github.com/zyansheep/rustyline-async) - crate by [zyansheep](https://github.com/zyansheep)
used in this example to create a command prompt interface.
