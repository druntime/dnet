# IPC dnet example

Simple chat application.

## Usage

Navigate to `dnet` root directory.

First, start server:
```bash
cargo run --example ipc -- --server
```

Start client (in another terminal window):
```bash
cargo run --example ipc
```

## See also

[parity-tokio-ipc](https://github.com/paritytech/parity-tokio-ipc) - interprocess transport for UNIX/Windows.
