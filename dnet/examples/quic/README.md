# QUIC dnet example

Simple chat application.

Also: broadcast server mouse position.

A mix of the TCP and UDP examples, using QUIC as the transport protocol.<br>
Reliable QUIC streams are used for the chat application, while unreliable QUIC datagrams are used to broadcast server mouse position.

## Usage

Navigate to `dnet` root directory.

First, start the server:
```bash
cargo run --example quic -- --server
```

Start the client (in another terminal window):
```bash
cargo run --example quic
```

## See also

[quinn](https://github.com/quinn-rs/quinn) - Rust implementation of the QUIC transport protocol.

[rustyline-async](https://github.com/zyansheep/rustyline-async) - crate by [zyansheep](https://github.com/zyansheep)
used in this example to create a command prompt interface.

[device_query](https://github.com/ostrosco/device_query) - crate by [ostrosco](https://github.com/ostrosco) 
used in this example to query current mouse position.
