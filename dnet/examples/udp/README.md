# UDP dnet example

Broadcast server mouse position over UDP.

## Usage

Navigate to `dnet` root directory.

Start the server:
```bash
cargo run --example udp -- --server
```

Start the client (in another terminal window/tab):
```bash
cargo run --example udp
```

## See also

[device_query](https://github.com/ostrosco/device_query) - crate by [ostrosco](https://github.com/ostrosco) 
used in this example to query current mouse position.
