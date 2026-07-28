# WebSocket dnet example

Simple chat application.

## Usage

Navigate to example directory.

Build using `build.sh` script:
```bash
./build.sh
```

Start the server:
```bash
cargo run --example websocket -- --server
```

Start the native client (in another terminal window):
```bash
cargo run --example websocket
```

Or start the web client by navigating to the server address in your browser.

## See also

[WebSockets API](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API) - WebSockets MDN documentation page.

[rustyline-async](https://github.com/zyansheep/rustyline-async) - crate by [zyansheep](https://github.com/zyansheep)
used in this example to create a command prompt interface for the native client.
