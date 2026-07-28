# Transferable dnet example

Pass `OffscreenCanvas` to a web worker using `TransferableTransport` and perform drawing operations on it in the worker thread.

## Usage

Navigate to example directory.

Build using `build.sh` script:
```bash
./build.sh
```

Navigate to `www` directory and start a HTTP server there (you may use [Host These Things Please](https://github.com/thecoshman/http) for that purpose):
```bash
cd www
http
```
And open the site in the browser.

## See also

[Transferable objects](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects) - Transferable objects MDN documentation page.
