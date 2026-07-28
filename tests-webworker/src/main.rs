#[cfg(not(target_arch = "wasm32"))]
fn main() {
    use base64::{prelude::BASE64_STANDARD, Engine};
    use std::{
        fs::{read, read_to_string, File},
        io::{BufWriter, Write},
    };

    let wasm = read("pkg/tests_webworker_bg.wasm")
        .expect("failed to open wasm file, build the project first");

    let wasm_base64 = BASE64_STANDARD.encode(wasm);

    let output = File::create("../tests-js/tests_webworker.js").unwrap();

    let mut writer = BufWriter::new(output);

    let js = read_to_string("pkg/tests_webworker.js")
        .expect("failed to open js file, build the project first");
    let line_count = js.lines().count();
    let js = js
        .lines()
        .take(line_count - 2)
        .collect::<Vec<&str>>()
        .join("\n");

    writeln!(writer, "{js}\n").unwrap();

    writeln!(writer, "const wasmBase64 = '{wasm_base64}';\n").unwrap();
    writeln!(writer, "const bytesString = atob(wasmBase64);").unwrap();
    writeln!(writer, "const bytes = new Uint8Array(bytesString.length);").unwrap();
    writeln!(
        writer,
        "for (let i = 0; i < bytesString.length; i++) {{ bytes[i] = bytesString.charCodeAt(i); }}"
    )
    .unwrap();
    writeln!(
        writer,
        "const blob = new Blob([bytes], {{ type: 'application/wasm' }});"
    )
    .unwrap();
    writeln!(writer, "const url = URL.createObjectURL(blob);").unwrap();
    writeln!(writer, "\n__wbg_init({{ module_or_path: url }});").unwrap();
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("this binary is not meant to be run on the wasm target");
}
