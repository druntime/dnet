# Length-delimited (framing) codec

A [`dnet::io::length_delimited::Codec`](https://docs.rs/dnet/latest/dnet/io/length_delimited/struct.Codec.html) prefixes each frame with its length in bytes.

The length prefix is used to divide the continuous byte stream into discrete frames: the codec reads the 4-byte length, then reads that many bytes as a single message frame.

Note: A `u32` (4 bytes) is used to encode the message length. The 4 length-prefix bytes themselves are not counted in that length; the length value refers only to the subsequent message bytes.
