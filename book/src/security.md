# Security

Security is intentionally left out of scope of the `dnet` crate.

`dnet` project provides a transport abstraction and networking implementation helpers, it focuses on flexible transport composition and message delivery semantics, but it does not provide encryption, authentication, or other security primitives by itself.

## Recommended security model

- Use `dnet` over insecure transports only in trusted or local environments.
- For untrusted networks, rely on a secure underlying transport layer (like TCP + TLS or QUIC - assuming secure configuration).

Always read underlying transport documentation to learn about its security semantics. 

## Implementing secure protocols over `dnet` transports 

Building a custom secure protocol on top of `dnet` transports, wrapping insecure lower-level protocols - like UDP or raw TCP without TLS - is technically possible, but not recommended for most users.

If you choose to do this, follow these guidelines:

1. Prefer well-vetted cryptographic libraries and protocols.
2. Keep cryptographic responsibilities separate from the `dnet` transport logic.
3. Avoid inventing custom encryption schemes unless you are a cryptography expert.

For most applications it's best to use an existing underlying secure transport layer instead of building security directly into `dnet` messages.
