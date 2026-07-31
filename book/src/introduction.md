# Introduction

Welcome to the **dnet** book.

`dnet` is a unified messaging abstraction library for Rust that makes it easy to send and receive messages over multiple transport protocols with a consistent async interface.

The core idea behind `dnet` is to delegate message encoding and decoding to [serde](https://serde.rs/). This allows library users to focus on the networking logic of their application instead of spending time on low-level details of the specific transport protocol used to send messages.

## Work in progress

This book is an early-stage work in progress and will continue to evolve.

## Security

Read the [security](./security.md) section first, before using the `dnet` crate.

## What you will learn

This book is designed to help you understand:

- What `dnet` is and when to use it
- How to work with common transports such as TCP, UDP, QUIC, WebSocket, MessagePort, and transport for communication with web workers
- How to build applications that share messaging logic between native and browser environments
- How to extend `dnet` with new transports or custom codecs

## Getting started

If you are new to `dnet`, it's best to start by learning the [core concepts](./core-concepts.md) of `dnet` and then explore the [examples](https://github.com/druntime/dnet/tree/main/dnet/examples).
