## Agrona Broadcast buffer
This example demostrates ipc via shared memory.
Process running in rust can exchange bytes with jvm process via
Agrona shared memory buffers [Agrona Broadcast](https://github.com/real-logic/agrona/blob/master/agrona/src/main/java/org/agrona/concurrent/broadcast/)

Run a broadcast receiver
```rust
cargo  run  --release --example broadcast_rx

```

```rust
cargo  run  --release --example broadcast_tx

```