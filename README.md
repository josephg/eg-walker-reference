# Diamond types sequence CRDT done simple

This is a simple reference implementation of the [diamond types sequence CRDT](https://github.com/josephg/diamond-types) algorithm in a few hundred lines of typescript.

The algorithm works by walking the (original) log of operations to reconstruct the CRDT metadata. I'm writing this up at the moment - hold tight for more details on how the algorithm works and why its nice.

This implementation is designed to be small, simple and readable. Based on early tests, it runs about 200x slower than the rust implementation. (But the implementation is also in 30x fewer lines of code). The implementation is designed to be fully compatible with diamond types.

The algorithm itself is in [src/oplog.ts](src/oplog.ts). Everything else is supporting code.