# keyvaluestore-rs

Like [ccbrown/keyvaluestore](https://github.com/ccbrown/keyvaluestore) but for Rust.

Abstracts a key-value store interface over AWS DynamoDB, Redis, and in-memory. Allows dynamic backend selection, and provides a read cache wrapper for these backends.
