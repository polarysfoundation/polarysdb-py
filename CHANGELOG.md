# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [1.0.0] - 2026-05-12

### Added
- Go-compatible state DB layout and path resolution: `state/state.rdb` (mirrors Go `modules/config`).
- Go-compatible WAL framing (little-endian length + CRC32 + proto3 WALEntry bytes).
- Legacy reader for old Python `PLRD...` state files (auto-migrates on next save).
- Automatic index rebuilding when data is imported or reloaded.
- Core PolarysDB engine with support for encryption, WAL, transactions, and indexing.

### Changed
- State DB binary format now matches Go: `nonce || AES-GCM(ciphertext)` (no `PLRD/version/CRC32` header).
- Transaction commit and batch writes now keep WAL + indexes + metrics consistent.
- `Key` conversion now matches Go `BytesToKey` semantics (left zero-pad / keep last 32 bytes).

### Fixed
- WAL recovery ordering: snapshot load happens before WAL replay; recovery is checkpointed and WAL is truncated after a successful replay.

