# 🚀 PolarysDB-Python v1.0.0 — The Foundation

We are excited to announce the official release of **PolarysDB-Python v1.0.0**. This milestone marks the first stable version of our high-performance, embedded key-value database, fully compatible with the [PolarysDB Go implementation](https://github.com/polarysfoundation/polarysdb).

PolarysDB is designed for developers who need a secure, durable, and extremely fast storage solution that works seamlessly across Python and Go environments.

---

## ✨ Release Highlights

- **🔒 Military-Grade Security**: Full AES-256-GCM encryption for all data at rest.
- **🔗 1:1 Go Compatibility**: Open and write to the same database files from both Python and Go.
- **⚡ ACID Durability**: Write-Ahead Logging (WAL) ensures your data survives crashes and power failures.
- **🔍 Blazing Fast Lookups**: In-memory hash indexing for $O(1)$ query performance on any field.
- **🔄 Seamless Migration**: Automatic migration path for legacy data formats.

---

## 🛠️ Key Features

### Robust Storage Engine
- **AES-256-GCM**: Industry-standard authenticated encryption.
- **CRC32 Integrity**: Automatic detection and reporting of file corruption.
- **Atomic Writes**: Zero-risk of partial writes during system failures.

### High Performance
- **Async Write Buffering**: Group-commit batching for high-throughput write workloads.
- **Thread-Safe**: Designed for concurrent access in multi-threaded Python applications.
- **Real-time Metrics**: Built-in monitoring for latency, throughput, and system health.

### Advanced Data Management
- **ACID Transactions**: Snapshot isolation with full commit/rollback support.
- **Flexible Indexing**: Create indexes on any JSON field for instant retrieval.
- **Automated Backups**: Time-based rotation and retention policies out of the box.

---

## 🌐 Cross-Language Interoperability

PolarysDB-Python isn't just a port; it's a peer. Version 1.0.0 implements the exact same storage layout as the Go version:

- **State Layout**: Uses the `state/state.rdb` path resolution matching Go.
- **WAL Framing**: Uses little-endian length prefixes and CRC32 checks compatible with the Go protobuf implementation.
- **Binary Format**: Decoupled from Python-specific headers for raw byte-level compatibility.

---

## 📦 Installation

Get started in seconds:

```bash
pip install polarysdb
```

*Requires Python 3.9+*

---

## 🚀 Quick Start

```python
from polarysdb import init, Key

# 32-byte encryption key
key = Key("my-secret-encryption-key-32b")

# Initialize
with init(key, "./data") as db:
    db.create("users")
    
    # Fast Write
    db.write("users", "u1", {"name": "Alice", "role": "admin"})
    
    # Fast Read
    user, exists = db.read("users", "u1")
    print(f"Found: {user['name']}")
```

---

## 📈 What's Next?

With the foundation laid in v1.0.0, our roadmap includes:
- [ ] Compaction and background GC for WAL files.
- [ ] Compression support (Zstd/Lz4).
- [ ] More complex query predicates.

---

## 🤝 Community & Support

- **Repository**: [github.com/polarysfoundation/polarysdb-py](https://github.com/polarysfoundation/polarysdb-py)
- **Documentation**: [Full API Reference](README.md#api-reference)
- **Issues**: [Report a bug](https://github.com/polarysfoundation/polarysdb-py/issues)

*Built with ❤️ by the Polarys Foundation.*
