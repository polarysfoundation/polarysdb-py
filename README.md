# 🗄️ PolarysDB — Python Edition

[![Version](https://img.shields.io/badge/version-1.1.0-blue.svg)](https://github.com/polarysfoundation/polarysdb-py)
[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=flat&logo=python)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Go version](https://img.shields.io/badge/Go%20version-compatible-00ADD8?style=flat)](https://github.com/polarysfoundation/polarysdb)

> **Python port of [PolarysDB (Go)](https://github.com/polarysfoundation/polarysdb) — same storage format, same API, full cross-language compatibility.**

PolarysDB-Python is an embedded key-value database with AES-256-GCM encryption, Write-Ahead Log (WAL), ACID transactions, and in-memory hash indexes. A database file written by the Go version can be opened by the Python version and vice-versa.

---

## ✨ Key Features

| Feature | Detail |
|---|---|
| 🔒 **AES-256-GCM encryption** | Every `.db` file is encrypted at rest |
| ✅ **CRC32 integrity checks** | Detects file corruption automatically |
| 📝 **Write-Ahead Log (WAL)** | Survives crashes; replayed on startup |
| ⚡ **Async write buffer** | Group-commit batching, same as Go version |
| 🔄 **ACID transactions** | Snapshot isolation, commit / rollback |
| 🔍 **Hash indexes** | O(1) field-value lookups |
| 💾 **Automatic backups** | Time-based rotation with configurable retention |
| 📊 **Real-time metrics** | Reads, writes, latency, save durations |
| 🔗 **Cross-language** | JSON export fully compatible with Go's `Export()` |

---

## 📦 Installation

```bash
pip install polarysdb          # from PyPI (once published)
# — or —
pip install .                  # from source
```

**Requires:** Python 3.9+, `cryptography >= 41.0`

---

## 🚀 Quick Start

```python
from polarysdb import init, Key

# Create a 32-byte encryption key — identical to Go:
#   var key common.Key
#   copy(key[:], []byte("my-secret-encryption-key-32b"))
key = Key("my-secret-encryption-key-32b")

# Initialize database
db = init(key, "./data", debug=False)

# Create a table
db.create("users")

# Write data
db.write("users", "user1", {
    "name":  "Alice",
    "email": "alice@example.com",
    "age":   30,
})

# Read data
value, exists = db.read("users", "user1")
if exists:
    print("User:", value)

# Delete data
db.delete("users", "user1")

db.close()
```

---

## 📚 Advanced Usage

### Custom Configuration

```python
from polarysdb import init_with_config, Config, Key

cfg = Config(
    dir_path        = "./data",
    backup_dir      = "./backups",
    encryption_key  = Key("my-secret-encryption-key-32b"),
    enable_wal          = True,
    enable_backup       = True,
    enable_indexes      = True,
    enable_transactions = True,
    save_interval       = 10.0,   # seconds
    buffer_size         = 2000,
    debug               = True,
)
db = init_with_config(cfg)
```

### ACID Transactions

```python
db.create("accounts")
db.write("accounts", "alice", {"balance": 900})
db.write("accounts", "bob",   {"balance": 600})

tx = db.begin_transaction()
tx.write("accounts", "alice", {"balance": 850})
tx.write("accounts", "bob",   {"balance": 650})

try:
    db.commit_transaction(tx)
except Exception as exc:
    tx.rollback()
```

### Fast Lookups with Indexes

```python
db.create("products")
db.write("products", "p1", {"name": "Phone",  "category": "Electronics"})
db.write("products", "p2", {"name": "Laptop", "category": "Electronics"})

db.create_index("products", "category")

results = db.query_by_index("products", "category", "Electronics")
for product in results:
    print(product)
```

### Batch Operations

```python
import time

db.create("logs")
batch = {
    f"log{i}": {"timestamp": time.time(), "message": f"Entry {i}"}
    for i in range(1000)
}
db.write_batch("logs", batch)   # 10× faster than individual writes
```

### Backup & Restore

```python
# Plain JSON — cross-language compatible with the Go version
db.export(key, "./backup.json")
db.import_db(key, "./backup.json")

# Encrypted binary — same format as Go's ExportEncrypted / ImportEncrypted
db.export_encrypted(key, "./backup.enc")
db.import_encrypted(key, "./backup.enc")
```

### Key Rotation

```python
old_key = Key("my-secret-encryption-key-32b")
new_key = Key.generate()   # random 32-byte key
db.change_key(old_key, new_key)
```

### Monitoring & Metrics

```python
metrics = db.get_metrics()
print("Total Reads:        ", metrics.total_reads)
print("Total Writes:       ", metrics.total_writes)
print("Avg Read Latency:   ", f"{metrics.avg_read_latency * 1000:.3f} ms")
print("Avg Write Latency:  ", f"{metrics.avg_write_latency * 1000:.3f} ms")

status = db.get_status()
print("Status:", status)
```

### Context manager

```python
with init(key, "./data") as db:
    db.create("items")
    db.write("items", "k", {"v": 1})
# db.close() is called automatically
```

---

## 🔗 Cross-Language Compatibility with Go

The Python and Go versions share the **same storage layout and file format**:

- State DB path: `state/state.rdb` under the configured `dir_path` (same convention as Go)
- WAL path: `polarysdb.wal` under `dir_path`

### State DB binary format (`state.rdb`)

**Go-compatible (current default):**

```
[12 bytes] AES-GCM nonce
[N bytes]  AES-256-GCM ciphertext  →  decrypts to UTF-8 JSON
```

**Legacy Python v1 (still readable; auto-migrated on next save):**

```
[4 bytes]  magic        "PLRD"
[4 bytes]  version      0x00000001
[4 bytes]  CRC32 of plaintext payload
[12 bytes] AES-GCM nonce
[N bytes]  AES-256-GCM ciphertext  →  decrypts to UTF-8 JSON
```

### Open a Go database from Python

```python
from polarysdb import init, Key
import shutil, os

# Copy the Go-generated state.rdb file into a fresh data directory
os.makedirs("./py_data/state", exist_ok=True)
shutil.copy("./go_data/state/state.rdb", "./py_data/state/state.rdb")

key = Key("same-key-used-in-go-32bytes!!")
db  = init(key, "./py_data")

value, ok = db.read("users", "alice")
print(value)   # same data written by Go
db.close()
```

### Use Export/Import for guaranteed compatibility

The safest cross-language path is through plain JSON:

```go
// Go — export
err := db.Export(key, "./shared.json")
```

```python
# Python — import
db.import_db(key, "./shared.json")
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     PUBLIC API                          │
│  create | write | read | delete | transactions          │
└────────────────────┬────────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   ┌────────┐  ┌─────────┐  ┌──────────┐
   │ Async  │  │  Hash   │  │   WAL    │
   │ Buffer │  │ Indexes │  │ (binary) │
   └────┬───┘  └────┬────┘  └────┬─────┘
        │           │            │
        └───────────┼────────────┘
                    ▼
          ┌──────────────────┐
          │  Storage Engine  │
          │  AES-256-GCM     │
          │  CRC32 checksum  │
          │  Atomic writes   │
          └────────┬─────────┘
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
   ┌──────┐  ┌────────┐  ┌────────┐
   │ .db  │  │  .wal  │  │ backup │
   │ File │  │  File  │  │  Dir   │
   └──────┘  └────────┘  └────────┘
```

### Module Structure

| Module | Purpose |
|---|---|
| `polarysdb/database.py` | Core `Database` class, write buffer, background workers |
| `modules/storage.py` | AES-256-GCM encryption, CRC32, atomic file writes |
| `modules/wal.py` | Write-Ahead Log with length-prefixed binary frames |
| `modules/index.py` | Hash-based in-memory index manager |
| `modules/tx.py` | ACID transaction manager with snapshot isolation |
| `modules/backup.py` | Automatic time-based backup with rotation |
| `modules/metrics.py` | Thread-safe performance metrics collector |
| `modules/common.py` | `Key` type (matches Go `common.Key [32]byte`) |
| `modules/logger.py` | Structured logger |

---

## 📖 API Reference

```python
# Lifecycle
db = polarysdb.init(key, dir_path, debug=False)
db = polarysdb.init_with_config(cfg)
db.close()
db.close_with_timeout(seconds)

# Table operations
db.exist(table)           → bool
db.create(table)

# Data operations
db.write(table, key, value)
db.write_batch(table, {key: value, ...})
db.read(table, key)       → (value, bool)
db.read_batch(table)      → [values]
db.delete(table, key)

# Index operations
db.create_index(table, field)
db.query_by_index(table, field, value)  → [values]

# Transaction operations
tx = db.begin_transaction()
tx.write(table, key, value)
tx.delete(table, key)
tx.read(table, key)        → (value, bool)
db.commit_transaction(tx)
tx.rollback()

# Backup / Export / Import
db.export(key, path)                    # plain JSON
db.export_encrypted(key, path)          # encrypted binary
db.import_db(key, path)                 # from plain JSON
db.import_encrypted(key, path)          # from encrypted binary
db.change_key(old_key, new_key)

# Monitoring
db.get_metrics()  → MetricsSnapshot
db.get_status()   → dict
```

---

## 🧪 Running Tests

```bash
python3 -m unittest discover -s tests -p 'test*.py' -q
```

---

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). For user-visible changes, please add an entry under **Unreleased** in [CHANGELOG.md](CHANGELOG.md).

## 📄 License

MIT — see [LICENSE](LICENSE)

## 🔗 Related

- [PolarysDB (Go)](https://github.com/polarysfoundation/polarysdb) — original implementation
- [Polarys Foundation](https://polarys.foundation)
