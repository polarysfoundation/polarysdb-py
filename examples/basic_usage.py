"""
examples/basic_usage.py
Python examples mirroring every code snippet from the Go README.
"""

import time
import shutil
import os
from polarysdb import Key, Config, init, init_with_config


# ── 1. Basic Usage ─────────────────────────────────────────────────────────

key = Key("my-secret-encryption-key-32b")  # ← same as Go: copy(key[:], []byte("..."))

db = init(key, "./data", debug=False)

db.create("users")

user = {"name": "Alice", "email": "alice@example.com", "age": 30}
db.write("users", "user1", user)

value, exists = db.read("users", "user1")
if exists:
    print("User:", value)

db.delete("users", "user1")


# ── 2. Custom Configuration ────────────────────────────────────────────────

cfg = Config(
    dir_path="./data",
    backup_dir="./backups",
    encryption_key=key,
    enable_wal=True,
    enable_backup=True,
    enable_indexes=True,
    enable_transactions=True,
    save_interval=10.0,  # seconds  (Go: 10 * time.Second)
    buffer_size=2000,
    debug=True,
)
db2 = init_with_config(cfg)


# ── 3. ACID Transactions ────────────────────────────────────────────────────

db2.create("accounts")
db2.write("accounts", "alice", {"balance": 1000})
db2.write("accounts", "bob", {"balance": 500})

tx = db2.begin_transaction()
tx.write("accounts", "alice", {"balance": 900})
tx.write("accounts", "bob", {"balance": 600})

try:
    db2.commit_transaction(tx)
    print("Transaction committed")
except Exception as exc:
    tx.rollback()
    print("Transaction rolled back:", exc)


# ── 4. Fast Lookups with Indexes ────────────────────────────────────────────

db2.create("products")
db2.write("products", "p1", {"name": "Phone", "category": "Electronics"})
db2.write("products", "p2", {"name": "Laptop", "category": "Electronics"})
db2.write("products", "p3", {"name": "Shirt", "category": "Clothing"})

db2.create_index("products", "category")

results = db2.query_by_index("products", "category", "Electronics")
for product in results:
    print("Product:", product)


# ── 5. Batch Operations ─────────────────────────────────────────────────────

db2.create("logs")
batch = {
    f"log{i}": {"timestamp": time.time(), "message": f"Log message {i}"}
    for i in range(1000)
}
db2.write_batch("logs", batch)
print(f"Wrote {len(batch)} log entries in one batch")


# ── 6. Backup & Restore ─────────────────────────────────────────────────────

db2.export(key, "./backup.json")  # plain JSON  (cross-language)
db2.export_encrypted(key, "./backup.enc")  # encrypted binary

db2.import_db(key, "./backup.json")
db2.import_encrypted(key, "./backup.enc")


# ── 7. Monitoring & Metrics ─────────────────────────────────────────────────

metrics = db2.get_metrics()
print("Total Reads:        ", metrics.total_reads)
print("Total Writes:       ", metrics.total_writes)
print("Avg Read Latency:   ", f"{metrics.avg_read_latency * 1000:.3f} ms")
print("Avg Write Latency:  ", f"{metrics.avg_write_latency * 1000:.3f} ms")

status = db2.get_status()
print("Status:", status)


# ── Cleanup ─────────────────────────────────────────────────────────────────
db.close()
db2.close()

for p in ("./data", "./backups", "./backup.json", "./backup.enc"):
    if os.path.exists(p):
        (shutil.rmtree if os.path.isdir(p) else os.unlink)(p)

print("\n✅ All examples completed successfully")
