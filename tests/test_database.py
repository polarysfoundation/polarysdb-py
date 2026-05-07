"""
Tests for PolarysDB Python — mirrors the Go test suite.
"""

import os
import shutil
import tempfile
import threading
import time
import unittest

import polarysdb
from polarysdb import Key, init, init_with_config, Config
from modules.config import get_state_db_path


class TestBasicOperations(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key    = Key("my-secret-encryption-key-32b")
        self.db     = init(self.key, self.tmpdir, debug=False)

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_create_table(self):
        self.db.create("users")
        self.assertTrue(self.db.exist("users"))

    def test_write_and_read(self):
        self.db.create("users")
        self.db.write("users", "alice", {"name": "Alice", "age": 30})
        val, ok = self.db.read("users", "alice")
        self.assertTrue(ok)
        self.assertEqual(val["name"], "Alice")

    def test_read_missing_key(self):
        self.db.create("users")
        val, ok = self.db.read("users", "ghost")
        self.assertFalse(ok)
        self.assertIsNone(val)

    def test_delete(self):
        self.db.create("users")
        self.db.write("users", "bob", {"name": "Bob"})
        self.db.delete("users", "bob")
        _, ok = self.db.read("users", "bob")
        self.assertFalse(ok)

    def test_write_batch(self):
        self.db.create("logs")
        batch = {f"log{i}": {"msg": f"entry {i}"} for i in range(100)}
        self.db.write_batch("logs", batch)
        records = self.db.read_batch("logs")
        self.assertEqual(len(records), 100)

    def test_read_batch(self):
        self.db.create("items")
        self.db.write("items", "a", {"x": 1})
        self.db.write("items", "b", {"x": 2})
        records = self.db.read_batch("items")
        self.assertEqual(len(records), 2)


class TestIndexes(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key    = Key("my-secret-encryption-key-32b")
        self.db     = init(self.key, self.tmpdir)

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_create_and_query_index(self):
        self.db.create("products")
        self.db.write("products", "p1", {"name": "Phone",  "category": "Electronics"})
        self.db.write("products", "p2", {"name": "Laptop", "category": "Electronics"})
        self.db.write("products", "p3", {"name": "Shirt",  "category": "Clothing"})

        self.db.create_index("products", "category")
        results = self.db.query_by_index("products", "category", "Electronics")
        self.assertEqual(len(results), 2)

    def test_query_no_results(self):
        self.db.create("products")
        self.db.create_index("products", "category")
        results = self.db.query_by_index("products", "category", "NonExistent")
        self.assertEqual(results, [])


class TestTransactions(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key    = Key("my-secret-encryption-key-32b")
        self.db     = init(self.key, self.tmpdir)

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_commit(self):
        self.db.create("accounts")
        self.db.write("accounts", "alice", {"balance": 1000})
        self.db.write("accounts", "bob",   {"balance": 500})

        tx = self.db.begin_transaction()
        tx.write("accounts", "alice", {"balance": 900})
        tx.write("accounts", "bob",   {"balance": 600})
        self.db.commit_transaction(tx)

        a, _ = self.db.read("accounts", "alice")
        b, _ = self.db.read("accounts", "bob")
        self.assertEqual(a["balance"], 900)
        self.assertEqual(b["balance"], 600)

    def test_rollback(self):
        self.db.create("accounts")
        self.db.write("accounts", "alice", {"balance": 1000})

        tx = self.db.begin_transaction()
        tx.write("accounts", "alice", {"balance": 0})
        tx.rollback()

        a, _ = self.db.read("accounts", "alice")
        self.assertEqual(a["balance"], 1000)

    def test_tx_delete(self):
        self.db.create("items")
        self.db.write("items", "x", {"val": 1})
        tx = self.db.begin_transaction()
        tx.delete("items", "x")
        self.db.commit_transaction(tx)
        _, ok = self.db.read("items", "x")
        self.assertFalse(ok)


class TestConcurrency(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key    = Key("my-secret-encryption-key-32b")
        self.db     = init(self.key, self.tmpdir)
        self.db.create("concurrent")

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_concurrent_writes(self):
        errors = []

        def worker(start):
            for i in range(start, start + 50):
                try:
                    self.db.write("concurrent", f"key{i}", {"v": i})
                except Exception as exc:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i * 50,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(errors, [], f"Errors during concurrent writes: {errors}")

    def test_concurrent_reads_writes(self):
        self.db.write("concurrent", "shared", {"count": 0})
        stop = threading.Event()
        read_errors = []

        def reader():
            while not stop.is_set():
                try:
                    self.db.read("concurrent", "shared")
                except Exception as exc:
                    read_errors.append(exc)

        def writer():
            for i in range(100):
                self.db.write("concurrent", "shared", {"count": i})

        rt = threading.Thread(target=reader)
        wt = threading.Thread(target=writer)
        rt.start()
        wt.start()
        wt.join(timeout=5)
        stop.set()
        rt.join(timeout=2)
        self.assertEqual(read_errors, [])


class TestExportImport(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key    = Key("my-secret-encryption-key-32b")
        self.db     = init(self.key, self.tmpdir)

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _populate(self):
        self.db.create("data")
        self.db.write("data", "k1", {"hello": "world"})
        self.db.write("data", "k2", {"foo": 42})

    def test_export_import_plain(self):
        self._populate()
        path = os.path.join(self.tmpdir, "export.json")
        self.db.export(self.key, path)
        self.assertTrue(os.path.exists(path))

        tmpdir2 = tempfile.mkdtemp()
        try:
            db2 = init(self.key, tmpdir2)
            db2.import_db(self.key, path)
            v, ok = db2.read("data", "k1")
            self.assertTrue(ok)
            self.assertEqual(v["hello"], "world")
            db2.close()
        finally:
            shutil.rmtree(tmpdir2, ignore_errors=True)

    def test_export_import_encrypted(self):
        self._populate()
        path = os.path.join(self.tmpdir, "export.enc")
        self.db.export_encrypted(self.key, path)

        tmpdir2 = tempfile.mkdtemp()
        try:
            db2 = init(self.key, tmpdir2)
            db2.import_encrypted(self.key, path)
            v, ok = db2.read("data", "k2")
            self.assertTrue(ok)
            self.assertEqual(v["foo"], 42)
            db2.close()
        finally:
            shutil.rmtree(tmpdir2, ignore_errors=True)


class TestPersistence(unittest.TestCase):
    def test_data_survives_restart(self):
        tmpdir = tempfile.mkdtemp()
        key = Key("persistence-test-key-32bytes!!")
        try:
            db = init(key, tmpdir)
            db.create("store")
            db.write("store", "persistent", {"survived": True})
            db.close_with_timeout(5.0)

            db2 = init(key, tmpdir)
            val, ok = db2.read("store", "persistent")
            db2.close()
            self.assertTrue(ok, "record not found after restart")
            self.assertTrue(val["survived"])
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestStatePathLayout(unittest.TestCase):
    def test_state_db_path_absolute_dir(self):
        tmpdir = tempfile.mkdtemp()
        try:
            key = Key("layout-test-key-32bytes-abcdef")
            db = init(key, tmpdir)
            db.create("t")
            db.write("t", "k", {"v": 1})
            db.close_with_timeout(5.0)

            state_path = get_state_db_path(tmpdir)
            self.assertTrue(
                state_path.endswith(os.path.join("state", "state.rdb")),
                f"unexpected state path: {state_path}",
            )
            self.assertTrue(os.path.exists(state_path))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_state_db_path_relative_dir_under_home(self):
        tmp_home = tempfile.mkdtemp()
        old_home = os.environ.get("HOME")
        try:
            os.environ["HOME"] = tmp_home

            rel = "polarysdb_test_rel"
            key = Key("layout-test-key-32bytes-abcdef")
            db = init(key, rel)
            db.create("t")
            db.write("t", "k", {"v": 1})
            db.close_with_timeout(5.0)

            expected = os.path.join(tmp_home, rel, "state", "state.rdb")
            self.assertEqual(get_state_db_path(rel), expected)
            self.assertTrue(os.path.exists(expected))
        finally:
            if old_home is None:
                os.environ.pop("HOME", None)
            else:
                os.environ["HOME"] = old_home
            shutil.rmtree(tmp_home, ignore_errors=True)


class TestMetrics(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key    = Key("my-secret-encryption-key-32b")
        self.db     = init(self.key, self.tmpdir)
        self.db.create("m")

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_metrics_increment(self):
        self.db.write("m", "a", {"x": 1})
        self.db.read("m", "a")
        m = self.db.get_metrics()
        self.assertGreaterEqual(m.total_reads,  1)
        self.assertGreaterEqual(m.total_writes, 1)

    def test_status_keys(self):
        status = self.db.get_status()
        expected = ["uptime_seconds", "closed", "dirty", "total_reads",
                    "total_writes", "total_deletes", "failed_ops",
                    "avg_read_latency", "avg_write_latency", "buffered_ops", "last_save"]
        for k in expected:
            self.assertIn(k, status, f"missing status key: {k}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
