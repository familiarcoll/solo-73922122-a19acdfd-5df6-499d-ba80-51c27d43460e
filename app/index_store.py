"""保留题集的连续 8 词索引：构建与只读查询，全部落在 SQLite。

内存策略：
- 索引数据库只做磁盘存储，page_cache 限制为 2 MiB；
- 构建期批量插入，内存中最多保留一个批次；
- 查询走 (hash, key) 主键，进程内不缓存整张索引，
  因而可处理远大于 128 MiB 内存的保留集。

碰撞策略：主键同时存 128 位哈希与无歧义精确窗口键，
查询必须哈希与精确键同时相等，哈希碰撞不会产生误剔除。
"""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable

from .config import SQLITE_CACHE_KIB
from .jsonl_utils import iter_jsonl
from .tokenizer import iter_ngrams

BATCH_ROWS = 2000


def _connect(path: str, *, readonly: bool) -> sqlite3.Connection:
    if readonly:
        uri = f"file:{path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    else:
        conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute(f"PRAGMA cache_size={SQLITE_CACHE_KIB}")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def build_index(
    index_path: str,
    retention_path: str,
    progress: Callable[[int], None] | None = None,
) -> dict[str, int]:
    """扫描保留集 JSONL，构建 n-gram 索引数据库。

    调用前 index_path 不得存在（保证失败不会污染旧库）。
    返回 {"records": 保留记录数, "windows": 去重后窗口数}。
    """
    conn = _connect(index_path, readonly=False)
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.executescript(
            """
            CREATE TABLE retention_ids (
                id TEXT PRIMARY KEY
            ) WITHOUT ROWID;
            CREATE TABLE ngrams (
                hash BLOB NOT NULL,
                key  BLOB NOT NULL,
                retention_id TEXT NOT NULL,
                PRIMARY KEY (hash, key, retention_id)
            ) WITHOUT ROWID;
            """
        )
        records = 0
        windows = 0
        batch: list[tuple[bytes, bytes, str]] = []

        def flush() -> None:
            nonlocal windows
            before = conn.total_changes
            conn.executemany(
                "INSERT OR IGNORE INTO ngrams(hash, key, retention_id) VALUES (?, ?, ?)",
                batch,
            )
            windows += conn.total_changes - before
            batch.clear()

        conn.execute("BEGIN")
        for rec in iter_jsonl(retention_path):
            try:
                conn.execute("INSERT INTO retention_ids(id) VALUES (?)", (rec.id,))
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"保留集 id 重复：{rec.id}") from exc
            for h, key, _words in iter_ngrams(rec.text.split()):
                batch.append((h, key, rec.id))
                if len(batch) >= BATCH_ROWS:
                    flush()
            records += 1
            if progress is not None:
                progress(records)
        if batch:
            flush()
        conn.commit()
    except Exception:
        conn.close()
        # 失败彻底删除半成品索引，避免任何场景读到不完整数据
        try:
            os.remove(index_path)
        except FileNotFoundError:
            pass
        raise
    else:
        conn.close()

    # 重新以只读参数打开做一次完整性检查
    check = _connect(index_path, readonly=True)
    try:
        check.execute("PRAGMA integrity_check").fetchone()
    finally:
        check.close()
    return {"records": records, "windows": windows}


class IndexReader:
    """只读索引句柄，线程内使用；每个清洗任务自建连接。"""

    # 单条 SQL 绑定变量数控制在各 SQLite 版本普遍支持的 999 以内
    _HASH_CHUNK = 900

    def __init__(self, index_path: str):
        self.conn = _connect(index_path, readonly=True)

    def lookup(self, window_hash: bytes, exact_key: bytes) -> str | None:
        """返回命中的任一保留 id（按 id 排序取第一个，保证确定性）。"""
        row = self.conn.execute(
            "SELECT retention_id FROM ngrams WHERE hash = ? AND key = ? "
            "ORDER BY retention_id LIMIT 1",
            (window_hash, exact_key),
        ).fetchone()
        return row[0] if row else None

    def lookup_first(
        self, windows: list[tuple[bytes, bytes]]
    ) -> tuple[int, str] | None:
        """按顺序找第一个命中的窗口，一次记录最多发起 ceil(n/900) 次查询。

        windows: 有序的 (128 位哈希, 精确键) 列表。
        返回 (命中窗口下标, 保留 id)；
        同一窗口命中多个保留 id 时取字典序最小者，保证结果确定。
        """
        key_to_rid: dict[bytes, str] = {}
        # 先收集所有可能命中的行，再按窗口顺序裁决首个命中
        seen_hashes: set[bytes] = set()
        for start in range(0, len(windows), self._HASH_CHUNK):
            chunk = windows[start : start + self._HASH_CHUNK]
            hashes: list[bytes] = []
            for h, _key in chunk:
                if h not in seen_hashes:
                    seen_hashes.add(h)
                    hashes.append(h)
            placeholders = ",".join("?" * len(hashes))
            rows = self.conn.execute(
                f"SELECT hash, key, retention_id FROM ngrams "
                f"WHERE hash IN ({placeholders})",
                hashes,
            ).fetchall()
            for _h, key, rid in rows:
                old = key_to_rid.get(key)
                if old is None or rid < old:
                    key_to_rid[key] = rid
        for i, (_h, key) in enumerate(windows):
            rid = key_to_rid.get(key)
            if rid is not None:
                return i, rid
        return None

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "IndexReader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
