"""任务与保留集生命周期管理：状态机、后台清洗管道、取消与发布门禁。

并发模型（修复版）：
- 单进程、单后台 worker 线程；
- ``_state_lock``（Condition）只保护内存态，临界区内绝不做任何 SQLite
  阻塞 I/O（仅有与控制库写锁无关的极短本地目录/改名操作），因此外部连接
  占用控制库写锁期间，进度查询、取消受理、已有结果下载与保留集信息查询
  都能立即响应；
- 所有控制库写入都在内存锁之外经由短连接 ``_db_write`` 完成，按
  ``SQLITE_BUSY_TIMEOUT`` 等待写锁（SQLite/WAL 自身串行化写者，不再叠加
  Python 写锁，避免 API 写者被 worker 的长等待队头阻塞）：API 路径超时即
  返回“库被占用、稍后重试”（503），worker 路径退避后持续重试，锁等待超时
  不会终止线程，释锁后队列自动恢复，无需重启；
- 发布采用两阶段裁决：先在 ``_state_lock`` 内对“取消 / 被更换 / 完成”做
  确定性裁决并翻转内存态，再在锁外做文件 ``os.replace`` 与控制库持久化，
  因而“取消先受理则绝不发布（即使扫描已跑完或随后才拿到写锁）；完成先
  提交则取消返回 409”。

每个任务的全部文件都在独立任务目录内，与其他任务完全隔离，旧任务清理
绝不触碰新任务数据。
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from contextlib import closing

from .config import (
    DEFAULT_DATA_DIR,
    PROGRESS_DB_BUSY_TIMEOUT,
    PROGRESS_FLUSH_LINES,
    PROGRESS_FLUSH_SECONDS,
    SQLITE_BUSY_TIMEOUT,
    SQLITE_CACHE_KIB,
    WORKER_BACKOFF_MAX,
)
from .errors import ValidationError
from .index_store import IndexReader, build_index
from .jsonl_utils import iter_jsonl_with_position
from .tokenizer import iter_ngrams

# ---- 任务状态 ---------------------------------------------------------------
QUEUED = "queued"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
CANCELED = "canceled"
SUPERSEDED = "superseded"
ACTIVE_STATES = (QUEUED, RUNNING)
TERMINAL_STATES = (DONE, FAILED, CANCELED, SUPERSEDED)

_REPORT_NAME = "removals.jsonl"
_CLEANED_NAME = "cleaned.jsonl"

# 控制库被外部写锁占用、等待超时后向 API 调用方返回的提示
_DB_BUSY_MESSAGE = "控制库正被其他连接占用写锁，请稍后重试"


class ControlDbBusy(ValidationError):
    """控制库写锁等待超时：API 层转 503，worker 层改为退避重试。"""

    def __init__(self) -> None:
        super().__init__(_DB_BUSY_MESSAGE, 503)


class JobManager:
    def __init__(self, data_dir: str | os.PathLike[str] | None = None):
        if data_dir is None:
            data_dir = os.environ.get("DATA_DIR", DEFAULT_DATA_DIR)
        self.data_dir = os.path.abspath(os.fspath(data_dir))
        self.jobs_dir = os.path.join(self.data_dir, "jobs")
        self.ret_dir = os.path.join(self.data_dir, "retention")
        os.makedirs(self.jobs_dir, exist_ok=True)
        os.makedirs(self.ret_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "control.db")

        # 内存态锁：临界区内禁止任何 SQLite 阻塞 I/O（本地极短文件操作除外）
        self._state_lock = threading.RLock()
        self._cond = threading.Condition(self._state_lock)
        # 保留集更换串行锁：耗时的索引构建在所有业务锁之外进行
        self._retention_lock = threading.Lock()

        # 内存态：job_id -> 任务信息；重启后由控制库回填。
        # 热路径（查询/取消/下载门禁）只读这里，不触碰 SQLite。
        self._jobs: dict[str, dict] = {}
        # 当前保留集版本信息（启动回填，更换成功后更新）
        self._retention_info: dict | None = None
        # 已在内存裁决、但控制库尚未持久化的终态写入（FIFO）。
        # API 线程只入队并唤醒 worker，由 worker 在锁外带退避重试落库，
        # 因而取消受理永远不会被写锁等待堵住，也不会与持锁方互锁。
        self._pending_persist: deque[Callable[[sqlite3.Connection], None]] = deque()
        self._stop = False
        self._worker: threading.Thread | None = None

        # 测试钩子：worker 在扫描循环每条记录处理前回调（默认空操作）
        self._scan_hook: Callable[[str], None] | None = None
        # 测试钩子：worker 拿到下一个排队任务后回调
        self._dispatch_hook: Callable[[str], None] | None = None
        # 测试钩子：worker 完成扫描、进入发布裁决前回调
        self._finalize_hook: Callable[[str], None] | None = None
        # 测试钩子：worker 每次尝试把任务置 running（写控制库）前回调
        self._mark_running_hook: Callable[[str], None] | None = None

        self._init_db()
        self._recover()
        self._worker = threading.Thread(
            target=self._worker_loop, name="clean-worker", daemon=True
        )
        self._worker.start()

    # ---- 控制库连接与写入 ---------------------------------------------------
    def _connect(
        self, *, busy_timeout: float | None = None
    ) -> sqlite3.Connection:
        """新建短连接：调用方必须用 closing()/with closing 关闭，避免句柄泄漏。"""
        if busy_timeout is None:
            # 运行时读取，便于测试调小锁等待
            busy_timeout = SQLITE_BUSY_TIMEOUT
        conn = sqlite3.connect(
            self.db_path,
            timeout=busy_timeout,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA cache_size={SQLITE_CACHE_KIB}")
        return conn

    def _read_connect(self) -> sqlite3.Connection:
        """只读连接：WAL 模式下读不阻塞写、写不阻塞读。"""
        uri = f"file:{self.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=SQLITE_BUSY_TIMEOUT)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA cache_size={SQLITE_CACHE_KIB}")
        return conn

    def _db_write(
        self,
        fn: Callable[[sqlite3.Connection], object],
        *,
        retry: bool,
        busy_timeout: float | None = None,
    ) -> object:
        """在内存锁之外执行一次控制库写事务。

        不额外加 Python 写锁：SQLite 在 WAL 下本就串行化所有写事务，
        再加一层锁只会让 API 写者排在 worker 的长等待之后（队头阻塞）。
        各连接按自己的 busy timeout 独立等待：

        retry=True（worker 路径）：锁等待超时抛回调用方，由 worker 退避
            后整体重试，worker 线程永不因写锁超时而退出；
        retry=False（API 路径）：超时统一转 :class:`ControlDbBusy`（503），
            调用按自身 timeout 快速返回，不影响查询/取消/下载。
        """
        if busy_timeout is None:
            busy_timeout = SQLITE_BUSY_TIMEOUT
        try:
            with closing(self._connect(busy_timeout=busy_timeout)) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                result = fn(conn)
                conn.commit()
                return result
        except sqlite3.OperationalError as exc:
            if self._is_busy(exc):
                if retry:
                    raise
                raise ControlDbBusy() from exc
            raise

    @staticmethod
    def _is_busy(exc: sqlite3.OperationalError) -> bool:
        msg = str(exc).lower()
        return "locked" in msg or "busy" in msg

    def _interruptible_sleep(self, seconds: float) -> None:
        """可被 shutdown 中断的退避等待；等待期间不持有任何业务锁。"""
        end = time.time() + seconds
        with self._cond:
            while not self._stop and time.time() < end:
                self._cond.wait(timeout=min(0.5, max(0.0, end - time.time())))

    def _init_db(self) -> None:
        # 启动期同样可能遇到外部写锁：有限重试，避免初始化直接崩溃
        deadline = time.time() + SQLITE_BUSY_TIMEOUT
        while True:
            try:
                with closing(self._connect()) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("PRAGMA synchronous=NORMAL")
                    conn.executescript(
                        """
                        CREATE TABLE IF NOT EXISTS retention_versions (
                            version      INTEGER PRIMARY KEY,
                            file_name    TEXT NOT NULL,
                            index_name   TEXT NOT NULL,
                            byte_size    INTEGER NOT NULL,
                            record_count INTEGER NOT NULL,
                            window_count INTEGER NOT NULL,
                            created_at   REAL NOT NULL
                        );
                        CREATE TABLE IF NOT EXISTS jobs (
                            job_id            TEXT PRIMARY KEY,
                            status            TEXT NOT NULL,
                            file_name         TEXT NOT NULL,
                            byte_size         INTEGER NOT NULL,
                            retention_version INTEGER NOT NULL,
                            progress          REAL NOT NULL DEFAULT 0,
                            total_lines       INTEGER,
                            removed_lines     INTEGER,
                            error             TEXT,
                            created_at        REAL NOT NULL,
                            started_at        REAL,
                            finished_at       REAL
                        );
                        CREATE TABLE IF NOT EXISTS meta (
                            key TEXT PRIMARY KEY,
                            value TEXT NOT NULL
                        );
                        """
                    )
                return
            except sqlite3.OperationalError as exc:
                if not self._is_busy(exc) or time.time() >= deadline:
                    raise
                time.sleep(0.2)

    # ---- 路径工具 -----------------------------------------------------------
    def _job_dir(self, job_id: str) -> str:
        return os.path.join(self.jobs_dir, job_id)

    def job_paths(self, job_id: str) -> dict[str, str]:
        d = self._job_dir(job_id)
        return {
            "upload": os.path.join(d, "upload.jsonl"),
            "cleaned": os.path.join(d, "cleaned"),
            "report": os.path.join(d, "report"),
            "state_db": os.path.join(d, "scan.db"),
        }

    def _ret_paths(self, version: int) -> dict[str, str]:
        return {
            "file": os.path.join(self.ret_dir, f"retention-v{version}.jsonl"),
            "index": os.path.join(self.ret_dir, f"index-v{version}.sqlite"),
        }

    # ---- 重启恢复：半成品一律不开放 ----------------------------------------
    def _recover(self) -> None:
        with closing(self._read_connect()) as conn:
            rows = conn.execute("SELECT * FROM jobs").fetchall()
            for row in rows:
                self._jobs[row["job_id"]] = dict(row)
            current = self._current_retention_version_locked(conn)
            if current is not None:
                rrow = conn.execute(
                    "SELECT * FROM retention_versions WHERE version=?", (current,)
                ).fetchone()
                if rrow is not None:
                    self._retention_info = dict(rrow)

            inflight = [row for row in rows if row["status"] in ACTIVE_STATES]

        now = time.time()
        if not inflight:
            return

        # 在途任务重启后标记失败，绝不发布半成品；持久化失败则退避重试
        def mark_failed(conn: sqlite3.Connection) -> None:
            for row in inflight:
                conn.execute(
                    "UPDATE jobs SET status=?, error=?, progress=1.0, "
                    "finished_at=? WHERE job_id=?",
                    (
                        FAILED,
                        "服务重启，任务未完成；请重新发起清洗",
                        now,
                        row["job_id"],
                    ),
                )

        self._persist_retrying(mark_failed)
        with self._state_lock:
            for row in inflight:
                job_id = row["job_id"]
                self._jobs[job_id].update(
                    status=FAILED,
                    error="服务重启，任务未完成；请重新发起清洗",
                    progress=1.0,
                    finished_at=now,
                )
                paths = self.job_paths(job_id)
                for tmp in (paths["cleaned"] + ".tmp", paths["report"] + ".tmp"):
                    self._silent_remove(tmp)
        self._sweep_orphan_retention_files()

    def _persist_retrying(
        self,
        fn: Callable[[sqlite3.Connection], None],
        *,
        busy_timeout: float | None = None,
    ) -> None:
        """worker 专用：控制库写入失败（含写锁超时）则退避后无限重试。

        等待期间不持有内存锁，查询/取消/下载始终可响应；shutdown 可中断。
        """
        if busy_timeout is None:
            busy_timeout = SQLITE_BUSY_TIMEOUT
        backoff = 0.2
        while True:
            try:
                self._db_write(fn, retry=True, busy_timeout=busy_timeout)
                return
            except sqlite3.OperationalError as exc:
                if not self._is_busy(exc):
                    raise
                if self._stop:
                    return
                self._interruptible_sleep(backoff)
                backoff = min(backoff * 2, WORKER_BACKOFF_MAX)

    def _enqueue_persist(
        self, fn: Callable[[sqlite3.Connection], None]
    ) -> None:
        """把一次终态持久化放入队列，由 worker 锁外带退避重试落库。

        供 API 线程（如排队任务取消受理）使用：入队即返回，绝不阻塞在
        写锁上，从根本上避免“请求线程等锁、持锁方等请求返回”的互锁。
        """
        with self._cond:
            self._pending_persist.append(fn)
            self._cond.notify_all()

    def _drain_pending_persist(self) -> None:
        """worker 排空待持久化终态；每条独立重试，顺序与受理顺序一致。"""
        while True:
            with self._cond:
                if not self._pending_persist:
                    return
                fn = self._pending_persist.popleft()
            self._persist_retrying(fn)
            if self._stop:
                return

    @staticmethod
    def _silent_remove(path: str) -> None:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def _remove_job_dir(self, job_id: str) -> None:
        """删除任务私有目录；仅用于从未产出结果的排队任务，含产物则保留。"""
        paths = self.job_paths(job_id)
        if os.path.exists(paths["cleaned"]) or os.path.exists(paths["report"]):
            return
        shutil.rmtree(self._job_dir(job_id), ignore_errors=True)

    def _sweep_orphan_retention_files(self) -> None:
        """删除没有任何任务引用、且不是当前版本的保留集文件。"""
        with self._state_lock:
            current = (
                self._retention_info["version"] if self._retention_info else None
            )
            referenced = {
                j["retention_version"] for j in self._jobs.values()
            }
        for name in os.listdir(self.ret_dir):
            for prefix in ("retention-v", "index-v"):
                if not (
                    name.startswith(prefix)
                    and (name.endswith(".jsonl") or name.endswith(".sqlite"))
                ):
                    continue
                middle = name[len(prefix) :].split(".", 1)[0]
                try:
                    ver = int(middle)
                except ValueError:
                    continue
                if ver != current and ver not in referenced:
                    self._silent_remove(os.path.join(self.ret_dir, name))

    # ---- 保留集 -------------------------------------------------------------
    @staticmethod
    def _current_retention_version_locked(
        conn: sqlite3.Connection,
    ) -> int | None:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='current_retention_version'"
        ).fetchone()
        return int(row["value"]) if row else None

    def get_retention(self) -> dict | None:
        # 纯内存读取：控制库被占用期间也立即响应
        with self._state_lock:
            return dict(self._retention_info) if self._retention_info else None

    def replace_retention(
        self,
        staged_path: str,
        original_name: str,
        byte_size: int,
        progress_cb: Callable[[int], None] | None = None,
    ) -> dict:
        """原子更换保留集。

        成功：新版本生效；排队中的旧任务立即作废，运行中的旧任务扫描结束后
              拒绝发布，旧结果的新下载一律拒绝；旧版本若无任务引用立即清理。
        失败：原保留集与已有结果完全不动，异常向上抛由路由转 400/503。

        耗时的索引构建在所有业务锁之外进行，构建期间扫描线程、网页查询、
        取消与下载均不受阻塞；只有最后的版本指针切换需要一次控制库写事务。
        """
        with self._retention_lock:
            with self._state_lock:
                current = (
                    self._retention_info["version"]
                    if self._retention_info
                    else None
                )
                next_version = (current or 0) + 1
            paths = self._ret_paths(next_version)
            if os.path.exists(paths["index"]) or os.path.exists(paths["file"]):
                raise RuntimeError("目标保留集文件已存在，拒绝覆盖")
            try:
                shutil.move(staged_path, paths["file"])
                # 所有业务锁之外构建：扫描线程与网页查询可继续运行
                stats = build_index(
                    paths["index"], paths["file"], progress=progress_cb
                )
            except Exception:
                # 回滚暂存与半成品，原集不受影响
                self._silent_remove(paths["file"])
                self._silent_remove(paths["index"])
                raise

            # 收集需要作废的排队任务（内存锁内只做内存裁决，不做 I/O）
            with self._state_lock:
                queued_victims = [
                    jid
                    for jid, job in self._jobs.items()
                    if job["status"] == QUEUED
                ]
                running_jobs = [
                    job
                    for job in self._jobs.values()
                    if job["status"] == RUNNING
                ]

            def switch(conn: sqlite3.Connection) -> dict:
                # 单个事务内完成版本切换 + 在途任务作废，保证原子性
                now = time.time()
                conn.execute(
                    "INSERT INTO retention_versions"
                    "(version, file_name, index_name, byte_size, record_count, "
                    "window_count, created_at) VALUES (?,?,?,?,?,?,?)",
                    (
                        next_version,
                        original_name,
                        os.path.basename(paths["index"]),
                        byte_size,
                        stats["records"],
                        stats["windows"],
                        now,
                    ),
                )
                conn.execute(
                    "INSERT INTO meta(key, value) "
                    "VALUES('current_retention_version', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(next_version),),
                )
                for job_id in queued_victims:
                    conn.execute(
                        "UPDATE jobs SET status=?, finished_at=? WHERE job_id=?",
                        (SUPERSEDED, now, job_id),
                    )
                info = dict(
                    conn.execute(
                        "SELECT * FROM retention_versions WHERE version=?",
                        (next_version,),
                    ).fetchone()
                )
                return info

            # API 路径：写锁超时直接转 503，半成品索引由下方回滚清理
            try:
                info = self._db_write(switch, retry=False)
            except BaseException:
                self._silent_remove(paths["file"])
                self._silent_remove(paths["index"])
                raise

            # 控制库已提交，再翻转内存态并通知 worker；锁内仍无阻塞 I/O
            with self._state_lock:
                self._retention_info = info
                now = info["created_at"]
                for job_id in queued_victims:
                    job = self._jobs.get(job_id)
                    if job is not None and job["status"] == QUEUED:
                        job.update(status=SUPERSEDED, finished_at=now)
                for job in running_jobs:
                    job["superseded"] = True
                self._cond.notify_all()

            # 排队任务从未被 worker 处理，锁外安全删除其任务目录
            for job_id in queued_victims:
                self._remove_job_dir(job_id)
            self._sweep_orphan_retention_files()
            return dict(info)

    # ---- 任务创建 / 查询 / 取消 --------------------------------------------
    def create_job(
        self, staged_path: str,
        original_name: str,
        byte_size: int,
    ) -> dict:
        # 槽位裁决只依赖内存态
        with self._state_lock:
            if self._retention_info is None:
                raise ValidationError("尚未上传保留题集，无法发起清洗", 409)
            version = self._retention_info["version"]
            # 已受理取消或已被保留集更换作废的任务视为即将释放，
            # 保证“取消后立即开新任务”不会被槽位限制挡住
            active = [
                j
                for j in self._jobs.values()
                if j["status"] in ACTIVE_STATES
                and not j.get("cancel_requested")
                and not j.get("superseded")
            ]
            if len(active) >= 2:
                raise ValidationError(
                    "已有任务在扫描或排队，请先取消当前任务再发起新任务", 409
                )
            job_id = uuid.uuid4().hex
            job_dir = self._job_dir(job_id)
            os.makedirs(job_dir, exist_ok=True)
            upload_path = self.job_paths(job_id)["upload"]
            shutil.move(staged_path, upload_path)
            now = time.time()
            info = {
                "job_id": job_id,
                "status": QUEUED,
                "file_name": original_name,
                "byte_size": byte_size,
                "retention_version": version,
                "progress": 0.0,
                "total_lines": None,
                "removed_lines": None,
                "error": None,
                "created_at": now,
                "started_at": None,
                "finished_at": None,
            }
            self._jobs[job_id] = info

        def insert(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT INTO jobs(job_id, status, file_name, byte_size, "
                "retention_version, progress, created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    job_id,
                    QUEUED,
                    original_name,
                    byte_size,
                    version,
                    0.0,
                    now,
                ),
            )

        try:
            # 锁等待发生在内存锁之外：其他 API 与 worker 均不受阻塞；
            # 超时返回 503，调用方可稍后重试，任务并未入队
            self._db_write(insert, retry=False)
        except BaseException:
            with self._state_lock:
                self._jobs.pop(job_id, None)
            shutil.rmtree(job_dir, ignore_errors=True)
            raise

        with self._cond:
            self._cond.notify_all()
        return dict(info)

    def get_job(self, job_id: str) -> dict | None:
        # 纯内存读取，控制库被占用期间立即响应
        with self._state_lock:
            info = self._jobs.get(job_id)
            return dict(info) if info else None

    def list_jobs(self) -> list[dict]:
        # 纯内存读取，控制库被占用期间立即响应
        with self._state_lock:
            return [dict(j) for j in self._jobs.values()]

    def cancel_job(self, job_id: str) -> dict:
        """受理取消：仅置内存标志位，无任何 SQLite I/O，接口立即返回。

        - 排队任务：直接在内存态裁决为 canceled，并由后台异步持久化；
        - 运行任务：置标志位，扫描循环逐行感知、发布门禁再次确认；
        - 已结束任务：不可取消，返回 409（完成先提交则取消失败）。
        """
        with self._state_lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise ValidationError("任务不存在", 404)
            if job["status"] in TERMINAL_STATES:
                raise ValidationError(
                    f"任务已处于终态 {job['status']}，无法取消", 409
                )
            job["cancel_requested"] = True
            if job["status"] == QUEUED:
                # 排队任务可立即裁决：worker 永远不会再拾取它
                job["status"] = CANCELED
                job["error"] = "任务已取消"
                job["finished_at"] = time.time()
                snapshot = dict(job)
            else:
                snapshot = dict(job)
            self._cond.notify_all()

        if snapshot["status"] == CANCELED:
            # 排队任务从未被 worker 处理，可立即清理其目录
            self._remove_job_dir(job_id)

            def persist(conn: sqlite3.Connection) -> None:
                conn.execute(
                    "UPDATE jobs SET status=?, error=?, finished_at=? "
                    "WHERE job_id=?",
                    (
                        CANCELED,
                        snapshot["error"],
                        snapshot["finished_at"],
                        job_id,
                    ),
                )

            # 异步持久化：请求线程绝不阻塞在写锁上，避免与持锁方互锁；
            # 内存态已裁决为 canceled，查询/下载门禁立即按终态生效
            self._enqueue_persist(persist)
        return snapshot

    # ---- 下载门禁（纯内存 + 文件存在性，不触碰控制库） ---------------------
    def _published_paths(self, job_id: str) -> tuple[dict, dict]:
        """仅当任务已完成且保留集未被更换时，返回可下载路径。"""
        with self._state_lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise ValidationError("任务不存在", 404)
            current_version = (
                self._retention_info["version"] if self._retention_info else None
            )
            snapshot = dict(job)
        if snapshot["status"] != DONE:
            raise ValidationError(
                f"任务当前状态为 {snapshot['status']}，没有可下载的结果", 409
            )
        if snapshot["retention_version"] != current_version:
            raise ValidationError(
                "保留题集已更换，旧任务结果按策略禁止下载", 410
            )
        paths = self.job_paths(job_id)
        if not os.path.exists(paths["cleaned"]) or not os.path.exists(
            paths["report"]
        ):
            raise ValidationError("结果文件缺失", 410)
        return snapshot, paths

    def download_cleaned(self, job_id: str) -> tuple[str, str]:
        _job, paths = self._published_paths(job_id)
        return paths["cleaned"], _CLEANED_NAME

    def download_report(self, job_id: str) -> tuple[str, str]:
        _job, paths = self._published_paths(job_id)
        return paths["report"], _REPORT_NAME

    # ---- 后台 worker --------------------------------------------------------
    def shutdown(self) -> None:
        with self._cond:
            self._stop = True
            self._cond.notify_all()
        if self._worker:
            self._worker.join(timeout=5)
        # 尽力把已受理但未落库的终态写一次（有界，失败则留给重启恢复，
        # 恢复会把所有在途任务判为 failed，同样不会开放半成品）
        self._flush_pending_best_effort()

    def _flush_pending_best_effort(self) -> None:
        deadline = time.time() + 2.0
        while time.time() < deadline:
            with self._cond:
                if not self._pending_persist:
                    return
                fn = self._pending_persist.popleft()
            try:
                self._db_write(fn, retry=False, busy_timeout=1.0)
            except Exception:
                # 落不下去就放弃：重启恢复保证不发布未完成产物
                return

    def _worker_loop(self) -> None:
        """单 worker 主循环：任何异常都不允许终止线程，队列永不停摆。"""
        backoff = 0.2
        while not self._stop:
            try:
                # 先排空 API 线程异步入队的终态持久化（如排队任务取消），
                # 避免它们在忙等写锁期间长期滞留控制库未提交
                self._drain_pending_persist()
                job_id = self._pick_next_job()
                if job_id is None:
                    backoff = 0.2
                    with self._cond:
                        if self._pending_persist:
                            continue  # 有新的待持久化项，立即处理
                        self._cond.wait(timeout=1.0)
                    continue

                # 置 running 的控制库写入可能等写锁：等待期间不持内存锁，
                # 进度查询/取消/下载照常响应；超时只退避重试，线程不退出
                if not self._mark_running_with_retry(job_id):
                    continue  # shutdown 或任务已被取消/更换：重新挑下一个

                if self._dispatch_hook is not None:
                    self._dispatch_hook(job_id)

                try:
                    self._run_pipeline(job_id)
                except Exception as exc:  # 任何异常都收敛为失败，不留下半成品
                    self._fail_job(job_id, str(exc))
                backoff = 0.2
            except Exception:  # 兜底：worker 线程永不因异常退出
                if self._stop:
                    return
                # 非锁类异常：短暂休眠继续，避免空转刷日志
                self._interruptible_sleep(1.0)

    def _pick_next_job(self) -> str | None:
        with self._state_lock:
            job = next(
                (
                    j
                    for j in self._jobs.values()
                    if j["status"] == QUEUED and not j.get("superseded")
                ),
                None,
            )
            if job is None:
                return None
            return job["job_id"]

    def _mark_running_with_retry(self, job_id: str) -> bool:
        """把任务翻转为 running；写库被占时退避重试。

        返回 False 表示任务已无需执行（被取消/更换或服务关闭）。
        """
        backoff = 0.2
        while not self._stop:
            # 每次尝试前重新确认内存态：取消/更换可能发生在等锁期间
            with self._state_lock:
                job = self._jobs.get(job_id)
                if job is None or job["status"] != QUEUED:
                    return False
                if job.get("cancel_requested") or job.get("superseded"):
                    return False
                started_at = time.time()

            def mark(conn: sqlite3.Connection) -> None:
                # 带状态条件更新：若等锁期间该任务已在控制库落为终态
                # （如排队取消的异步持久化），本次翻转不得覆盖终态。
                cur = conn.execute(
                    "UPDATE jobs SET status=?, started_at=? "
                    "WHERE job_id=? AND status=?",
                    (RUNNING, started_at, job_id, QUEUED),
                )
                if cur.rowcount == 0:
                    raise sqlite3.OperationalError(
                        "job no longer queued; skip mark-running"
                    )

            if self._mark_running_hook is not None:
                self._mark_running_hook(job_id)
            try:
                self._db_write(mark, retry=True)
            except sqlite3.OperationalError as exc:
                if "no longer queued" in str(exc):
                    # 控制库中已是终态（如排队取消的异步落库），无需再执行
                    return False
                if not self._is_busy(exc):
                    raise
                self._interruptible_sleep(backoff)
                backoff = min(backoff * 2, WORKER_BACKOFF_MAX)
                continue

            with self._state_lock:
                job = self._jobs.get(job_id)
                if job is None:
                    return False
                # 等锁期间被取消/更换：内存态已是终态，绝不能回滚成排队态；
                # 由对应的终态流程（取消/作废）接管，本任务不再执行
                if job.get("cancel_requested") or job.get("superseded"):
                    return False
                job["status"] = RUNNING
                job["started_at"] = started_at
            return True
        return False

    def _run_pipeline(self, job_id: str) -> None:
        with self._state_lock:
            job = self._jobs[job_id]
            paths = self.job_paths(job_id)
            upload = paths["upload"]
            version = job["retention_version"]
            index_path = self._ret_paths(version)["index"]
            state_db = paths["state_db"]
            tmp_cleaned = paths["cleaned"] + ".tmp"
            tmp_report = paths["report"] + ".tmp"

        # 任务私有 id 去重库（磁盘，避免大文件 id 集合占用内存）
        self._silent_remove(state_db)
        state = sqlite3.connect(state_db, check_same_thread=False)
        state.execute(f"PRAGMA cache_size={SQLITE_CACHE_KIB}")
        state.execute("PRAGMA journal_mode=OFF")
        state.execute("PRAGMA synchronous=OFF")
        state.execute("CREATE TABLE seen_ids(id TEXT PRIMARY KEY) WITHOUT ROWID")

        total_lines = 0
        removed_lines = 0
        last_flush = time.time()
        stop = False
        error: str | None = None
        # 控制库被占期间进度落库静默降级为仅内存，不再反复等锁拖慢扫描
        progress_db_degraded = False

        try:
            with IndexReader(index_path) as reader, open(
                tmp_cleaned, "wb"
            ) as out_clean, open(tmp_report, "wb") as out_report:
                for rec, read_bytes, total_bytes in iter_jsonl_with_position(
                    upload
                ):
                    if self._scan_hook is not None:
                        self._scan_hook(job_id)
                    with self._state_lock:
                        job = self._jobs[job_id]
                        if job.get("cancel_requested") or job.get("superseded"):
                            stop = True
                            break

                    total_lines += 1
                    try:
                        state.execute(
                            "INSERT INTO seen_ids(id) VALUES (?)", (rec.id,)
                        )
                    except sqlite3.IntegrityError:
                        error = f"训练集 id 重复：{rec.id}"
                        break

                    hit_id: str | None = None
                    evidence: list[str] | None = None
                    # text.split 与 tokenizer 规则一致：Unicode 空白、区分大小写
                    grams = list(iter_ngrams(rec.text.split()))
                    if grams:
                        # 每记录一次（必要时分块）批量查询，逐窗口精确键裁决
                        found = reader.lookup_first([(h, key) for h, key, _ in grams])
                        if found is not None:
                            index, rid = found
                            hit_id = rid
                            evidence = grams[index][2]

                    if hit_id is None:
                        # 原始字节直写，严格保持原行序与原行字节
                        out_clean.write(rec.raw)
                    else:
                        removed_lines += 1
                        out_report.write(
                            (
                                json.dumps(
                                    {
                                        "id": rec.id,
                                        "hit_retention_id": hit_id,
                                        "evidence_8_words": evidence,
                                    },
                                    ensure_ascii=False,
                                )
                                + "\n"
                            ).encode("utf-8")
                        )

                    now = time.time()
                    if (
                        total_lines % PROGRESS_FLUSH_LINES == 0
                        or now - last_flush >= PROGRESS_FLUSH_SECONDS
                    ):
                        progress = (
                            1.0 if total_bytes == 0 else read_bytes / total_bytes
                        )
                        progress_db_degraded = self._write_progress(
                            job_id,
                            min(0.999, progress),
                            total_lines,
                            removed_lines,
                            db_degraded=progress_db_degraded,
                        )
                        last_flush = now
        finally:
            state.close()

        if error is not None:
            self._fail_job(job_id, error)
            self._silent_remove(tmp_cleaned)
            self._silent_remove(tmp_report)
            self._silent_remove(state_db)
            return

        if stop:
            with self._state_lock:
                superseded = bool(self._jobs[job_id].get("superseded"))
            self._finish_unsuccessful(
                job_id,
                SUPERSEDED if superseded else CANCELED,
                "保留题集已更换，任务结果不予发布"
                if superseded
                else "任务已取消",
                tmp_cleaned,
                tmp_report,
                state_db,
            )
            return

        # 扫描完成：两阶段发布门禁
        if self._finalize_hook is not None:
            self._finalize_hook(job_id)
        self._finalize_completed(
            job_id,
            version,
            total_lines,
            removed_lines,
            tmp_cleaned,
            tmp_report,
            state_db,
            paths,
        )

    def _finalize_completed(
        self,
        job_id: str,
        version: int,
        total_lines: int,
        removed_lines: int,
        tmp_cleaned: str,
        tmp_report: str,
        state_db: str,
        paths: dict[str, str],
    ) -> None:
        """阶段一：内存锁内做确定性裁决（取消/更换优先于完成）。"""
        with self._state_lock:
            job = self._jobs[job_id]
            current_version = (
                self._retention_info["version"] if self._retention_info else None
            )
            if job.get("cancel_requested"):
                decision = CANCELED
                message = "任务已取消"
            elif job.get("superseded") or version != current_version:
                decision = SUPERSEDED
                message = "保留题集已更换，任务结果不予发布"
            else:
                decision = DONE
                message = None
                # 内存态先翻转：此后取消请求必然拿到 409（完成先提交）
                now = time.time()
                job.update(
                    status=DONE,
                    progress=1.0,
                    total_lines=total_lines,
                    removed_lines=removed_lines,
                    finished_at=now,
                )
            snapshot = dict(job)

        # 阶段二：锁外做文件发布与控制库持久化；写锁再久也不影响 API 响应
        if decision != DONE:
            self._finish_unsuccessful(
                job_id,
                decision,
                message,
                tmp_cleaned,
                tmp_report,
                state_db,
            )
            return

        # 取消先受理时不可能走到这里：DONE 裁决已在锁内确定，
        # os.replace 之后的持久化重试期间下载门禁按内存态放行，
        # 而重启恢复只认控制库中的终态 + 实际文件，半成品不会开放。
        os.replace(tmp_cleaned, paths["cleaned"])
        os.replace(tmp_report, paths["report"])
        self._silent_remove(state_db)

        def persist_done(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE jobs SET status=?, progress=1.0, total_lines=?, "
                "removed_lines=?, finished_at=? WHERE job_id=?",
                (
                    DONE,
                    total_lines,
                    removed_lines,
                    snapshot["finished_at"],
                    job_id,
                ),
            )

        self._persist_retrying(persist_done)

    def _write_progress(
        self,
        job_id: str,
        progress: float,
        total_lines: int,
        removed_lines: int,
        *,
        db_degraded: bool,
    ) -> bool:
        """进度先写内存（API 立即可见），再最佳努力落控制库。

        返回新的降级标志：一旦控制库写锁超时，本任务后续进度只维护内存态，
        避免扫描主循环反复阻塞；终态持久化仍会无限重试，不丢最终结果。
        """
        with self._state_lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job.update(
                    progress=progress,
                    total_lines=total_lines,
                    removed_lines=removed_lines,
                )
        if db_degraded:
            return True

        def update(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE jobs SET progress=?, total_lines=?, removed_lines=? "
                "WHERE job_id=?",
                (progress, total_lines, removed_lines, job_id),
            )

        try:
            self._db_write(
                update,
                retry=False,
                busy_timeout=PROGRESS_DB_BUSY_TIMEOUT,
            )
        except ControlDbBusy:
            return True
        return False

    def _finish_unsuccessful(
        self,
        job_id: str,
        status: str,
        error: str | None,
        *tmp_paths: str,
    ) -> None:
        """非成功结束（取消/被更换/失败）：先删临时产物，再翻转内存与控制库。"""
        for path in tmp_paths:
            self._silent_remove(path)
        now = time.time()
        with self._state_lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job.update(status=status, error=error, finished_at=now)
                snapshot = dict(job)
            else:
                return

        def persist(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE jobs SET status=?, error=?, finished_at=? WHERE job_id=?",
                (status, error, snapshot["finished_at"], job_id),
            )

        self._persist_retrying(persist)

    def _fail_job(self, job_id: str, error: str) -> None:
        with self._state_lock:
            paths = self.job_paths(job_id)
        self._finish_unsuccessful(
            job_id,
            FAILED,
            error,
            paths["cleaned"] + ".tmp",
            paths["report"] + ".tmp",
            paths["state_db"],
        )

    # ---- 测试与演示辅助 -----------------------------------------------------
    def reset_for_tests(self) -> None:
        """仅测试用：清空全部任务与保留集。"""
        with self._state_lock:
            self._jobs.clear()
            self._retention_info = None
        if os.path.isdir(self.jobs_dir):
            shutil.rmtree(self.jobs_dir)
        if os.path.isdir(self.ret_dir):
            shutil.rmtree(self.ret_dir)
        os.makedirs(self.jobs_dir, exist_ok=True)
        os.makedirs(self.ret_dir, exist_ok=True)
        self._silent_remove(self.db_path)
        for suffix in ("-wal", "-shm"):
            self._silent_remove(self.db_path + suffix)
        self._init_db()
