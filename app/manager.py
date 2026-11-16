"""任务与保留集生命周期管理：状态机、后台清洗管道、取消与发布门禁。

并发模型：单进程、单后台 worker 线程、一把可重入锁 + Condition。
同一时刻最多一个任务在扫描、一个任务排队；取消仅置标志位，
扫描循环逐行感知（受理即返回，不等待扫描结束）。每个任务的全部文件
都在独立任务目录内，与其他任务完全隔离，旧任务清理绝不触碰新任务数据。
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable

from .config import (
    DEFAULT_DATA_DIR,
    PROGRESS_FLUSH_LINES,
    PROGRESS_FLUSH_SECONDS,
    SQLITE_CACHE_KIB,
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

        self._lock = threading.RLock()
        # 保留集更换串行化锁：耗时的索引构建在 self._lock 之外进行，
        # 避免构建期间阻塞扫描线程与进度查询
        self._retention_lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        # 内存态：job_id -> 任务信息；重启后由控制库回填
        self._jobs: dict[str, dict] = {}
        self._stop = False
        self._worker: threading.Thread | None = None

        self._init_db()
        self._recover()
        self._worker = threading.Thread(
            target=self._worker_loop, name="clean-worker", daemon=True
        )
        self._worker.start()

    # ---- 控制库 -------------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA cache_size={SQLITE_CACHE_KIB}")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
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

    # ---- 路径工具 -----------------------------------------------------------
    def _job_dir(self, job_id: str) -> str:
        return os.path.join(self.jobs_dir, job_id)

    def job_paths(self, job_id: str) -> dict[str, str]:
        d = self._job_dir(job_id)
        return {
            "upload": os.path.join(d, "upload.jsonl"),
            "cleaned": os.path.join(d, _CLEANED_NAME),
            "report": os.path.join(d, _REPORT_NAME),
            "state_db": os.path.join(d, "scan.db"),
        }

    def _ret_paths(self, version: int) -> dict[str, str]:
        return {
            "file": os.path.join(self.ret_dir, f"retention-v{version}.jsonl"),
            "index": os.path.join(self.ret_dir, f"index-v{version}.sqlite"),
        }

    # ---- 重启恢复：半成品一律不开放 ----------------------------------------
    def _recover(self) -> None:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM jobs").fetchall()
            for row in rows:
                self._jobs[row["job_id"]] = dict(row)
            now = time.time()
            for row in rows:
                if row["status"] not in ACTIVE_STATES:
                    continue
                job_id = row["job_id"]
                # 在途任务重启后标记失败，绝不发布半成品
                conn.execute(
                    "UPDATE jobs SET status=?, error=?, progress=1.0, "
                    "finished_at=? WHERE job_id=?",
                    (FAILED, "服务重启，任务未完成；请重新发起清洗", now, job_id),
                )
                self._jobs[job_id].update(
                    status=FAILED,
                    error="服务重启，任务未完成；请重新发起清洗",
                    progress=1.0,
                    finished_at=now,
                )
                paths = self.job_paths(job_id)
                for tmp in (paths["cleaned"] + ".tmp", paths["report"] + ".tmp"):
                    self._silent_remove(tmp)
            self._sweep_orphans_locked(conn)

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

    def _sweep_orphans_locked(self, conn: sqlite3.Connection) -> None:
        """删除没有任何任务引用、且不是当前版本的保留集文件。"""
        current = self._current_retention_version_locked(conn)
        referenced = {
            r["retention_version"]
            for r in conn.execute(
                "SELECT DISTINCT retention_version FROM jobs"
            ).fetchall()
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
    def _current_retention_version_locked(
        self, conn: sqlite3.Connection
    ) -> int | None:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='current_retention_version'"
        ).fetchone()
        return int(row["value"]) if row else None

    def get_retention(self) -> dict | None:
        with self._lock, self._connect() as conn:
            version = self._current_retention_version_locked(conn)
            if version is None:
                return None
            row = conn.execute(
                "SELECT * FROM retention_versions WHERE version=?", (version,)
            ).fetchone()
            return dict(row) if row else None

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
        失败：原保留集与已有结果完全不动，异常向上抛由路由转 400。

        耗时的索引构建在主锁之外进行（仅用更换串行锁），构建期间扫描线程
        与进度查询不受阻塞；只有最后的版本指针切换短暂持有主锁。
        """
        with self._retention_lock:
            # 短暂持锁确定下一个版本号
            with self._lock, self._connect() as conn:
                current = self._current_retention_version_locked(conn)
                next_version = (current or 0) + 1
                paths = self._ret_paths(next_version)
            if os.path.exists(paths["index"]) or os.path.exists(paths["file"]):
                raise RuntimeError("目标保留集文件已存在，拒绝覆盖")
            try:
                shutil.move(staged_path, paths["file"])
                # 主锁之外构建：扫描线程与网页查询可继续运行
                stats = build_index(
                    paths["index"], paths["file"], progress=progress_cb
                )
            except Exception:
                # 回滚暂存与半成品，原集不受影响
                self._silent_remove(paths["file"])
                self._silent_remove(paths["index"])
                raise

            # 索引已就绪，短暂持锁做原子切换
            with self._lock, self._connect() as conn:
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
                    "INSERT INTO meta(key, value) VALUES('current_retention_version', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(next_version),),
                )
                # 在途旧任务：排队的立即作废，运行中的扫描结束后拒绝发布
                rows = conn.execute(
                    "SELECT job_id FROM jobs WHERE status IN (?, ?)",
                    (QUEUED, RUNNING),
                ).fetchall()
                for row in rows:
                    job = self._jobs.get(row["job_id"])
                    if job and job["status"] == QUEUED:
                        conn.execute(
                            "UPDATE jobs SET status=?, finished_at=? WHERE job_id=?",
                            (SUPERSEDED, now, row["job_id"]),
                        )
                        job.update(status=SUPERSEDED, finished_at=now)
                        # 排队任务从未被 worker 处理，安全删除其任务目录
                        self._remove_job_dir(row["job_id"])
                    elif job:
                        job["superseded"] = True
                self._cond.notify_all()
                self._sweep_orphans_locked(conn)
                return dict(
                    conn.execute(
                        "SELECT * FROM retention_versions WHERE version=?",
                        (next_version,),
                    ).fetchone()
                )

    # ---- 任务创建 / 查询 / 取消 --------------------------------------------
    def create_job(
        self, staged_path: str, original_name: str, byte_size: int
    ) -> dict:
        with self._lock:
            with self._connect() as conn:
                version = self._current_retention_version_locked(conn)
                if version is None:
                    raise ValidationError("尚未上传保留题集，无法发起清洗", 409)
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
                self._jobs[job_id] = info
                self._cond.notify_all()
                return dict(info)

    def get_job(self, job_id: str) -> dict | None:
        with self._lock:
            info = self._jobs.get(job_id)
            return dict(info) if info else None

    def list_jobs(self) -> list[dict]:
        with self._lock:
            return [dict(j) for j in self._jobs.values()]

    def cancel_job(self, job_id: str) -> dict:
        """受理取消：仅置标志位，不等待扫描结束；接口立即返回。"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise ValidationError("任务不存在", 404)
            if job["status"] in TERMINAL_STATES:
                return dict(job)
            job["cancel_requested"] = True
            self._cond.notify_all()
            return dict(job)

    # ---- 下载门禁 -----------------------------------------------------------
    def _published_paths(self, job_id: str) -> tuple[dict, dict]:
        """仅当任务已完成且保留集未被更换时，返回可下载路径。"""
        with self._lock, self._connect() as conn:
            current = self._current_retention_version_locked(conn)
            row = conn.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if row is None:
                raise ValidationError("任务不存在", 404)
            job = dict(row)
            if job["status"] != DONE:
                raise ValidationError(
                    f"任务当前状态为 {job['status']}，没有可下载的结果", 409
                )
            if job["retention_version"] != current:
                raise ValidationError(
                    "保留题集已更换，旧任务结果按策略禁止下载", 410
                )
            paths = self.job_paths(job_id)
            if not os.path.exists(paths["cleaned"]) or not os.path.exists(
                paths["report"]
            ):
                raise ValidationError("结果文件缺失", 410)
            return job, paths

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

    def _worker_loop(self) -> None:
        while True:
            with self._cond:
                if self._stop:
                    return
                job = next(
                    (
                        j
                        for j in self._jobs.values()
                        if j["status"] == QUEUED and not j.get("superseded")
                    ),
                    None,
                )
                if job is None:
                    self._cond.wait(timeout=1.0)
                    continue
                job_id = job["job_id"]
                job["status"] = RUNNING
                job["started_at"] = time.time()
                with self._connect() as conn:
                    conn.execute(
                        "UPDATE jobs SET status=?, started_at=? WHERE job_id=?",
                        (RUNNING, job["started_at"], job_id),
                    )
            try:
                self._run_pipeline(job_id)
            except Exception as exc:  # 任何异常都收敛为失败，不留下半成品
                self._fail_job(job_id, str(exc))

    def _run_pipeline(self, job_id: str) -> None:
        with self._lock:
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

        try:
            with IndexReader(index_path) as reader, open(
                tmp_cleaned, "wb"
            ) as out_clean, open(tmp_report, "wb") as out_report:
                for rec, read_bytes, total_bytes in iter_jsonl_with_position(
                    upload
                ):
                    with self._lock:
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
                        self._write_progress(
                            job_id,
                            min(0.999, progress),
                            total_lines,
                            removed_lines,
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
            with self._lock:
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

        # 扫描完成：发布前再次确认保留集版本未被更换
        with self._lock, self._connect() as conn:
            current = self._current_retention_version_locked(conn)
            job = self._jobs[job_id]
            if version != current or job.get("superseded"):
                self._finish_unsuccessful(
                    job_id,
                    SUPERSEDED,
                    "保留题集已更换，任务结果不予发布",
                    tmp_cleaned,
                    tmp_report,
                    state_db,
                )
                return
            os.replace(tmp_cleaned, paths["cleaned"])
            os.replace(tmp_report, paths["report"])
            self._silent_remove(state_db)
            now = time.time()
            conn.execute(
                "UPDATE jobs SET status=?, progress=1.0, total_lines=?, "
                "removed_lines=?, finished_at=? WHERE job_id=?",
                (DONE, total_lines, removed_lines, now, job_id),
            )
            job.update(
                status=DONE,
                progress=1.0,
                total_lines=total_lines,
                removed_lines=removed_lines,
                finished_at=now,
            )

    def _write_progress(
        self,
        job_id: str,
        progress: float,
        total_lines: int,
        removed_lines: int,
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET progress=?, total_lines=?, removed_lines=? "
                "WHERE job_id=?",
                (progress, total_lines, removed_lines, job_id),
            )
            job = self._jobs[job_id]
            job.update(
                progress=progress,
                total_lines=total_lines,
                removed_lines=removed_lines,
            )

    def _finish_unsuccessful(
        self,
        job_id: str,
        status: str,
        error: str | None,
        *tmp_paths: str,
    ) -> None:
        for path in tmp_paths:
            self._silent_remove(path)
        with self._lock, self._connect() as conn:
            now = time.time()
            conn.execute(
                "UPDATE jobs SET status=?, error=?, finished_at=? WHERE job_id=?",
                (status, error, now, job_id),
            )
            self._jobs[job_id].update(
                status=status, error=error, finished_at=now
            )

    def _fail_job(self, job_id: str, error: str) -> None:
        with self._lock:
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
        with self._lock:
            self._jobs.clear()
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
