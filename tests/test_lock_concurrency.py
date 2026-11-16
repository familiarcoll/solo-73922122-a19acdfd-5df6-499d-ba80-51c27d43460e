"""写锁占用下的调度、取消、发布协作测试。

覆盖题目关键行为：
1. 外部 SQLite 连接持有控制库写锁期间，进度查询、已有结果下载、取消受理
   都必须在释锁前立即响应（不被 worker 的锁等待堵住）；
2. worker 等写锁超时只能退避重试，线程不死；释锁后已受理任务与新任务
   自动继续处理，无需重启；
3. 取消在发布前受理则最终取消且绝不发布，即使扫描已经跑完；
4. 完成先提交后，取消返回 409；
5. 控制库被占期间创建任务返回明确的“稍后重试”，释锁后可正常创建。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

import app.manager as mmod


@contextmanager
def short_busy_timeout(seconds: float = 0.5):
    """把 API/worker 的 SQLite 锁等待缩短，避免测试真实等待 30s。

    生产代码在调用时读取模块常量，因此直接 patch 常量即可生效。
    """
    import app.config as cfg

    orig_cfg = cfg.SQLITE_BUSY_TIMEOUT
    orig_mmod = mmod.SQLITE_BUSY_TIMEOUT
    cfg.SQLITE_BUSY_TIMEOUT = seconds
    mmod.SQLITE_BUSY_TIMEOUT = seconds
    try:
        yield
    finally:
        cfg.SQLITE_BUSY_TIMEOUT = orig_cfg
        mmod.SQLITE_BUSY_TIMEOUT = orig_mmod


@pytest.fixture()
def client(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    from app import main as main_mod

    with TestClient(main_mod.app) as c:
        main_mod.manager.reset_for_tests()
        yield c


@contextmanager
def external_write_lock(db_path: str):
    """模拟“另一个 SQLite 连接持有控制库写锁”。

    BEGIN IMMEDIATE 立即获取 RESERVED→EXCLUSIVE 写锁，事务提交前其他连接
    的写入会在 busy timeout 后报 database is locked；WAL 下读不受影响。
    """
    holder = sqlite3.connect(db_path, timeout=0.1)
    try:
        holder.execute("BEGIN IMMEDIATE")
        yield holder
    finally:
        holder.execute("COMMIT")
        holder.close()


def _jsonl_bytes(records):
    return b"".join(
        (json.dumps(r, ensure_ascii=False) + "\n").encode() for r in records
    )


def _upload(client, url, records, filename="x.jsonl"):
    return client.post(
        url,
        files={"file": (filename, _jsonl_bytes(records), "application/x-ndjson")},
    )


def _wait_status(client, job_id, statuses, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get(f"/api/jobs/{job_id}").json()["job"]
        if job["status"] in statuses:
            return job
        time.sleep(0.03)
    raise AssertionError(f"任务未在超时内进入 {statuses}，当前 {job['status']}")


def _wait_running(client, job_id, timeout=10):
    return _wait_status(client, job_id, {"running", "done", "failed"}, timeout)


RETENTION = [
    {"id": "h1", "text": " ".join(f"word{i}" for i in range(12))},
    {"id": "h2", "text": "the quick brown fox jumps over the lazy dog repeat twice"},
]


def _setup_retention(client):
    resp = _upload(client, "/api/retention", RETENTION)
    assert resp.status_code == 200, resp.text
    return resp.json()["retention"]


def _big_training(n=20000):
    return [
        {"id": f"r{i}", "text": " ".join(f"tok{i}_{k}" for k in range(20))}
        for i in range(n)
    ]


# ---- 1. 写锁占用期间热路径立即响应 -----------------------------------------
def test_apis_respond_while_external_lock_held(client):
    from app import main as main_mod

    _setup_retention(client)
    # 先完成一个任务，保证“已有结果下载”可验证
    resp = _upload(
        client, "/api/jobs",
        [{"id": "done-1", "text": "totally unrelated clean content here today"}],
    )
    done_job = _wait_status(client, resp.json()["job"]["job_id"], {"done"})
    done_id = done_job["job_id"]

    # 再起一个长跑任务
    resp = _upload(client, "/api/jobs", _big_training())
    job_id = resp.json()["job"]["job_id"]
    _wait_running(client, job_id)

    db_path = main_mod.manager.db_path
    with external_write_lock(db_path):
        # 释锁前：进度查询必须立即响应（纯内存，不碰控制库）
        t0 = time.time()
        r = client.get(f"/api/jobs/{job_id}")
        assert r.status_code == 200
        assert time.time() - t0 < 2.0
        assert r.json()["job"]["status"] == "running"

        t0 = time.time()
        r = client.get("/api/jobs")
        assert r.status_code == 200 and time.time() - t0 < 2.0

        # 已有结果下载不受写锁影响
        t0 = time.time()
        r = client.get(f"/api/jobs/{done_id}/download/cleaned")
        assert r.status_code == 200 and time.time() - t0 < 2.0
        r = client.get(f"/api/jobs/{done_id}/download/removals")
        assert r.status_code == 200

        # 保留集信息查询同样立即响应
        t0 = time.time()
        r = client.get("/api/retention")
        assert r.status_code == 200 and time.time() - t0 < 2.0

        # 取消必须立即受理，不等写锁、不等扫描结束
        t0 = time.time()
        r = client.post(f"/api/jobs/{job_id}/cancel")
        assert r.status_code == 200 and time.time() - t0 < 2.0
        assert r.json()["cancel_requested"] is True

        # 持锁期间 worker 逐行感知后，内存态应在释锁前翻为 canceled
        cancel_deadline = time.time() + 10
        while time.time() < cancel_deadline:
            job = client.get(f"/api/jobs/{job_id}").json()["job"]
            if job["status"] == "canceled":
                break
            time.sleep(0.05)
        assert job["status"] == "canceled", job

    # 释锁后：无需重启，队列可继续处理新任务
    resp = _upload(
        client, "/api/jobs",
        [{"id": "after-lock", "text": "another clean independent sentence today ok"}],
    )
    assert resp.status_code == 200
    new_job = _wait_status(client, resp.json()["job"]["job_id"], {"done"})
    assert new_job["status"] == "done"
    assert (
        client.get(
            f"/api/jobs/{new_job['job_id']}/download/cleaned"
        ).status_code
        == 200
    )
    # 被取消的任务最终保持 canceled，且没有产物可下载
    final = client.get(f"/api/jobs/{job_id}").json()["job"]
    assert final["status"] == "canceled"
    assert (
        client.get(f"/api/jobs/{job_id}/download/cleaned").status_code == 409
    )


# ---- 2. 锁超时不杀死 worker，释锁后排队任务自动继续 ------------------------
def test_worker_survives_lock_timeout_and_queue_resumes(client):
    from app import main as main_mod

    _setup_retention(client)
    # 先让一个任务入队，然后在它被拾取前占住写锁
    resp = _upload(client, "/api/jobs", _big_training(15000))
    job_id = resp.json()["job"]["job_id"]
    _wait_running(client, job_id)

    db_path = main_mod.manager.db_path
    # 持锁超过一次 busy timeout（PROGRESS_DB_BUSY_TIMEOUT=2s），
    # 期间 worker 的进度/状态写入会反复超时；线程必须存活
    with external_write_lock(db_path):
        time.sleep(4.0)
        # 查询始终可用，任务仍是 running（线程没崩、任务没丢）
        job = client.get(f"/api/jobs/{job_id}").json()["job"]
        assert job["status"] == "running"

    # 释锁后任务正常跑到完成，无需重启
    done = _wait_status(client, job_id, {"done", "failed", "canceled"}, timeout=30)
    assert done["status"] == "done", done

    # 后续新任务也能被同一 worker 正常处理（证明线程仍在循环）
    resp = _upload(
        client, "/api/jobs",
        [{"id": "q2", "text": "fully independent text without any overlap today"}],
    )
    j2 = _wait_status(client, resp.json()["job"]["job_id"], {"done"})
    assert j2["status"] == "done"


# ---- 3. 控制库被占期间建任务返回 503，释锁后恢复 ---------------------------
def test_create_job_returns_503_while_locked_then_recovers(client):
    from app import main as main_mod

    _setup_retention(client)
    db_path = main_mod.manager.db_path
    with short_busy_timeout(0.5):
        with external_write_lock(db_path):
            resp = _upload(
                client, "/api/jobs",
                [{"id": "blocked", "text": "a b c d e f g h i j"}],
            )
            assert resp.status_code == 503
            assert "稍后重试" in resp.json()["error"]
            # 被拒任务没有入队、没有残留目录
            assert client.get("/api/jobs").json()["jobs"] == []
            data_dir = main_mod.manager.data_dir
            assert os.listdir(os.path.join(data_dir, "jobs")) == []

    # 释锁后创建立即恢复
    resp = _upload(
        client, "/api/jobs",
        [{"id": "ok", "text": "a b c d e f g h i j"}],
    )
    assert resp.status_code == 200
    job = _wait_status(client, resp.json()["job"]["job_id"], {"done"})
    assert job["status"] == "done"


# ---- 4. 扫描跑完后、发布前受理取消 => 绝不发布 -----------------------------
def test_cancel_after_scan_finished_but_before_publish_wins(client):
    from app import main as main_mod

    _setup_retention(client)
    resp = _upload(
        client, "/api/jobs",
        [{"id": "t", "text": " ".join(f"w{i}" for i in range(12))}],
    )
    job_id = resp.json()["job"]["job_id"]

    release = threading.Event()
    fired = threading.Event()

    # worker 扫描结束、进入发布裁决的瞬间发起取消
    def finalize_hook(jid):
        if jid != job_id or fired.is_set():
            return
        fired.set()
        r = client.post(f"/api/jobs/{jid}/cancel")
        assert r.status_code == 200
        release.set()

    main_mod.manager._finalize_hook = finalize_hook
    try:
        job = _wait_status(
            client, job_id, {"canceled", "done", "failed", "superseded"}, timeout=15
        )
    finally:
        main_mod.manager._finalize_hook = None
    assert fired.is_set()
    release.wait(2.0)
    assert job["status"] == "canceled", job

    # 取消先受理：任何产物都不得发布
    paths = main_mod.manager.job_paths(job_id)
    assert not os.path.exists(paths["cleaned"])
    assert not os.path.exists(paths["report"])
    assert (
        client.get(f"/api/jobs/{job_id}/download/cleaned").status_code == 409
    )


# ---- 5. 完成先提交 => 取消返回 409，结果可下载 -----------------------------
def test_cancel_after_completion_is_rejected_409(client):
    _setup_retention(client)
    resp = _upload(
        client, "/api/jobs",
        [{"id": "t", "text": "clean independent content without overlaps today"}],
    )
    job_id = resp.json()["job"]["job_id"]
    done = _wait_status(client, job_id, {"done"})
    assert done["status"] == "done"

    r = client.post(f"/api/jobs/{job_id}/cancel")
    assert r.status_code == 409
    assert "无法取消" in r.json()["error"]
    # 结果仍可正常下载
    assert (
        client.get(f"/api/jobs/{job_id}/download/cleaned").status_code == 200
    )


# ---- 6. 排队任务在写锁期间也能立即取消，释锁后不被执行 ---------------------
def test_queued_job_cancel_during_lock_then_never_runs(client):
    from app import main as main_mod

    _setup_retention(client)
    # 长跑任务占住 worker
    resp_a = _upload(client, "/api/jobs", _big_training(30000))
    jid_a = resp_a.json()["job"]["job_id"]
    _wait_running(client, jid_a)
    # 第二个任务排队
    resp_b = _upload(
        client, "/api/jobs",
        [{"id": "b", "text": "a b c d e f g h i j"}],
    )
    jid_b = resp_b.json()["job"]["job_id"]
    assert client.get(f"/api/jobs/{jid_b}").json()["job"]["status"] == "queued"

    db_path = main_mod.manager.db_path
    with short_busy_timeout(0.5):
        with external_write_lock(db_path):
            # 排队任务的取消是纯内存裁决，必须立即生效
            t0 = time.time()
            r = client.post(f"/api/jobs/{jid_b}/cancel")
            assert r.status_code == 200 and time.time() - t0 < 2.0
            job = client.get(f"/api/jobs/{jid_b}").json()["job"]
            assert job["status"] == "canceled"

    # 释锁后 B 不会被翻出来执行；A 正常完成
    client.post(f"/api/jobs/{jid_a}/cancel")
    _wait_status(client, jid_a, {"canceled", "done"})
    time.sleep(0.3)
    assert client.get(f"/api/jobs/{jid_b}").json()["job"]["status"] == "canceled"


# ---- 7. 等锁期间被取消：running 翻转重试识别标志位，不发布 ------------------
def test_cancel_while_worker_waiting_for_write_lock(client):
    from app import main as main_mod

    _setup_retention(client)
    # 长跑任务占住唯一 worker，第二个任务保持排队
    resp_a = _upload(client, "/api/jobs", _big_training(40000))
    jid_a = resp_a.json()["job"]["job_id"]
    _wait_running(client, jid_a)

    resp_b = _upload(
        client, "/api/jobs",
        [{"id": "b", "text": " ".join(f"w{i}" for i in range(12))}],
    )
    jid_b = resp_b.json()["job"]["job_id"]
    assert client.get(f"/api/jobs/{jid_b}").json()["job"]["status"] == "queued"

    client.post(f"/api/jobs/{jid_a}/cancel")
    _wait_status(client, jid_a, {"canceled", "done"})

    db_path = main_mod.manager.db_path
    manager = main_mod.manager
    hook_fired = threading.Event()
    state = {"holder": None}

    def mark_running_hook(jid):
        # 仅对 B 生效：非阻塞抢占写锁，让 worker 随后的 mark-running 等待；
        # 若本次没抢到（worker 自身连接竞争），放行由下一次重试再占。
        if jid != jid_b or state["holder"] is not None:
            return
        try:
            holder = sqlite3.connect(
                db_path, timeout=0, check_same_thread=False
            )
            holder.execute("BEGIN IMMEDIATE")
            state["holder"] = holder
            hook_fired.set()
        except sqlite3.OperationalError:
            try:
                holder.close()
            except Exception:  # noqa: BLE001
                pass

    manager._mark_running_hook = mark_running_hook
    try:
        # worker 拾取 B，并在钩子内被外部写锁挡住、进入退避重试
        assert hook_fired.wait(10.0)
        assert state["holder"] is not None, state.get("error")

        # 此时 worker 正在等写锁；取消必须立即受理且不被锁等待阻塞
        t0 = time.time()
        r = client.post(f"/api/jobs/{jid_b}/cancel")
        assert r.status_code == 200 and time.time() - t0 < 2.0

        # 释放写锁：worker 重试时发现取消标志/终态，必须停止，绝不发布
        state["holder"].execute("COMMIT")
        state["holder"].close()

        job = _wait_status(
            client, jid_b, {"canceled", "done", "failed"}, timeout=15
        )
    finally:
        manager._mark_running_hook = None
    assert job["status"] == "canceled", job
    paths = manager.job_paths(jid_b)
    assert not os.path.exists(paths["cleaned"])
    assert not os.path.exists(paths["report"])
