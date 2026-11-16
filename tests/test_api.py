"""API 全链路测试：清洗正确性、取消、保留集更换门禁、重启恢复、大小限制。"""
from __future__ import annotations

import json
import os
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    # 每个测试使用独立数据目录，必须在导入应用模块前设置
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    # 延迟导入，确保环境变量生效
    from app import main as main_mod

    with TestClient(main_mod.app) as c:
        main_mod.manager.reset_for_tests()
        yield c


def _jsonl_bytes(records):
    return b"".join(
        (json.dumps(r, ensure_ascii=False) + "\n").encode() for r in records
    )


def _upload(client, url, records, filename="x.jsonl"):
    return client.post(
        url,
        files={"file": (filename, _jsonl_bytes(records), "application/x-ndjson")},
    )


def _wait_terminal(client, job_id, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = client.get(f"/api/jobs/{job_id}")
        job = resp.json()["job"]
        if job["status"] in ("done", "failed", "canceled", "superseded"):
            return job
        time.sleep(0.05)
    raise AssertionError("任务未在超时内结束")


RETENTION = [
    {"id": "h1", "text": " ".join(f"word{i}" for i in range(12))},
    {"id": "h2", "text": "the quick brown fox jumps over the lazy dog repeat twice"},
]


def _setup_retention(client):
    resp = _upload(client, "/api/retention", RETENTION)
    assert resp.status_code == 200, resp.text
    return resp.json()["retention"]


def test_full_cleaning_pipeline_and_byte_preservation(client):
    _setup_retention(client)
    raw_lines = [
        json.dumps({"id": "keep-1", "text": "totally different content here ok"}, ensure_ascii=False).encode() + b"\n",
        # 与 h1 共享连续 8 词（word2..word9）
        json.dumps({"id": "drop-1", "text": "prefix " + " ".join(f"word{i}" for i in range(2, 10)) + " suffix"}, ensure_ascii=False).encode() + b"\r\n",
        # 大小写不同，不命中
        json.dumps({"id": "keep-2", "text": "THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG REPEAT TWICE"}, ensure_ascii=False).encode() + b"\n",
        # 与 h2 精确命中
        json.dumps({"id": "drop-2", "text": "noise. the quick brown fox jumps over the lazy dog repeat twice end"}, ensure_ascii=False).encode() + b"\n",
        # 不足 8 词，无法构成窗口，保留
        json.dumps({"id": "keep-3", "text": "short text"}, ensure_ascii=False).encode() + b"\n",
    ]
    resp = client.post(
        "/api/jobs",
        files={"file": ("train.jsonl", b"".join(raw_lines), "application/x-ndjson")},
    )
    assert resp.status_code == 200, resp.text
    job_id = resp.json()["job"]["job_id"]
    job = _wait_terminal(client, job_id)
    assert job["status"] == "done"
    assert job["total_lines"] == 5
    assert job["removed_lines"] == 2

    cleaned = client.get(f"/api/jobs/{job_id}/download/cleaned").content
    # 原行序 + 原行字节（含 CRLF 行尾的那一行被剔除，其余逐字节一致）
    assert cleaned == raw_lines[0] + raw_lines[2] + raw_lines[4]

    report = [
        json.loads(line)
        for line in client.get(f"/api/jobs/{job_id}/download/removals").content.decode().splitlines()
    ]
    by_id = {r["id"]: r for r in report}
    assert set(by_id) == {"drop-1", "drop-2"}
    assert by_id["drop-1"]["hit_retention_id"] == "h1"
    assert by_id["drop-2"]["hit_retention_id"] == "h2"
    assert len(by_id["drop-1"]["evidence_8_words"]) == 8
    assert by_id["drop-1"]["evidence_8_words"] == [f"word{i}" for i in range(2, 10)]


def test_duplicate_training_id_fails_and_no_outputs(client):
    _setup_retention(client)
    resp = _upload(
        client,
        "/api/jobs",
        [
            {"id": "x", "text": "a b c d e f g h"},
            {"id": "x", "text": "i j k l m n o p"},
        ],
    )
    job_id = resp.json()["job"]["job_id"]
    job = _wait_terminal(client, job_id)
    assert job["status"] == "failed"
    assert "id 重复" in job["error"]
    # 半成品不得开放
    assert client.get(f"/api/jobs/{job_id}/download/cleaned").status_code == 409


def test_retention_swap_blocks_old_downloads_and_inflight_jobs(client):
    info1 = _setup_retention(client)
    assert info1["version"] == 1

    # 完成一个 v1 任务
    resp = _upload(
        client,
        "/api/jobs",
        [{"id": "t1", "text": " ".join(f"word{i}" for i in range(12))}],
    )
    job1 = _wait_terminal(client, resp.json()["job"]["job_id"])
    assert job1["status"] == "done"
    assert client.get(f"/api/jobs/{job1['job_id']}/download/cleaned").status_code == 200

    # 更换保留集
    resp = _upload(
        client,
        "/api/retention",
        [{"id": "new-h", "text": "brand new retention window eight words exactly now"}],
    )
    assert resp.status_code == 200
    assert resp.json()["retention"]["version"] == 2

    # 旧结果禁止新下载
    blocked = client.get(f"/api/jobs/{job1['job_id']}/download/cleaned")
    assert blocked.status_code == 410
    assert "更换" in blocked.json()["error"]

    # 新任务基于新版本，正常完成且可下载
    resp = _upload(
        client,
        "/api/jobs",
        [{"id": "t2", "text": "brand new retention window eight words exactly now yes"}],
    )
    job2 = _wait_terminal(client, resp.json()["job"]["job_id"])
    assert job2["status"] == "done"
    assert job2["retention_version"] == 2
    assert client.get(f"/api/jobs/{job2['job_id']}/download/cleaned").status_code == 200


def test_failed_retention_swap_keeps_old_set(client):
    _setup_retention(client)
    # 结构非法的保留集：更换必须失败且原集不动
    bad = b'{"id": "h9", "text": "a b c d e f g h"}\nnot-json\n'
    resp = client.post(
        "/api/retention",
        files={"file": ("bad.jsonl", bad, "application/x-ndjson")},
    )
    assert resp.status_code == 400
    info = client.get("/api/retention").json()["retention"]
    assert info["version"] == 1  # 原集保留

    # 重复 id 的保留集同样失败
    resp = _upload(
        client,
        "/api/retention",
        [
            {"id": "dup", "text": "a b c d e f g h"},
            {"id": "dup", "text": "a2 b2 c2 d2 e2 f2 g2 h2"},
        ],
    )
    assert resp.status_code == 400
    assert client.get("/api/retention").json()["retention"]["version"] == 1


def test_cancel_accepts_immediately_and_new_task_starts(client):
    _setup_retention(client)
    # 足够大的输入让任务保持 running
    big = [
        {"id": f"r{i}", "text": " ".join(f"tok{i}_{k}" for k in range(20))}
        for i in range(20000)
    ]
    resp = _upload(client, "/api/jobs", big)
    job_id = resp.json()["job"]["job_id"]

    # 等到 running 后立即取消，接口应快速受理
    deadline = time.time() + 10
    while time.time() < deadline:
        job = client.get(f"/api/jobs/{job_id}").json()["job"]
        if job["status"] == "running":
            break
        time.sleep(0.02)
    t0 = time.time()
    cancel_resp = client.post(f"/api/jobs/{job_id}/cancel")
    assert cancel_resp.status_code == 200
    assert time.time() - t0 < 2.0  # 受理不等待扫描结束
    canceled = _wait_terminal(client, job_id)
    assert canceled["status"] == "canceled"

    # 取消后立即开新任务，且新任务不受旧任务影响
    resp = _upload(
        client,
        "/api/jobs",
        [{"id": "new", "text": "clean independent text without any overlaps today"}],
    )
    assert resp.status_code == 200
    new_job = _wait_terminal(client, resp.json()["job"]["job_id"])
    assert new_job["status"] == "done"
    cleaned = client.get(
        f"/api/jobs/{new_job['job_id']}/download/cleaned"
    ).content
    assert b'"id": "new"' in cleaned


def test_restart_marks_inflight_failed_and_no_half_outputs(client, tmp_path):
    _setup_retention(client)
    resp = _upload(
        client,
        "/api/jobs",
        [{"id": "t", "text": " ".join(f"w{i}" for i in range(12))}],
    )
    job_id = resp.json()["job"]["job_id"]
    _wait_terminal(client, job_id)

    # 人为制造一个“在途”任务：直接改库为 running，并放临时输出
    from app import main as main_mod

    db = main_mod.manager.db_path
    import sqlite3

    conn = sqlite3.connect(db)
    conn.execute("UPDATE jobs SET status='running' WHERE job_id=?", (job_id,))
    conn.commit()
    conn.close()
    paths = main_mod.manager.job_paths(job_id)
    with open(paths["cleaned"] + ".tmp", "wb") as fh:
        fh.write(b"partial\n")

    # 模拟重启：新建 manager 执行恢复
    main_mod.manager.shutdown()
    from app.manager import JobManager

    new_manager = JobManager(os.environ["DATA_DIR"])
    try:
        job = new_manager.get_job(job_id)
        assert job["status"] == "failed"
        assert "重启" in job["error"]
        assert not os.path.exists(paths["cleaned"] + ".tmp")
    finally:
        new_manager.shutdown()


def test_file_size_limit_enforced(client):
    from app.config import MAX_FILE_BYTES

    oversized = b'{"id": "x", "text": "y"}\n' * (MAX_FILE_BYTES // 20 + 10)
    resp = client.post(
        "/api/retention",
        files={"file": ("big.jsonl", oversized, "application/x-ndjson")},
    )
    assert resp.status_code == 400
    assert "256 MiB" in resp.json()["error"]


def test_demo_endpoints(client):
    assert client.post("/api/demo/retention").status_code == 200
    resp = client.post("/api/demo/training")
    assert resp.status_code == 200
    job = _wait_terminal(client, resp.json()["job"]["job_id"])
    assert job["status"] == "done"
    report = client.get(f"/api/jobs/{job['job_id']}/download/removals").content.decode()
    removed = {json.loads(line)["id"] for line in report.splitlines()}
    assert removed == {"train-001", "train-005"}
    cleaned = client.get(f"/api/jobs/{job['job_id']}/download/cleaned").content.decode()
    for kept in ("train-002", "train-003", "train-004"):
        assert kept in cleaned


def test_job_requires_retention(client):
    resp = _upload(client, "/api/jobs", [{"id": "x", "text": "a b c d e f g h"}])
    assert resp.status_code == 409


def test_new_job_allowed_immediately_after_cancel_with_full_slots(client):
    """两个槽位都占满时，取消其中一个后应能立即创建新任务。"""
    _setup_retention(client)
    big = [
        {"id": f"r{i}", "text": " ".join(f"tok{i}_{k}" for k in range(20))}
        for i in range(30000)
    ]
    # 任务 A 运行中
    jid_a = _upload(client, "/api/jobs", big).json()["job"]["job_id"]
    # 任务 B 排队
    jid_b = _upload(
        client, "/api/jobs", [{"id": "b", "text": "a b c d e f g h i"}]
    ).json()["job"]["job_id"]
    # 此时两个槽位已满，再创建应被拒
    resp = _upload(client, "/api/jobs", [{"id": "c", "text": "a b c d e f g h i"}])
    assert resp.status_code == 409

    # 取消 A 后立即创建 C，应被接受（A 视为即将释放）
    client.post(f"/api/jobs/{jid_a}/cancel")
    resp = _upload(client, "/api/jobs", [{"id": "c", "text": "a b c d e f g h i"}])
    assert resp.status_code == 200
    jid_c = resp.json()["job"]["job_id"]

    # A 最终取消，C 与 B 都能正常结束
    for jid in (jid_a, jid_b, jid_c):
        job = _wait_terminal(client, jid)
        assert job["status"] in ("done", "canceled")
    assert client.get(f"/api/jobs/{jid_a}").json()["job"]["status"] == "canceled"
    assert client.get(f"/api/jobs/{jid_c}").json()["job"]["status"] == "done"


def test_queued_job_superseded_and_cleaned_on_swap(client):
    """排队任务在保留集更换时立即作废，且其未使用的任务目录被清理。"""
    _setup_retention(client)
    # 占满 worker 的长跑任务
    big = [
        {"id": f"r{i}", "text": " ".join(f"tok{i}_{k}" for k in range(20))}
        for i in range(40000)
    ]
    _upload(client, "/api/jobs", big)
    # 第二个任务进入排队
    queued = _upload(
        client, "/api/jobs", [{"id": "q", "text": "a b c d e f g h i"}]
    ).json()["job"]["job_id"]
    qjob = client.get(f"/api/jobs/{queued}").json()["job"]
    assert qjob["status"] == "queued"

    from app import main as main_mod

    qdir = main_mod.manager._job_dir(queued)
    assert os.path.isdir(qdir)

    resp = _upload(
        client,
        "/api/retention",
        [{"id": "new-h", "text": "brand new retention window eight words exactly now"}],
    )
    assert resp.status_code == 200
    qjob = client.get(f"/api/jobs/{queued}").json()["job"]
    assert qjob["status"] == "superseded"
    assert not os.path.exists(qdir)  # 排队任务目录已清理
    assert client.get(f"/api/jobs/{queued}/download/cleaned").status_code in (
        409,
        410,
    )


def test_swap_during_running_scan_supersedes_result(client):
    """扫描进行中更换保留集：在途任务不得发布，即使扫描正常跑完。"""
    _setup_retention(client)
    big = [
        {"id": f"r{i}", "text": " ".join(f"tok{i}_{k}" for k in range(20))}
        for i in range(30000)
    ]
    jid = _upload(client, "/api/jobs", big).json()["job"]["job_id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        if client.get(f"/api/jobs/{jid}").json()["job"]["status"] == "running":
            break
        time.sleep(0.02)
    # 运行中更换保留集
    resp = _upload(
        client,
        "/api/retention",
        [{"id": "new-h", "text": "brand new retention window eight words exactly now"}],
    )
    assert resp.status_code == 200
    job = _wait_terminal(client, jid)
    assert job["status"] == "superseded"
    # 非完成态返回 409；即便曾完成也会因版本不匹配返回 410，两者都拒绝下载
    assert (
        client.get(f"/api/jobs/{jid}/download/cleaned").status_code in (409, 410)
    )
