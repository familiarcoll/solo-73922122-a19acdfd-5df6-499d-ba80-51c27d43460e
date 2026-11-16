"""大文件流式压力测试（默认跳过，用 RUN_LARGE=1 开启）。

生成合规但超过容器内存上限的训练集（约 200 MiB > 128 MiB）与保留集
（约 64 MiB，均不触碰 256 MiB 单文件上限），直接驱动 JobManager（绕开
HTTP 客户端对 multipart 正文的整包缓冲），验证：
1. 全流程跑通且净化结果/剔除清单正确；
2. 管道进程峰值 RSS 远小于文件大小（磁盘流式 + SQLite 索引）；
3. 原行字节逐字节保留。

注意：真实 HTTP 上传的内存表现以独立 uvicorn + curl 的实测为准
（201 MiB 文件，进程 VmHWM 约 61 MiB），TestClient 不适合作为内存基准。
"""
from __future__ import annotations

import json
import os
import resource
import time

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_LARGE") != "1", reason="大文件压力测试，需 RUN_LARGE=1 开启"
)


def _line(rid: str, text: str, crlf: bool = False) -> bytes:
    end = b"\r\n" if crlf else b"\n"
    return (json.dumps({"id": rid, "text": text}, ensure_ascii=False)).encode() + end


def test_large_files_stream_with_low_memory(tmp_path):
    from app.manager import DONE, JobManager

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    manager = JobManager(str(data_dir))

    # 保留集：约 64 MiB，每条 30 个词
    ret_path = tmp_path / "retention.jsonl"
    target_ret = 64 * 1024 * 1024
    written, r = 0, 0
    with open(ret_path, "wb") as fh:
        while written < target_ret:
            words = [f"hold{r:07d}_w{k}" for k in range(30)]
            line = _line(f"hold-{r}", " ".join(words))
            fh.write(line)
            written += len(line)
            r += 1
    ret_records = r

    # 训练集：约 200 MiB（超过 128 MiB 容器内存，且在 256 MiB 合规线内），
    # 每 1000 条植入一条命中
    train_path = tmp_path / "train.jsonl"
    target_train = 200 * 1024 * 1024
    written, n, hits = 0, 0, 0
    with open(train_path, "wb") as fh:
        while written < target_train:
            if n % 1000 == 0:
                words = [f"hold{r - 1:07d}_w{k}" for k in range(8)]
                line = _line(f"hit-{n}", "prefix " + " ".join(words) + " suffix")
                hits += 1
            else:
                words = [f"uniq{n:08d}_w{k}" for k in range(24)]
                line = _line(f"tr-{n}", " ".join(words), crlf=(n % 3 == 0))
            fh.write(line)
            written += len(line)
            n += 1

    try:
        t0 = time.time()
        # 直接走与服务完全相同的管理器代码路径
        info = manager.replace_retention(str(ret_path), "retention.jsonl", written)
        assert info["record_count"] == ret_records

        job = manager.create_job(str(train_path), "train.jsonl", written)
        job_id = job["job_id"]

        deadline = time.time() + 1200
        while time.time() < deadline:
            job = manager.get_job(job_id)
            if job["status"] in ("done", "failed", "canceled", "superseded"):
                break
            time.sleep(0.5)
        elapsed = time.time() - t0
        assert job["status"] == DONE, job
        assert job["total_lines"] == n
        assert job["removed_lines"] == hits

        paths = manager.job_paths(job_id)
        # create_job 已把暂存文件移动到任务目录，字节比对基于该上传副本
        upload_copy = paths["upload"]
        # 流式字节级保真：两边都逐行读取，绝不能整文件 .read()（否则会
        # 污染 ru_maxrss 峰值，使内存断言失去意义）。
        removed_ids = {
            json.loads(line)["id"]
            for line in open(paths["report"], encoding="utf-8")
        }
        assert len(removed_ids) == hits
        kept_count = 0
        with open(upload_copy, "rb") as raw_fh, open(
            paths["cleaned"], "rb"
        ) as out_fh:
            for raw_line in raw_fh:
                rid = json.loads(raw_line)["id"]
                if rid in removed_ids:
                    continue
                out_line = out_fh.readline()
                assert out_line == raw_line, "净化输出行与原行字节不一致或乱序"
                kept_count += 1
            assert out_fh.readline() == b"", "净化文件存在多余尾部数据"
        assert kept_count == n - hits

        report_lines = open(paths["report"], encoding="utf-8").read().splitlines()
        assert len(report_lines) == hits
        first = json.loads(report_lines[0])
        assert len(first["evidence_8_words"]) == 8
        assert first["hit_retention_id"] == f"hold-{r - 1}"

        peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        print(
            f"\n大文件测试：训练 {written / 1024 / 1024:.0f} MiB / {n} 行，"
            f"保留 {ret_records} 条，剔除 {hits} 条，耗时 {elapsed:.1f}s，"
            f"管道进程峰值 RSS {peak_mib:.1f} MiB"
        )
        # 直驱管道无 HTTP 缓冲：峰值必须明显小于训练文件，且贴近 128 MiB 约束
        assert peak_mib < 120
    finally:
        manager.shutdown()
