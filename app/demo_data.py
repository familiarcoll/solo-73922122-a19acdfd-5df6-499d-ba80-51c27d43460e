"""演示用样例数据生成，便于网页一键体验。"""
from __future__ import annotations

import json
import os
import tempfile

# 8 个以上词的保留段落，训练集中混入共享连续 8 词的记录
RETENTION_RECORDS = [
    {
        "id": "holdout-001",
        "text": (
            "the quick brown fox jumps over the lazy dog near the quiet river "
            "bend at dawn while mist covers the meadow and birds begin their calls"
        ),
    },
    {
        "id": "holdout-002",
        "text": (
            "模型 训练 前 必须 剔除 混入 保留 题集 的 文本 以 保证 评测 结果 真实 可信"
        ),
    },
]

TRAINING_RECORDS = [
    # 与 holdout-001 共享连续 8 词，应被剔除
    {
        "id": "train-001",
        "text": (
            "random unrelated sentence here. the quick brown fox jumps over the lazy dog "
            "near the quiet river bend, copied almost verbatim from evaluation material."
        ),
    },
    # 大小写不同：按区分大小写规则不构成命中，应保留
    {
        "id": "train-002",
        "text": (
            "THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG NEAR THE QUIET RIVER BEND"
        ),
    },
    # 干净记录，应保留
    {
        "id": "train-003",
        "text": "completely independent training text about weather patterns and ocean currents.",
    },
    # 干净中文记录（词序列与保留集不连续重合），应保留
    {
        "id": "train-004",
        "text": "这是 一条 干净 的 训练 句子 ， 没有 任何 重叠 内容 。",
    },
    # 与 holdout-002 共享连续 8 词，应被剔除
    {
        "id": "train-005",
        "text": (
            "数据工程 实践 中 模型 训练 前 必须 剔除 混入 保留 题集 的 文本 以 保证 评测 公正 。"
        ),
    },
]


def _write_temp_jsonl(records: list[dict]) -> tuple[str, int]:
    fd, path = tempfile.mkstemp(prefix="demo-", suffix=".jsonl")
    size = 0
    with os.fdopen(fd, "wb") as fh:
        for rec in records:
            line = (json.dumps(rec, ensure_ascii=False) + "\n").encode("utf-8")
            fh.write(line)
            size += len(line)
    return path, size


def demo_retention() -> tuple[str, str, int]:
    path, size = _write_temp_jsonl(RETENTION_RECORDS)
    return path, "demo-retention.jsonl", size


def demo_training() -> tuple[str, str, int]:
    path, size = _write_temp_jsonl(TRAINING_RECORDS)
    return path, "demo-training.jsonl", size
