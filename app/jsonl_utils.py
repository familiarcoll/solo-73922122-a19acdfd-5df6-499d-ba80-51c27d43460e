"""JSONL 流式读取：严格限制单行与总字节，解析 id/text 记录。

所有读取以二进制流式进行，单行超过 64 KiB 即报错，
避免把超大文件整体读入内存，满足 128 MiB 容器内存约束。
"""
from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass

from .config import MAX_FILE_BYTES, MAX_LINE_BYTES
from .errors import ValidationError


@dataclass(frozen=True)
class JsonlRecord:
    id: str
    text: str
    raw: bytes  # 原始行字节（含行尾），净化输出逐字节复用


def parse_line(raw: bytes, line_no: int) -> JsonlRecord:
    """解析并校验单行 JSONL。"""
    if not raw.strip():
        raise ValidationError(f"第 {line_no} 行为空，JSONL 不允许空行")
    try:
        obj = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ValidationError(f"第 {line_no} 行不是合法 UTF-8：{exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValidationError(f"第 {line_no} 行不是合法 JSON：{exc.msg}") from exc
    if not isinstance(obj, dict):
        raise ValidationError(f"第 {line_no} 行必须是 JSON 对象")
    rid = obj.get("id")
    text = obj.get("text")
    if not isinstance(rid, str) or not isinstance(text, str):
        raise ValidationError(
            f"第 {line_no} 行必须包含字符串类型的 id 与 text 字段"
        )
    return JsonlRecord(id=rid, text=text, raw=raw)


def iter_jsonl(
    path: str | os.PathLike[str],
    *,
    max_file_bytes: int = MAX_FILE_BYTES,
    max_line_bytes: int = MAX_LINE_BYTES,
) -> Iterator[JsonlRecord]:
    """逐行产出 JSONL 记录，做结构与大小校验（id 唯一性由调用方处理）。"""
    total = 0
    line_no = 0
    with open(path, "rb") as fh:
        for raw in fh:
            line_no += 1
            total += len(raw)
            if len(raw) > max_line_bytes:
                raise ValidationError(
                    f"第 {line_no} 行 {len(raw)} 字节，超过单行上限 "
                    f"{max_line_bytes} 字节（64 KiB）"
                )
            if total > max_file_bytes:
                raise ValidationError(
                    f"文件超过 {max_file_bytes} 字节（256 MiB）上限"
                )
            yield parse_line(raw, line_no)


def iter_jsonl_with_position(
    path: str | os.PathLike[str],
    *,
    max_file_bytes: int = MAX_FILE_BYTES,
    max_line_bytes: int = MAX_LINE_BYTES,
) -> Iterator[tuple[JsonlRecord, int, int]]:
    """同 iter_jsonl，额外返回 (已读字节数, 文件总字节) 供进度展示。"""
    total_bytes = os.path.getsize(path)
    read_bytes = 0
    line_no = 0
    with open(path, "rb") as fh:
        for raw in fh:
            line_no += 1
            read_bytes += len(raw)
            if len(raw) > max_line_bytes:
                raise ValidationError(
                    f"第 {line_no} 行 {len(raw)} 字节，超过单行上限 "
                    f"{max_line_bytes} 字节（64 KiB）"
                )
            if read_bytes > max_file_bytes:
                raise ValidationError(
                    f"文件超过 {max_file_bytes} 字节（256 MiB）上限"
                )
            yield parse_line(raw, line_no), read_bytes, total_bytes
