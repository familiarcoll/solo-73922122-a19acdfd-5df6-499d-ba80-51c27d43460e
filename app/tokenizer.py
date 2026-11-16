"""分词与连续 8 词窗口。

规则：按 Unicode 空白分词（str.split 覆盖全部 Unicode White_Space），
区分大小写，不做大小写归一化、不去标点。
"""
from __future__ import annotations

import hashlib
from collections import deque
from collections.abc import Iterator

from .config import NGRAM_SIZE

# 单元分隔符（控制字符，正常文本不会出现），配合长度前缀编码窗口键。
_SEP = "\x1f"


def tokenize(text: str) -> list[str]:
    """按 Unicode 空白分词，保留词原貌（区分大小写、含标点）。"""
    return text.split()


def encode_window(window: list[str] | deque[str]) -> bytes:
    """把词窗口编码为无歧义字节串（长度前缀 + 单元分隔符）。"""
    parts: list[bytes] = []
    for tok in window:
        parts.append(str(len(tok)).encode("utf-8"))
        parts.append(b":")
        parts.append(tok.encode("utf-8"))
        parts.append(_SEP.encode("utf-8"))
    return b"".join(parts)


def window_hash(key: bytes) -> bytes:
    """窗口键的 128 位哈希，用作 SQLite 索引主键前缀。"""
    return hashlib.blake2b(key, digest_size=16).digest()


def iter_ngrams(tokens: list[str], size: int = NGRAM_SIZE) -> Iterator[tuple[bytes, bytes, list[str]]]:
    """滑动窗口产出 (128 位哈希, 精确键, 8 个词的列表)。

    词数不足 8 的记录不产出任何窗口，因此不会被剔除。
    """
    if len(tokens) < size:
        return
    window: deque[str] = deque(tokens[:size])
    for i in range(len(tokens) - size + 1):
        if i > 0:
            window.popleft()
            window.append(tokens[i + size - 1])
        word_list = list(window)
        key = encode_window(word_list)
        yield window_hash(key), key, word_list
