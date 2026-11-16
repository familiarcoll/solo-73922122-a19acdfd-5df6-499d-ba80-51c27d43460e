"""核心清洗规则测试：分词、8 词命中、原行字节、校验、磁盘索引。"""
from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import MAX_LINE_BYTES  # noqa: E402
from app.errors import ValidationError  # noqa: E402
from app.index_store import IndexReader, build_index  # noqa: E402
from app.jsonl_utils import iter_jsonl  # noqa: E402
from app.tokenizer import encode_window, iter_ngrams, tokenize  # noqa: E402


def write_jsonl(path, records=None, raw_lines=None):
    with open(path, "wb") as fh:
        if raw_lines is not None:
            for line in raw_lines:
                fh.write(line if isinstance(line, bytes) else line.encode())
        else:
            for rec in records:
                fh.write((json.dumps(rec, ensure_ascii=False) + "\n").encode())
    return path


def test_tokenizer_unicode_whitespace_and_case():
    # 含普通空格、Tab、全角空格（U+3000）、换行等 Unicode 空白
    text = "alpha\tbeta　gamma\nalpha"
    assert tokenize(text) == ["alpha", "beta", "gamma", "alpha"]
    # 区分大小写
    assert tokenize("Alpha alpha") == ["Alpha", "alpha"]


def test_window_encoding_is_unambiguous():
    k1 = encode_window(["ab", "c"])
    k2 = encode_window(["a", "bc"])
    assert k1 != k2  # 长度前缀避免分隔符歧义


def test_ngram_count_and_order():
    tokens = ["w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9", "w10"]
    grams = list(iter_ngrams(tokens))
    assert len(grams) == 3
    assert grams[0][2] == tokens[:8]
    assert grams[-1][2] == tokens[2:10]
    # 不足 8 词不产出窗口
    assert list(iter_ngrams(tokens[:7])) == []


def test_build_and_lookup_index(tmp_path):
    ret = tmp_path / "ret.jsonl"
    write_jsonl(ret, [
        {"id": "r1", "text": " ".join(f"w{i}" for i in range(12))},
        {"id": "r2", "text": "hello " * 10},
    ])
    idx = tmp_path / "idx.sqlite"
    stats = build_index(str(idx), str(ret))
    assert stats["records"] == 2
    # 去重后窗口：r1 的 5 个 + r2 的 3 个（hello 重复导致窗口互相重复）
    assert stats["windows"] == (12 - 8 + 1) + 1

    with IndexReader(str(idx)) as reader:
        grams = list(iter_ngrams(tokenize(" ".join(f"w{i}" for i in range(12)))))
        assert reader.lookup(*grams[0][:2]) == "r1"
        # 大小写变化不命中
        upper = list(iter_ngrams(tokenize((" ".join(f"w{i}" for i in range(12))).upper())))
        assert reader.lookup(*upper[0][:2]) is None
        # 完全不同的窗口不命中
        other = list(iter_ngrams(tokenize("nothing matches this index at all now today either")))
        assert reader.lookup(*other[0][:2]) is None


def test_index_rejects_duplicate_retention_id(tmp_path):
    ret = tmp_path / "ret.jsonl"
    write_jsonl(ret, [
        {"id": "dup", "text": "a b c d e f g h"},
        {"id": "dup", "text": "a2 b2 c2 d2 e2 f2 g2 h2"},
    ])
    idx = tmp_path / "idx.sqlite"
    with pytest.raises(ValueError):
        build_index(str(idx), str(ret))
    assert not idx.exists()  # 失败不留半成品


def test_jsonl_validation(tmp_path):
    good = tmp_path / "good.jsonl"
    write_jsonl(good, [{"id": "a", "text": "x y"}])
    recs = list(iter_jsonl(good))
    assert recs[0].id == "a"
    assert recs[0].raw.endswith(b"\n")

    bad_json = tmp_path / "bad.jsonl"
    write_jsonl(bad_json, raw_lines=['{"id": "a", "text": "x"}\n', "not-json\n"])
    with pytest.raises(ValidationError, match="合法 JSON"):
        list(iter_jsonl(bad_json))

    bad_schema = tmp_path / "schema.jsonl"
    write_jsonl(bad_schema, raw_lines=['{"id": 1, "text": "x"}\n'])
    with pytest.raises(ValidationError, match="字符串类型"):
        list(iter_jsonl(bad_schema))

    blank = tmp_path / "blank.jsonl"
    write_jsonl(blank, raw_lines=['{"id": "a", "text": "x"}\n', "\n"])
    with pytest.raises(ValidationError, match="空行"):
        list(iter_jsonl(blank))

    long_line = tmp_path / "long.jsonl"
    write_jsonl(
        long_line,
        raw_lines=[
            json.dumps({"id": "a", "text": "x" * MAX_LINE_BYTES}).encode() + b"\n"
        ],
    )
    with pytest.raises(ValidationError, match="单行上限"):
        list(iter_jsonl(long_line))
