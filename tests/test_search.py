"""Functional tests (TestClient) for ZIP log indexing and literal search.

Covers: index build + search hits (task/entry/line/text), literal (non-regex)
matching, idempotent/forced builds, bad logs, duplicate zip entry paths,
corrupt zips, the 32 MiB cumulative decompression cap, package corruption
on disk (no stale hits, rebuild after fix), disk-failure injection, and
restart persistence.
"""
import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import storage as storage_mod  # noqa: E402
from app.main import CHUNK_SIZE, create_app  # noqa: E402


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def make_zip(files: dict) -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            if isinstance(content, str):
                content = content.encode("utf-8")
            zf.writestr(name, content)
    return bio.getvalue()


def make_zip_dup(name: str, a: bytes, b: bytes) -> bytes:
    """A zip containing the same entry path twice (legal per spec)."""
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, a)
        zf.writestr(name, b)
    return bio.getvalue()


def loglines(*msgs: str) -> str:
    return "".join(json.dumps({"message": m}) + "\n" for m in msgs)


def make_client(tmp_path: Path) -> TestClient:
    return TestClient(create_app(tmp_path / "data"))


def publish(client: TestClient, data: bytes, filename="bundle.zip") -> str:
    r = client.post(
        "/api/tasks",
        json={"filename": filename, "size": len(data), "sha256": sha256(data)},
    )
    assert r.status_code == 201, r.text
    tid = r.json()["task_id"]
    for i in range(0, len(data), CHUNK_SIZE):
        rr = client.put(
            f"/api/tasks/{tid}/chunks/{i // CHUNK_SIZE}",
            content=data[i : i + CHUNK_SIZE],
        )
        assert rr.status_code == 200, rr.text
    fr = client.post(f"/api/tasks/{tid}/finalize")
    assert fr.json()["state"] == "published", fr.text
    return tid


def index_path(tmp_path: Path, tid: str) -> Path:
    return tmp_path / "data" / "indexes" / f"{tid}.jsonl"


def published_path(tmp_path: Path, tid: str) -> Path:
    return tmp_path / "data" / "published" / tid


PKG1 = make_zip(
    {
        "logs/app.log": loglines(
            "INFO boot ok", "ERROR disk full on /data", "INFO retry ok"
        ),
        "logs/db.log": loglines(
            "ERROR connection refused", "WARN slow query", "ERROR db timeout"
        ),
    }
)
PKG2 = make_zip({"svc.log": loglines("ERROR disk full on /cache", "INFO done")})


def test_build_index_and_search(tmp_path):
    c = make_client(tmp_path)
    tid = publish(c, PKG1)

    r = c.post(f"/api/tasks/{tid}/index")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["state"] == "indexed" and body["built"] is True
    assert body["entries"] == 2 and body["lines"] == 6
    assert body["uncompressed"] > 0 and body["uncompressed"] <= 32 * 1024 * 1024

    # 索引文件落盘且首行为 meta
    raw = index_path(tmp_path, tid).read_text(encoding="utf-8").splitlines()
    meta = json.loads(raw[0])
    assert meta["_meta"] is True and meta["lines"] == 6 and meta["entries"] == 2
    assert len(raw) == 7

    # 任务详情/列表带索引状态
    st = c.get(f"/api/tasks/{tid}").json()
    assert st["index"]["indexed"] is True and st["index"]["lines"] == 6
    lst = c.get("/api/tasks").json()["tasks"]
    assert lst[0]["index"]["indexed"] is True

    # 搜索：命中携带 任务/条目/行号/原文
    r = c.get("/api/search", params={"q": "ERROR"})
    assert r.status_code == 200, r.text
    hits = r.json()["hits"]
    assert len(hits) == 3
    first = hits[0]
    assert first["task_id"] == tid and first["filename"] == "bundle.zip"
    assert first["entry"] == "logs/app.log" and first["line"] == 2
    assert first["message"] == "ERROR disk full on /data"
    assert json.loads(first["text"])["message"] == first["message"]
    assert {h["entry"] for h in hits} == {"logs/app.log", "logs/db.log"}
    assert r.json()["packages"] == [
        {"task_id": tid, "filename": "bundle.zip", "state": "searched", "hits": 3}
    ]

    # 短语（含空格）字面子串
    hits = c.get("/api/search", params={"q": "disk full on"}).json()["hits"]
    assert len(hits) == 1 and hits[0]["line"] == 2


def test_search_is_literal_not_regex(tmp_path):
    c = make_client(tmp_path)
    pkg = make_zip({"a.log": loglines("a.b[c](d) e*g", "aXb normal")})
    tid = publish(c, pkg)
    assert c.post(f"/api/tasks/{tid}/index").json()["built"] is True

    assert len(c.get("/api/search", params={"q": "a.b"}).json()["hits"]) == 1
    assert c.get("/api/search", params={"q": "aXb"}).json()["hits"][0][
        "message"
    ] == "aXb normal"
    # 正则元字符按字面处理
    assert c.get("/api/search", params={"q": "a.b[c]"}).json()["hits"]
    assert not c.get("/api/search", params={"q": "a.*g"}).json()["hits"]
    assert c.get("/api/search", params={"q": "e*g"}).json()["hits"]
    # 大小写敏感
    assert not c.get("/api/search", params={"q": "A.B"}).json()["hits"]
    # 空查询被拒绝
    assert c.get("/api/search", params={"q": ""}).status_code == 400


def test_search_across_packages_and_scoping(tmp_path):
    c = make_client(tmp_path)
    t1 = publish(c, PKG1, "b1.zip")
    t2 = publish(c, PKG2, "b2.zip")
    t3 = publish(c, PKG1, "b3.zip")  # 不建索引
    assert c.post(f"/api/tasks/{t1}/index").json()["built"] is True
    assert c.post(f"/api/tasks/{t2}/index").json()["built"] is True

    # 默认搜索覆盖所有已索引的包，未索引的包不参与
    r = c.get("/api/search", params={"q": "disk full"}).json()
    assert {h["task_id"] for h in r["hits"]} == {t1, t2}
    assert {p["task_id"] for p in r["packages"]} == {t1, t2}

    # 指定包搜索
    r = c.get("/api/search", params={"q": "disk full", "task_id": t2}).json()
    assert len(r["hits"]) == 1 and r["hits"][0]["entry"] == "svc.log"

    # 显式指定未建索引的包：明确告知 not_indexed
    r = c.get("/api/search", params={"q": "x", "task_id": t3}).json()
    assert r["hits"] == [] and r["packages"][0]["state"] == "not_indexed"

    # 命中数上限
    r = c.get("/api/search", params={"q": "ERROR", "limit": 2}).json()
    assert len(r["hits"]) == 2 and r["truncated"] is True
    assert sum(p["hits"] for p in r["packages"]) == 4


def test_build_index_idempotent_and_force(tmp_path):
    c = make_client(tmp_path)
    tid = publish(c, PKG1)
    r1 = c.post(f"/api/tasks/{tid}/index").json()
    assert r1["built"] is True
    r2 = c.post(f"/api/tasks/{tid}/index").json()
    assert r2["built"] is False and r2["state"] == "indexed"
    assert r2["lines"] == r1["lines"]
    r3 = c.post(f"/api/tasks/{tid}/index", params={"force": True}).json()
    assert r3["built"] is True
    # 仍然只有一个索引文件
    assert list((tmp_path / "data" / "indexes").glob("*.jsonl")) == [
        index_path(tmp_path, tid)
    ]


def test_index_requires_published_task(tmp_path):
    c = make_client(tmp_path)
    r = c.post(
        "/api/tasks",
        json={"filename": "x.zip", "size": 10, "sha256": sha256(b"0123456789")},
    )
    tid = r.json()["task_id"]
    assert c.post(f"/api/tasks/{tid}/index").status_code == 409
    assert c.post(f"/api/tasks/{'0' * 32}/index").status_code == 404


def test_bad_log_rejected_without_partial_index(tmp_path):
    c = make_client(tmp_path)
    bad_cases = [
        make_zip({"ok.log": loglines("INFO fine"), "bad.log": "not json at all\n"}),
        make_zip({"bad.log": '{"no_message": true}\n'}),
        make_zip({"bad.log": '["array", 1]\n'}),
        make_zip({"bad.log": '{"message": 42}\n'}),
        make_zip({"bad.log": b'{"message": "\xff\xfe"}\n'}),
    ]
    for i, pkg in enumerate(bad_cases):
        tid = publish(c, pkg, f"bad{i}.zip")
        r = c.post(f"/api/tasks/{tid}/index")
        assert r.status_code == 422, (i, r.text)
        assert r.json()["state"] == "failed" and r.json()["reason"] == "bad_log"
        assert not index_path(tmp_path, tid).exists()  # 不留部分索引
        assert c.get(f"/api/tasks/{tid}").json()["index"]["indexed"] is False
        # 失败可重试（结果仍是确定性失败，但不卡死、可再次调用）
        assert c.post(f"/api/tasks/{tid}/index").status_code == 422
    # 全部失败后搜索无任何命中
    assert c.get("/api/search", params={"q": "INFO"}).json()["hits"] == []


def test_duplicate_entry_rejected(tmp_path):
    c = make_client(tmp_path)
    pkg = make_zip_dup("dup.log", b'{"message": "one"}\n', b'{"message": "two"}\n')
    tid = publish(c, pkg)
    r = c.post(f"/api/tasks/{tid}/index")
    assert r.status_code == 422 and r.json()["reason"] == "duplicate_entry"
    assert not index_path(tmp_path, tid).exists()


def test_corrupt_zip_rejected(tmp_path):
    c = make_client(tmp_path)
    not_zip = b"this is definitely not a zip archive" * 10
    t1 = publish(c, not_zip, "notzip.zip")
    r = c.post(f"/api/tasks/{t1}/index")
    assert r.status_code == 422 and r.json()["reason"] == "zip_corrupt"

    truncated = PKG1[: len(PKG1) - 100]  # 截断：中央目录缺失
    t2 = publish(c, truncated, "truncated.zip")
    r = c.post(f"/api/tasks/{t2}/index")
    assert r.status_code == 422 and r.json()["reason"] == "zip_corrupt"

    # 压缩数据损坏：翻转数据区一个字节（保持大小与整体 sha 一致地落盘）
    corrupted = bytearray(PKG1)
    corrupted[40] ^= 0xFF
    t3 = publish(c, bytes(corrupted), "badcrc.zip")
    r = c.post(f"/api/tasks/{t3}/index")
    assert r.status_code == 422 and r.json()["reason"] == "zip_corrupt"

    for tid in (t1, t2, t3):
        assert not index_path(tmp_path, tid).exists()


def test_cumulative_decompression_cap(tmp_path):
    c = make_client(tmp_path)
    # 单行约 1 KiB 的合法 JSONL，累计解压超过 32 MiB（压缩后很小）
    line = json.dumps({"message": "A" * 1000}) + "\n"
    n_lines = (33 * 1024 * 1024) // len(line)  # 略超 32 MiB
    pkg = make_zip({"big.log": line * n_lines})
    assert len(pkg) < 5 * 1024 * 1024  # 高压缩比，上传快
    tid = publish(c, pkg)
    r = c.post(f"/api/tasks/{tid}/index")
    assert r.status_code == 422 and r.json()["reason"] == "too_large"
    assert not index_path(tmp_path, tid).exists()


def test_package_corrupted_on_disk_blocks_build(tmp_path):
    c = make_client(tmp_path)
    tid = publish(c, PKG1)
    published_path(tmp_path, tid).write_bytes(b"tampered content")
    r = c.post(f"/api/tasks/{tid}/index")
    assert r.status_code == 409 and r.json()["state"] == "corrupt"
    assert not index_path(tmp_path, tid).exists()


def test_package_deleted_on_disk_no_stale_hits(tmp_path):
    c = make_client(tmp_path)
    tid = publish(c, PKG1)
    assert c.post(f"/api/tasks/{tid}/index").json()["built"] is True
    published_path(tmp_path, tid).unlink()  # 原包丢失
    r = c.get("/api/search", params={"q": "ERROR"}).json()
    assert r["hits"] == [] and r["packages"][0]["state"] == "stale"
    assert not index_path(tmp_path, tid).exists()
    assert c.post(f"/api/tasks/{tid}/index").status_code == 409


def test_corrupted_package_yields_no_stale_hits_and_rebuild_after_fix(tmp_path):
    c = make_client(tmp_path)
    tid = publish(c, PKG1)
    assert c.post(f"/api/tasks/{tid}/index").json()["built"] is True
    assert len(c.get("/api/search", params={"q": "ERROR"}).json()["hits"]) == 3

    # 原包被改坏：搜索不得返回旧命中，旧索引被移除
    published_path(tmp_path, tid).write_bytes(b"tampered content")
    r = c.get("/api/search", params={"q": "ERROR"}).json()
    assert r["hits"] == []
    assert r["packages"][0]["state"] == "stale"
    assert not index_path(tmp_path, tid).exists()
    # 改坏状态下不能建索引
    assert c.post(f"/api/tasks/{tid}/index").status_code == 409

    # 修复原包后可重建，命中恢复
    published_path(tmp_path, tid).write_bytes(PKG1)
    r = c.post(f"/api/tasks/{tid}/index")
    assert r.status_code == 200 and r.json()["built"] is True
    assert len(c.get("/api/search", params={"q": "ERROR"}).json()["hits"]) == 3


def test_disk_failure_on_index_publish_leaves_no_index(tmp_path, monkeypatch):
    c = make_client(tmp_path)
    tid = publish(c, PKG1)

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(storage_mod, "replace", boom)
    r = c.post(f"/api/tasks/{tid}/index")
    assert r.status_code == 500
    assert not index_path(tmp_path, tid).exists()  # 无残缺索引
    assert list((tmp_path / "data" / "indexes").iterdir()) == []

    monkeypatch.undo()  # 失败可重试
    assert c.post(f"/api/tasks/{tid}/index").json()["built"] is True
    assert len(c.get("/api/search", params={"q": "ERROR"}).json()["hits"]) == 3


def test_index_survives_restart(tmp_path):
    c1 = make_client(tmp_path)
    tid = publish(c1, PKG1)
    assert c1.post(f"/api/tasks/{tid}/index").json()["built"] is True

    c2 = make_client(tmp_path)  # 进程重启：同一数据目录
    st = c2.get(f"/api/tasks/{tid}").json()
    assert st["index"]["indexed"] is True and st["index"]["lines"] == 6
    hits = c2.get("/api/search", params={"q": "disk full"}).json()["hits"]
    assert len(hits) == 1 and hits[0]["entry"] == "logs/app.log"


def test_blank_lines_crlf_and_line_numbers(tmp_path):
    c = make_client(tmp_path)
    content = (
        b'{"message": "first"}\r\n'
        b"\n"
        b"   \n"
        b'{"message": "ERROR fourth"}\r\n'
    )
    pkg = make_zip({"crlf.log": content})
    tid = publish(c, pkg)
    r = c.post(f"/api/tasks/{tid}/index").json()
    assert r["built"] is True and r["lines"] == 2  # 空行跳过但占行号
    hits = c.get("/api/search", params={"q": "fourth"}).json()["hits"]
    assert hits[0]["line"] == 4
    assert hits[0]["text"] == '{"message": "ERROR fourth"}'


def test_empty_zip_and_directories_only(tmp_path):
    c = make_client(tmp_path)
    t1 = publish(c, make_zip({}), "empty.zip")
    r = c.post(f"/api/tasks/{t1}/index").json()
    assert r["built"] is True and r["entries"] == 0 and r["lines"] == 0

    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w") as zf:
        zf.writestr("logs/", b"")
    t2 = publish(c, bio.getvalue(), "dirs.zip")
    r = c.post(f"/api/tasks/{t2}/index").json()
    assert r["built"] is True and r["entries"] == 0

    r = c.get("/api/search", params={"q": "anything"}).json()
    assert r["hits"] == []
    assert {p["state"] for p in r["packages"]} == {"searched"}


def test_broken_index_file_not_served(tmp_path):
    c = make_client(tmp_path)
    tid = publish(c, PKG1)
    assert c.post(f"/api/tasks/{tid}/index").json()["built"] is True
    # 索引文件自身损坏：不得返回命中，损坏索引被清除
    index_path(tmp_path, tid).write_bytes(b"\x00\x01garbage")
    r = c.get("/api/search", params={"q": "ERROR"}).json()
    assert r["hits"] == [] and r["packages"][0]["state"] == "index_error"
    assert not index_path(tmp_path, tid).exists()
    # 可重建
    assert c.post(f"/api/tasks/{tid}/index").json()["built"] is True


def test_upload_download_still_works_with_index(tmp_path):
    c = make_client(tmp_path)
    tid = publish(c, PKG1)
    assert c.post(f"/api/tasks/{tid}/index").json()["built"] is True
    dl = c.get(f"/api/tasks/{tid}/download")
    assert dl.status_code == 200 and dl.content == PKG1
