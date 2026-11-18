"""Live multi-process tests: two uvicorn processes share one data dir.

Covers: concurrent chunk uploads (incl. duplicate and conflicting content),
concurrent finalize from both processes, kill/restart mid-upload, and that
published packages remain downloadable after restarts. Indexing: concurrent
builds of the same package from two processes yield exactly one complete
index, and a package corrupted on disk stops yielding hits (stale index
removed) until the package is fixed and the index rebuilt.

Run:  .venv/bin/python -m pytest tests/test_live.py -v
"""
import hashlib
import io
import json
import os
import random
import socket
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
CHUNK = 1024 * 1024


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self, data_dir: Path, log_dir: Path):
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        env = dict(os.environ, DATA_DIR=str(data_dir), PYTHONPATH=str(ROOT))
        self.log = open(log_dir / f"server-{self.port}.log", "wb")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            env=env, cwd=str(ROOT), stdout=self.log, stderr=subprocess.STDOUT,
        )

    def wait_ready(self, timeout=30.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server on {self.port} exited early")
            try:
                if httpx.get(self.base + "/api/health", timeout=1).status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise TimeoutError(f"server on {self.port} not ready")

    def kill(self):
        self.proc.kill()
        self.proc.wait()
        self.log.close()

    def stop(self):
        self.proc.terminate()
        self.proc.wait(timeout=10)
        self.log.close()


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def create_task(client: httpx.Client, base: str, data: bytes) -> str:
    r = client.post(f"{base}/api/tasks", json={
        "filename": "diag.tar.gz", "size": len(data), "sha256": sha256(data)})
    assert r.status_code == 201, r.text
    return r.json()["task_id"]


def put_chunk(client: httpx.Client, base: str, tid: str, i: int, payload: bytes):
    return client.put(f"{base}/api/tasks/{tid}/chunks/{i}", content=payload)


def make_zip(files: dict) -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return bio.getvalue()


def publish(client: httpx.Client, base: str, data: bytes) -> str:
    tid = create_task(client, base, data)
    for i in range(0, len(data), CHUNK):
        r = put_chunk(client, base, tid, i // CHUNK, data[i:i + CHUNK])
        assert r.status_code == 200, r.text
    r = client.post(f"{base}/api/tasks/{tid}/finalize")
    assert r.json()["state"] == "published", r.text
    return tid


def test_two_processes_concurrent_upload_and_finalize(tmp_path):
    data_dir = tmp_path / "data"
    s1, s2 = Server(data_dir, tmp_path), Server(data_dir, tmp_path)
    try:
        s1.wait_ready()
        s2.wait_ready()
        data = random.Random(7).randbytes(5 * CHUNK + 999)  # 6 chunks
        chunks = [data[i:i + CHUNK] for i in range(0, len(data), CHUNK)]

        with httpx.Client(timeout=30) as client:
            tid = create_task(client, s1.base, data)

            # 两个进程并发上传：正常分块 + 同内容重复分块
            jobs = []
            for i, c in enumerate(chunks):
                jobs.append((s1.base if i % 2 == 0 else s2.base, i, c))
            # 同内容重传（打到另一个进程）
            for i in range(3):
                jobs.append((s2.base if i % 2 == 0 else s1.base, i, chunks[i]))

            def send(job):
                base, i, payload = job
                return put_chunk(client, base, tid, i, payload)

            with ThreadPoolExecutor(max_workers=9) as ex:
                results = list(ex.map(send, jobs))
            assert all(r.status_code == 200 for r in results), \
                [r.text for r in results]

            st = client.get(f"{s2.base}/api/tasks/{tid}").json()
            assert st["received_count"] == 6 and st["missing_chunks"] == []

            # 异内容重传：拒绝且保留原块
            evil = bytearray(chunks[0])
            evil[0] ^= 0xFF
            r = put_chunk(client, s2.base, tid, 0, bytes(evil))
            assert r.status_code == 409

            # 两个进程并发提交同一任务
            with ThreadPoolExecutor(max_workers=2) as ex:
                futs = [ex.submit(client.post, f"{b}/api/tasks/{tid}/finalize")
                        for b in (s1.base, s2.base)]
                finals = [f.result() for f in futs]
            assert all(r.status_code == 200 for r in finals), \
                [r.text for r in finals]
            assert all(r.json()["state"] == "published" for r in finals)

            # 两个进程下载到的成品一致且正确；磁盘上只有一份成品
            for base in (s1.base, s2.base):
                dl = client.get(f"{base}/api/tasks/{tid}/download")
                assert dl.status_code == 200 and dl.content == data
            published = list((data_dir / "published").iterdir())
            assert len(published) == 1
            assert published[0].read_bytes() == data
    finally:
        s1.stop()
        s2.stop()


def test_kill_restart_mid_upload_and_published_survives(tmp_path):
    data_dir = tmp_path / "data"
    data = random.Random(11).randbytes(3 * CHUNK + 1)  # 4 chunks
    chunks = [data[i:i + CHUNK] for i in range(0, len(data), CHUNK)]

    s1 = Server(data_dir, tmp_path)
    s1.wait_ready()
    with httpx.Client(timeout=30) as client:
        tid = create_task(client, s1.base, data)
        assert put_chunk(client, s1.base, tid, 0, chunks[0]).status_code == 200
        assert put_chunk(client, s1.base, tid, 1, chunks[1]).status_code == 200
    s1.kill()  # 模拟进程崩溃（kill -9）

    s2 = Server(data_dir, tmp_path)
    try:
        s2.wait_ready()
        with httpx.Client(timeout=30) as client:
            st = client.get(f"{s2.base}/api/tasks/{tid}").json()
            assert st["received_chunks"] == [0, 1]  # 已收分块保留
            assert put_chunk(client, s2.base, tid, 2, chunks[2]).status_code == 200
            assert put_chunk(client, s2.base, tid, 3, chunks[3]).status_code == 200
            r = client.post(f"{s2.base}/api/tasks/{tid}/finalize")
            assert r.status_code == 200 and r.json()["state"] == "published"
    finally:
        s2.kill()  # 发布后再崩溃

    s3 = Server(data_dir, tmp_path)
    try:
        s3.wait_ready()
        with httpx.Client(timeout=30) as client:
            dl = client.get(f"{s3.base}/api/tasks/{tid}/download")
            assert dl.status_code == 200 and dl.content == data  # 已发布仍可下载
    finally:
        s3.stop()


def test_concurrent_duplicate_same_chunk_across_processes(tmp_path):
    """Same chunk, same content, raced from two processes: all 200, one record."""
    data_dir = tmp_path / "data"
    s1, s2 = Server(data_dir, tmp_path), Server(data_dir, tmp_path)
    try:
        s1.wait_ready()
        s2.wait_ready()
        data = random.Random(3).randbytes(CHUNK)
        with httpx.Client(timeout=30) as client:
            tid = create_task(client, s1.base, data)
            with ThreadPoolExecutor(max_workers=2) as ex:
                r1 = ex.submit(put_chunk, client, s1.base, tid, 0, data)
                r2 = ex.submit(put_chunk, client, s2.base, tid, 0, data)
                results = [r1.result(), r2.result()]
            assert all(r.status_code == 200 for r in results)
            st = client.get(f"{s1.base}/api/tasks/{tid}").json()
            assert st["received_chunks"] == [0]
            assert client.post(f"{s2.base}/api/tasks/{tid}/finalize").json()[
                "state"] == "published"
            assert client.get(f"{s1.base}/api/tasks/{tid}/download").content == data
    finally:
        s1.stop()
        s2.stop()


def test_two_processes_concurrent_index_build(tmp_path):
    """Two processes building the same package's index: exactly one builds,
    exactly one complete index file exists, both processes search it."""
    data_dir = tmp_path / "data"
    s1, s2 = Server(data_dir, tmp_path), Server(data_dir, tmp_path)
    try:
        s1.wait_ready()
        s2.wait_ready()
        n_lines = 3000
        log = "".join(
            json.dumps({"message": f"ERROR line {i}"}) + "\n"
            for i in range(n_lines)
        )
        data = make_zip({"logs/app.log": log})

        with httpx.Client(timeout=60) as client:
            tid = publish(client, s1.base, data)

            # 两个进程并发建同一包的索引
            with ThreadPoolExecutor(max_workers=2) as ex:
                futs = [
                    ex.submit(client.post, f"{b}/api/tasks/{tid}/index")
                    for b in (s1.base, s2.base)
                ]
                results = [f.result() for f in futs]
            assert all(r.status_code == 200 for r in results), \
                [r.text for r in results]
            bodies = [r.json() for r in results]
            assert all(b["state"] == "indexed" for b in bodies)
            assert sum(1 for b in bodies if b["built"]) == 1  # 只有一个真正构建

            # 磁盘上只有一版完整索引：meta + 全部命中行
            idx_files = list((data_dir / "indexes").glob("*.jsonl"))
            assert len(idx_files) == 1
            lines = idx_files[0].read_text(encoding="utf-8").splitlines()
            meta = json.loads(lines[0])
            assert meta["_meta"] is True and meta["lines"] == n_lines
            assert len(lines) == n_lines + 1
            for raw in lines[1:]:
                json.loads(raw)  # 每行都是完整 JSON

            # 两个进程搜索结果一致
            for base in (s1.base, s2.base):
                r = client.get(f"{base}/api/search",
                               params={"q": "ERROR line 2999"})
                assert r.status_code == 200
                hits = r.json()["hits"]
                assert len(hits) == 1
                assert hits[0]["entry"] == "logs/app.log"
                assert hits[0]["line"] == 3000
    finally:
        s1.stop()
        s2.stop()


def test_stale_index_removed_and_rebuild_after_fix_across_processes(tmp_path):
    """原包被改坏后（另一进程）搜索不得返回旧命中；修复后可重建。"""
    data_dir = tmp_path / "data"
    data = make_zip({"a.log": '{"message": "ERROR alpha"}\n'})
    s1 = Server(data_dir, tmp_path)
    s1.wait_ready()
    with httpx.Client(timeout=30) as client:
        tid = publish(client, s1.base, data)
        assert client.post(f"{s1.base}/api/tasks/{tid}/index").json()["built"]
        r = client.get(f"{s1.base}/api/search", params={"q": "alpha"})
        assert len(r.json()["hits"]) == 1
    s1.stop()

    # 原包被改坏
    (data_dir / "published" / tid).write_bytes(b"corrupted!")
    s2 = Server(data_dir, tmp_path)
    try:
        s2.wait_ready()
        with httpx.Client(timeout=30) as client:
            r = client.get(f"{s2.base}/api/search", params={"q": "alpha"})
            body = r.json()
            assert body["hits"] == []  # 不得返回旧命中
            assert body["packages"][0]["state"] == "stale"
            assert not (data_dir / "indexes" / f"{tid}.jsonl").exists()
            # 改坏状态下建索引被拒绝
            assert client.post(
                f"{s2.base}/api/tasks/{tid}/index").status_code == 409

            # 修复原包后可重建，命中恢复
            (data_dir / "published" / tid).write_bytes(data)
            r = client.post(f"{s2.base}/api/tasks/{tid}/index")
            assert r.status_code == 200 and r.json()["built"] is True
            r = client.get(f"{s2.base}/api/search", params={"q": "alpha"})
            assert len(r.json()["hits"]) == 1
    finally:
        s2.stop()


def test_concurrent_build_and_search_never_partial(tmp_path):
    """Search racing a concurrent build sees either no index or the complete
    one — never partial hits."""
    data_dir = tmp_path / "data"
    s1, s2 = Server(data_dir, tmp_path), Server(data_dir, tmp_path)
    try:
        s1.wait_ready()
        s2.wait_ready()
        n_lines = 5000
        log = "".join(
            json.dumps({"message": f"ERROR row {i}"}) + "\n"
            for i in range(n_lines)
        )
        data = make_zip({"big.log": log})
        with httpx.Client(timeout=60) as client:
            tid = publish(client, s1.base, data)

            def build():
                return client.post(f"{s1.base}/api/tasks/{tid}/index")

            def search():
                return client.get(f"{s2.base}/api/search",
                                  params={"q": "ERROR row"})

            with ThreadPoolExecutor(max_workers=5) as ex:
                build_fut = ex.submit(build)
                search_futs = [ex.submit(search) for _ in range(8)]
                assert build_fut.result().status_code == 200
                for f in search_futs:
                    body = f.result().json()
                    # 要么索引尚未建立（0 命中），要么完整索引（全部命中）
                    total = sum(p["hits"] for p in body["packages"])
                    assert total in (0, n_lines), \
                        f"partial hits observed: {total}"
            # 建完后搜索必为完整命中
            body = search().json()
            assert sum(p["hits"] for p in body["packages"]) == n_lines
    finally:
        s1.stop()
        s2.stop()
