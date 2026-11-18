"""JSONL log indexing and literal substring search for published ZIP packages.

Index build rules:
  * the on-disk published package is re-verified against the task's
    registered size + SHA-256 before anything is read from it;
  * every regular (non-directory) ZIP entry is parsed as UTF-8 JSONL: each
    non-blank line must be a JSON object with a string "message" field;
  * cumulative decompressed bytes across all entries may not exceed
    MAX_UNCOMPRESSED (32 MiB). Actually inflated bytes are counted as they
    are read, so a zip bomb is aborted as soon as it crosses the limit
    regardless of what the central directory claims;
  * duplicate entry paths, unparseable lines, corrupt zips and oversize
    content all abort the build; the index is written to a temp file and
    atomically renamed into indexes/, so a failure or a crash never leaves
    a partial index that could produce partial hits.

Index file format (JSONL, one JSON object per line):
  line 1:   {"_meta": true, "format": 1, "task_id": ..., "sha256": ...,
             "size": ..., "entries": N, "lines": M, "uncompressed": B,
             "built_at": ...}
  line 2+:  {"entry": <zip entry path>, "line": <1-based line no>,
             "message": <str>, "text": <original raw line>}

Search rules:
  * literal, case-sensitive substring match on the "message" field;
  * before a package's index is consulted, the on-disk package is
    re-hashed and compared with the registered size + SHA-256; if it no
    longer matches, the stale index is removed and contributes no hits.
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid
import zipfile
import zlib
from pathlib import Path

from . import storage as storage_mod

MAX_UNCOMPRESSED = 32 * 1024 * 1024  # 32 MiB cumulative decompressed budget
INDEX_FORMAT = 1
_READ_CHUNK = 64 * 1024
_META_MAX_LINE = 1024 * 1024  # first line of an index file may not exceed 1 MiB


class IndexBuildError(Exception):
    """Deterministic content problem; the build leaves no index behind."""

    def __init__(self, reason: str, detail: str):
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


class IndexFileError(Exception):
    """An existing index file is unreadable/corrupted on disk."""


def verify_package(path: Path, size: int, sha256: str) -> bool:
    """True iff the on-disk file still matches the registered size+sha256."""
    try:
        if os.stat(path).st_size != size:
            return False
        digest = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                buf = f.read(1024 * 1024)
                if not buf:
                    break
                digest.update(buf)
    except OSError:
        return False
    return digest.hexdigest() == sha256


class _Budget:
    """Cumulative decompressed-byte budget across all zip entries."""

    def __init__(self, limit: int):
        self.limit = limit
        self.used = 0

    def add(self, n: int) -> None:
        self.used += n
        if self.used > self.limit:
            raise IndexBuildError(
                "too_large",
                f"cumulative decompressed data exceeds the 32 MiB limit"
                f" ({self.used} bytes so far)",
            )


def _iter_lines(raw, budget: _Budget):
    """Yield raw lines (without trailing newline) from a binary stream.

    Decompressed bytes are charged to *budget* as they are read, so the
    stream is aborted promptly once the limit is crossed.
    """
    pending = []
    while True:
        chunk = raw.read(_READ_CHUNK)
        if not chunk:
            break
        budget.add(len(chunk))
        start = 0
        while True:
            nl = chunk.find(b"\n", start)
            if nl == -1:
                pending.append(chunk[start:])
                break
            pending.append(chunk[start:nl])
            line = b"".join(pending)
            pending.clear()
            yield line
            start = nl + 1
    if pending:
        yield b"".join(pending)


def _parse_log_line(entry: str, lineno: int, raw_line: bytes) -> dict | None:
    """Parse one log line; None for blank lines, IndexBuildError for bad ones."""
    stripped = raw_line.rstrip(b"\r")
    if not stripped.strip():
        return None  # blank line: skipped, but it still consumed a line number
    try:
        text = stripped.decode("utf-8")
    except UnicodeDecodeError:
        raise IndexBuildError(
            "bad_log", f"{entry}:{lineno}: line is not valid UTF-8"
        )
    try:
        obj = json.loads(text)
    except ValueError:
        raise IndexBuildError(
            "bad_log", f"{entry}:{lineno}: line is not valid JSON"
        )
    if not isinstance(obj, dict) or not isinstance(obj.get("message"), str):
        raise IndexBuildError(
            "bad_log",
            f"{entry}:{lineno}: line is not a JSON object with a string"
            " 'message' field",
        )
    return {
        "entry": entry,
        "line": lineno,
        "message": obj["message"],
        "text": text,
    }


def build_index(storage, task, now_iso: str) -> dict:
    """Build indexes/<task_id>.jsonl from the published package, atomically.

    Raises IndexBuildError on content problems (no partial index is left),
    OSError on disk failures.
    """
    task_id = task["id"]
    pkg = storage.published_path(task_id)
    hits_tmp = storage.tmp_dir / f".idx-hits-{uuid.uuid4().hex}"
    final_tmp = storage.tmp_dir / f".idx-{uuid.uuid4().hex}"
    budget = _Budget(MAX_UNCOMPRESSED)
    entries = 0
    lines = 0
    try:
        with open(hits_tmp, "wb") as out:
            try:
                zf = zipfile.ZipFile(pkg)
            except zipfile.BadZipFile as exc:
                raise IndexBuildError(
                    "zip_corrupt", f"not a readable zip archive: {exc}"
                )
            with zf:
                seen = set()
                for name in zf.namelist():
                    if name in seen:
                        raise IndexBuildError(
                            "duplicate_entry",
                            f"duplicate zip entry path: {name!r}",
                        )
                    seen.add(name)
                try:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        entries += 1
                        with zf.open(info, "r") as raw:
                            for lineno, raw_line in enumerate(
                                _iter_lines(raw, budget), start=1
                            ):
                                rec = _parse_log_line(
                                    info.filename, lineno, raw_line
                                )
                                if rec is None:
                                    continue
                                out.write(
                                    json.dumps(rec, ensure_ascii=False).encode(
                                        "utf-8"
                                    )
                                    + b"\n"
                                )
                                lines += 1
                except IndexBuildError:
                    raise
                except (
                    zipfile.BadZipFile,
                    RuntimeError,  # encrypted entries
                    NotImplementedError,  # unsupported compression method
                    EOFError,
                    zlib.error,
                    OverflowError,
                ) as exc:
                    raise IndexBuildError(
                        "zip_corrupt", f"zip data corrupted or unsupported: {exc}"
                    )
            out.flush()
            os.fsync(out.fileno())

        meta = {
            "_meta": True,
            "format": INDEX_FORMAT,
            "task_id": task_id,
            "sha256": task["sha256"],
            "size": task["size"],
            "entries": entries,
            "lines": lines,
            "uncompressed": budget.used,
            "built_at": now_iso,
        }
        with open(final_tmp, "wb") as f:
            f.write(json.dumps(meta, ensure_ascii=False).encode("utf-8") + b"\n")
            with open(hits_tmp, "rb") as h:
                while True:
                    buf = h.read(1024 * 1024)
                    if not buf:
                        break
                    f.write(buf)
            f.flush()
            os.fsync(f.fileno())

        storage_mod.replace(str(final_tmp), str(storage.index_path(task_id)))
        storage_mod.fsync_dir(storage.indexes_dir)
    finally:
        storage_mod.unlink_quiet(hits_tmp)
        storage_mod.unlink_quiet(final_tmp)

    return {
        "entries": entries,
        "lines": lines,
        "uncompressed": budget.used,
        "sha256": task["sha256"],
        "size": task["size"],
        "built_at": meta["built_at"],
    }


def read_index_meta(path: Path) -> dict | None:
    """Return the meta dict of an index file, or None if missing/invalid."""
    try:
        with open(path, "rb") as f:
            line = f.readline(_META_MAX_LINE + 2)
        if len(line) > _META_MAX_LINE:
            return None
        obj = json.loads(line)
    except (OSError, ValueError):
        return None
    if isinstance(obj, dict) and obj.get("_meta") is True:
        return obj
    return None


def iter_index_hits(path: Path, query: str):
    """Yield hit dicts whose message contains *query* (literal substring).

    Raises IndexFileError if the index file is malformed.
    """
    try:
        f = open(path, "r", encoding="utf-8")
    except OSError as exc:
        raise IndexFileError(str(exc))
    with f:
        try:
            first = f.readline()
            meta = json.loads(first)
            if not (isinstance(meta, dict) and meta.get("_meta") is True):
                raise IndexFileError("index meta line missing")
            for raw in f:
                obj = json.loads(raw)
                message = obj["message"]
                if not isinstance(message, str):
                    raise KeyError("message")
                if query in message:
                    yield {
                        "entry": obj["entry"],
                        "line": obj["line"],
                        "message": message,
                        "text": obj["text"],
                    }
        except IndexFileError:
            raise
        except (ValueError, KeyError, TypeError) as exc:
            # JSON errors, UTF-8 decode errors, missing/wrong-typed fields
            raise IndexFileError(f"index record corrupted: {exc}")
