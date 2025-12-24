# streamlit_annotator/libs/locker.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Optional


def try_acquire_lock(lock_path: Path, sid: str) -> bool:
    """
    原子创建 lock 文件（成功=获得锁；失败=已被占用）
    lock 文件内容写入 sid 与 ts，便于 TTL 回收。
    """
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"sid": sid, "ts": time.time()}, f, ensure_ascii=False)
        return True
    except FileExistsError:
        return False


def read_lock(lock_path: Path) -> Optional[dict]:
    if not lock_path.exists():
        return None
    try:
        return json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def refresh_lock(lock_path: Path, sid: str) -> None:
    """
    更新锁的时间戳，防止被 TTL 回收（best-effort）。
    """
    if not lock_path.exists():
        return
    try:
        lock_path.write_text(json.dumps({"sid": sid, "ts": time.time()}, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)  # py3.8+ ok, py3.12 ok
    except Exception:
        pass


def is_lock_stale(lock_path: Path, ttl_seconds: int) -> bool:
    """
    判断锁是否过期（用于“意外关闭页面”后的回收）
    """
    info = read_lock(lock_path)
    if not info:
        # lock 文件坏了/读不到，直接视为过期
        return True
    ts = info.get("ts", 0)
    try:
        ts = float(ts)
    except Exception:
        return True
    return (time.time() - ts) > ttl_seconds
