# streamlit_annotator/libs/bank_io.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple


def load_questionnaire_jsonl(path: Path) -> Tuple[dict, List[Dict[str, Any]]]:
    """
    读取问卷文件：
    - 如果第 1 行是 {"__meta__": {...}}，则视为 meta 行
    - 否则 meta={}
    返回 (meta, questions)
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return {}, []

    first = lines[0].strip()
    meta = {}
    start = 0
    try:
        obj0 = json.loads(first)
        if isinstance(obj0, dict) and "__meta__" in obj0:
            meta = obj0["__meta__"] or {}
            start = 1
    except Exception:
        pass

    questions: List[Dict[str, Any]] = []
    for ln in lines[start:]:
        ln = ln.strip()
        if not ln:
            continue
        questions.append(json.loads(ln))
    return meta, questions


def is_done(meta: dict) -> bool:
    return str(meta.get("status", "")).lower() == "done"


def atomic_write_jsonl(path: Path, meta: dict, questions: List[Dict[str, Any]]) -> None:
    """
    原子写回：写到临时文件，再 replace 覆盖，避免并发/中途崩溃导致文件损坏。
    """
    tmp = path.with_suffix(path.suffix + f".tmp_{int(time.time()*1000)}")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(json.dumps({"__meta__": meta}, ensure_ascii=False) + "\n")
        for q in questions:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")
    tmp.replace(path)
