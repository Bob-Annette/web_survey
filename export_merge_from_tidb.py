# streamlit_annotator/scripts/export_merge_from_tidb.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import pymysql


def connect_tidb(host: str, port: int, user: str, password: str, database: str, ca_path: str | None):
    ssl = None
    if ca_path:
        ssl = {"ca": ca_path}

    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        ssl=ssl,
        connect_timeout=10,
        read_timeout=60,
        write_timeout=60,
    )


def parse_json_field(v) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
    if isinstance(v, str):
        return json.loads(v)
    return json.loads(str(v))


def dt_to_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, datetime):
        return x.isoformat(sep=" ", timespec="seconds")
    # pymysql 有时会给 str
    return str(x)


def atomic_write_jsonl(path: Path, meta: dict, questions: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp_{int(datetime.now().timestamp() * 1000)}")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(json.dumps({"__meta__": meta}, ensure_ascii=False) + "\n")
        for q in questions:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")
    tmp.replace(path)


def fetch_questionnaires(conn, only_done: bool) -> List[dict]:
    sql = """
    SELECT qid, bank, rel_path, payload, question_count, status, claimed_by, claimed_at
    FROM questionnaires
    """
    if only_done:
        sql += " WHERE status='done' "
    sql += " ORDER BY bank, rel_path;"

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return rows


def fetch_submissions_for_qid(conn, qid: str) -> List[dict]:
    sql = """
    SELECT sid, submitted_at, answers
    FROM submissions
    WHERE qid=%s
    ORDER BY submitted_at ASC, id ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (qid,))
        rows = cur.fetchall()
    return rows


def merge_one_questionnaire(
    q_row: dict,
    submissions: List[dict],
) -> Tuple[dict, List[Dict[str, Any]]]:
    """
    输出结构与 Step2 的问卷一致（jsonl + __meta__），但每条题目多一个：
      "choice": [
        {"sid": "...", "submitted_at": "...", "choice": "A/B/C"},
        ...
      ]
    """
    payload = parse_json_field(q_row["payload"])
    meta = payload.get("meta", {}) or {}
    questions: List[Dict[str, Any]] = payload.get("questions", []) or []

    # 建索引：题目 qid -> choice list
    choice_map: Dict[str, List[Dict[str, Any]]] = {}
    for q in questions:
        qid = q.get("qid")
        if qid is not None:
            choice_map[str(qid)] = []

    # 逐份 submission 合并进每题的 choice list
    for s in submissions:
        sid = str(s.get("sid", ""))
        submitted_at = dt_to_str(s.get("submitted_at"))
        ans_obj = parse_json_field(s.get("answers"))

        # answers 是 {question_qid: "A"/"B"/"C"} 的 dict
        for qid_str, lst in choice_map.items():
            chosen = ans_obj.get(qid_str, None)
            if chosen is None:
                continue
            lst.append(
                {
                    "sid": sid,
                    "submitted_at": submitted_at,
                    "choice": str(chosen),
                }
            )

    # 把 choice list 写回每道题（即使为空也写，便于后续统计）
    merged_questions: List[Dict[str, Any]] = []
    for q in questions:
        qid_str = str(q.get("qid"))
        q_out = dict(q)
        q_out["choice"] = choice_map.get(qid_str, [])
        merged_questions.append(q_out)

    # meta 可附加导出信息（不影响你后续统计）
    meta_out = dict(meta)
    meta_out.update(
        {
            "exported_at": datetime.now().isoformat(sep=" ", timespec="seconds"),
            "tidb_qid": q_row["qid"],
            "bank": q_row["bank"],
            "rel_path": q_row["rel_path"],
            "status": q_row.get("status"),
            "submission_count": len(submissions),
        }
    )

    return meta_out, merged_questions


def main():
    p = argparse.ArgumentParser("Export & merge questionnaires+submissions from TiDB into local banks-like folders.")
    p.add_argument("--output_dir", required=True, help="导出目录：会在其下生成 banks 结构（bank/问卷文件）")
    p.add_argument("--host", default="gateway01.eu-central-1.prod.aws.tidbcloud.com")
    p.add_argument("--port", type=int, default=4000)
    p.add_argument("--user", default="2b1cKMtrfhzxjYj.root")
    p.add_argument("--database", default="github_sample")
    p.add_argument("--password_env", default="TIDB_PASSWORD")
    p.add_argument("--ca_env", default="TIDB_CA")
    p.add_argument("--only_done", action="store_true", help="只导出 status='done' 的问卷（默认导出全部）")
    p.add_argument("--limit", type=int, default=0, help="只导出前 limit 份问卷（0=不限制）")
    args = p.parse_args()

    out_root = Path(args.output_dir).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    password = os.environ.get(args.password_env, "")
    if not password:
        raise RuntimeError(f"Missing password env: {args.password_env}")

    ca_path = os.environ.get(args.ca_env, "") or None
    if ca_path:
        ca_path = str(Path(ca_path).resolve())

    conn = connect_tidb(args.host, args.port, args.user, password, args.database, ca_path)
    try:
        q_rows = fetch_questionnaires(conn, only_done=args.only_done)
        if args.limit and args.limit > 0:
            q_rows = q_rows[: args.limit]

        total_q = len(q_rows)
        print(f"[scan] questionnaires rows: {total_q}  (only_done={args.only_done})")
        written = 0
        total_sub = 0

        for idx, q_row in enumerate(q_rows, start=1):
            qid = str(q_row["qid"])
            rel_path = str(q_row["rel_path"])  # 例如 bank_x/questionnaire_001.jsonl
            sub_rows = fetch_submissions_for_qid(conn, qid)
            total_sub += len(sub_rows)

            meta_out, merged_questions = merge_one_questionnaire(q_row, sub_rows)

            out_path = out_root / Path(rel_path)  # 保持 Step2 的 bank/问卷结构
            atomic_write_jsonl(out_path, meta_out, merged_questions)

            written += 1
            if written % 50 == 0 or idx == total_q:
                print(f"[export] {written}/{total_q} written... (submissions seen={total_sub})")

        # 总结
        print(f"[OK] exported questionnaires: {written}")
        print(f"[OK] submissions processed: {total_sub}")
        print(f"[OK] output_dir: {out_root}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
