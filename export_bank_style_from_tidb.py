# streamlit_annotator/scripts/export_bank_style_from_tidb.py
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
        read_timeout=120,
        write_timeout=120,
    )


def parse_json(v) -> Dict[str, Any]:
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
    return str(x)


def safe_get_row_index(item: dict) -> int:
    # 用 Step1 的 source.row_index 作为排序基准（若存在）
    try:
        src = item.get("source", {}) or {}
        ri = src.get("row_index", None)
        if ri is None:
            return 10**18
        return int(ri)
    except Exception:
        return 10**18


def atomic_write_jsonl(path: Path, records: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp_{int(datetime.now().timestamp() * 1000)}")
    with tmp.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(path)


def fetch_all_questionnaires(conn, only_done: bool) -> List[dict]:
    sql = """
    SELECT qid, bank, rel_path, payload, status
    FROM questionnaires
    """
    if only_done:
        sql += " WHERE status='done' "
    sql += " ORDER BY bank, rel_path;"
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def fetch_all_submissions(conn) -> List[dict]:
    # submissions.qid 是“问卷ID（questionnaire id）”，不是题目 qid
    sql = """
    SELECT qid AS questionnaire_id, sid, submitted_at, answers
    FROM submissions
    ORDER BY submitted_at ASC, id ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def main():
    p = argparse.ArgumentParser(
        "Export TiDB submissions and merge back into Step1 bank style jsonl (one line per question item)."
    )
    p.add_argument("--output_dir", required=True, help="输出根目录：将生成 output_dir/<bank_name>/bank_merged.jsonl")
    p.add_argument("--host", default="gateway01.eu-central-1.prod.aws.tidbcloud.com")
    p.add_argument("--port", type=int, default=4000)
    p.add_argument("--user", default="2b1cKMtrfhzxjYj.root")
    p.add_argument("--database", default="github_sample")
    p.add_argument("--password_env", default="TIDB_PASSWORD")
    p.add_argument("--ca_env", default="TIDB_CA")

    p.add_argument("--only_done", action="store_true", help="仅使用 status='done' 的问卷 payload 构建题库索引")
    p.add_argument("--bank", default="", help="仅导出指定 bank（题库文件夹名），默认全部")
    p.add_argument("--out_name", default="bank_merged.jsonl", help="每个 bank 输出文件名")
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
        # 1) 读取问卷（用于拿到“题目原始 item 结构”）
        q_rows = fetch_all_questionnaires(conn, only_done=args.only_done)
        print(f"[load] questionnaires rows: {len(q_rows)} (only_done={args.only_done})")

        # bank -> {question_qid -> canonical_item_dict_with_choice_list}
        bank_items: Dict[str, Dict[str, dict]] = {}
        # questionnaire_id -> bank（用于把 submissions 分配回对应 bank）
        questionnaire_to_bank: Dict[str, str] = {}

        for qr in q_rows:
            questionnaire_id = str(qr["qid"])  # 这是问卷ID（rel_path）
            bank = str(qr.get("bank", ""))
            if args.bank and bank != args.bank:
                continue
            questionnaire_to_bank[questionnaire_id] = bank

            payload = parse_json(qr["payload"])
            questions = payload.get("questions", []) or []

            if bank not in bank_items:
                bank_items[bank] = {}

            for item in questions:
                # item 就是 Step1 的题库 item 风格（qid/task/method_a/.../raw/options...）
                question_qid = item.get("qid", None)
                if question_qid is None:
                    continue
                question_qid = str(question_qid)

                if question_qid not in bank_items[bank]:
                    canonical = dict(item)
                    canonical["choice"] = []  # ✅ 追加汇总字段
                    bank_items[bank][question_qid] = canonical
                else:
                    # 已存在：不覆盖 canonical，避免不同问卷中字段轻微差异导致抖动
                    pass

        if not bank_items:
            raise RuntimeError("No bank items built from questionnaires payload. Check --only_done/--bank filters.")

        # 2) 读取所有 submissions，把答案汇总到对应题目 item 的 choice 列表里
        subs = fetch_all_submissions(conn)
        print(f"[load] submissions rows: {len(subs)}")

        missed_questionnaires = 0
        appended = 0

        for s in subs:
            questionnaire_id = str(s["questionnaire_id"])
            bank = questionnaire_to_bank.get(questionnaire_id, None)
            if bank is None:
                # submissions 里有，但 questionnaires 里找不到（例如你用了 --only_done 导致过滤掉了）
                missed_questionnaires += 1
                continue
            if args.bank and bank != args.bank:
                continue

            sid = str(s.get("sid", ""))
            submitted_at = dt_to_str(s.get("submitted_at"))
            answers = parse_json(s.get("answers"))

            # answers: {question_qid: "A"/"B"/"C"}
            for qid_str, choice in answers.items():
                qid_str = str(qid_str)
                if qid_str not in bank_items.get(bank, {}):
                    # 理论上不会发生：除非 payload/questions 不全或题目缺失
                    continue
                bank_items[bank][qid_str]["choice"].append(
                    {
                        "sid": sid,
                        "submitted_at": submitted_at,
                        "choice": str(choice),
                    }
                )
                appended += 1

        print(f"[merge] appended choice records: {appended}")
        if missed_questionnaires:
            print(
                f"[warn] submissions referring to questionnaires not present in built index: {missed_questionnaires} "
                f"(可能是你加了 --only_done 或只导出了部分问卷 payload)"
            )

        # 3) 按 bank 输出 Step1 风格的题库 jsonl（每行一题，含 choice 列表）
        total_items = 0
        for bank, items_map in bank_items.items():
            # 输出顺序：优先 source.row_index，其次 qid
            items = list(items_map.values())
            items.sort(key=lambda it: (safe_get_row_index(it), str(it.get("qid", ""))))

            out_path = out_root / bank / args.out_name
            atomic_write_jsonl(out_path, items)
            total_items += len(items)
            print(f"[write] {bank}: {len(items)} items -> {out_path}")

        print(f"[OK] total banks: {len(bank_items)}")
        print(f"[OK] total unique items written: {total_items}")
        print(f"[OK] output_dir: {out_root}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
