# streamlit_annotator/scripts/import_questionnaires_tidb.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pymysql


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

    meta: dict = {}
    start = 0
    try:
        obj0 = json.loads(lines[0].strip())
        if isinstance(obj0, dict) and "__meta__" in obj0:
            meta = obj0.get("__meta__", {}) or {}
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


def connect_tidb(host: str, port: int, user: str, password: str, database: str, ca_path: str | None):
    """
    TiDB Cloud: MySQL 协议 + TLS，建议配置 CA。
    """
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
        read_timeout=30,
        write_timeout=30,
    )


def ensure_schema(conn) -> None:
    """
    建表：1张问卷库存表 + 1张答卷表
    questionnaires.payload 用 JSON 存整份问卷内容（meta + questions）
    """
    ddl_questionnaires = """
    CREATE TABLE IF NOT EXISTS questionnaires (
      qid             VARCHAR(512) PRIMARY KEY,
      bank            VARCHAR(128) NOT NULL,
      rel_path        VARCHAR(512) NOT NULL,
      payload         JSON NOT NULL,
      question_count  INT NOT NULL,
      status          VARCHAR(16) NOT NULL DEFAULT 'available',
      claimed_by      VARCHAR(32) NULL,
      claimed_at      DATETIME NULL,
      lock_expires_at DATETIME NULL,
      created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      KEY idx_status (status),
      KEY idx_bank   (bank)
    );
    """

    ddl_submissions = """
    CREATE TABLE IF NOT EXISTS submissions (
      id           BIGINT PRIMARY KEY AUTO_INCREMENT,
      qid          VARCHAR(512) NOT NULL,
      sid          VARCHAR(32) NOT NULL,
      answers      JSON NOT NULL,
      submitted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      KEY idx_qid (qid),
      KEY idx_sid (sid)
    );
    """

    with conn.cursor() as cur:
        cur.execute(ddl_questionnaires)
        cur.execute(ddl_submissions)
    conn.commit()


def reset_data(conn) -> None:
    """
    清空已有数据（重新导入用）
    - TRUNCATE 会清空表并重置 AUTO_INCREMENT
    """
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE submissions;")
        cur.execute("TRUNCATE TABLE questionnaires;")
    conn.commit()


def upsert_questionnaire(conn, qid: str, bank: str, rel_path: str, payload_obj: dict, question_count: int) -> None:
    payload_json = json.dumps(payload_obj, ensure_ascii=False)

    # 默认：重复导入时不会覆盖 status/claimed 字段，避免把进行中/已完成状态冲掉
    sql = """
    INSERT INTO questionnaires (qid, bank, rel_path, payload, question_count)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      bank = VALUES(bank),
      rel_path = VALUES(rel_path),
      payload = VALUES(payload),
      question_count = VALUES(question_count),
      updated_at = CURRENT_TIMESTAMP;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (qid, bank, rel_path, payload_json, question_count))


def main():
    p = argparse.ArgumentParser("Import questionnaires under banks root into TiDB (TiDB Cloud).")
    p.add_argument("--banks_dir", required=True, help="banks 根目录（递归扫描 banks/**/questionnaire_*.jsonl）")
    p.add_argument("--host", default="gateway01.eu-central-1.prod.aws.tidbcloud.com")
    p.add_argument("--port", type=int, default=4000)
    p.add_argument("--user", default="2b1cKMtrfhzxjYj.root")
    p.add_argument("--database", default="github_sample")
    p.add_argument("--password_env", default="TIDB_PASSWORD", help="密码环境变量名（默认 TIDB_PASSWORD）")
    p.add_argument("--ca_env", default="TIDB_CA", help="CA 路径环境变量名（默认 TIDB_CA，可为空）")

    p.add_argument("--limit", type=int, default=0, help="只导入前 limit 份问卷（0 表示不限制）")
    p.add_argument("--dry_run", action="store_true", help="只扫描统计，不写入数据库")
    p.add_argument("--batch", type=int, default=200, help="每 batch 提交一次事务")

    # ✅ 新增：是否清空已有数据后重新导入
    p.add_argument("--reset", action="store_true", help="清空 questionnaires/submissions 后重新导入（危险操作）")

    args = p.parse_args()

    banks_dir = Path(args.banks_dir).resolve()
    if not banks_dir.exists():
        raise FileNotFoundError(f"banks_dir not found: {banks_dir}")

    password = os.environ.get(args.password_env, "")
    if not password:
        raise RuntimeError(f"Missing password env: {args.password_env}")

    ca_path = os.environ.get(args.ca_env, "") or None
    if ca_path:
        ca_path = str(Path(ca_path).resolve())

    qfiles = sorted(banks_dir.rglob("questionnaire_*.jsonl"))
    if args.limit and args.limit > 0:
        qfiles = qfiles[: args.limit]

    print(f"[scan] banks_dir={banks_dir}")
    print(f"[scan] found questionnaires: {len(qfiles)}")

    if args.dry_run:
        for qf in qfiles[:10]:
            rel = qf.relative_to(banks_dir).as_posix()
            bank = rel.split("/", 1)[0]
            print(f"  - {bank} / {rel}")
        print("[dry_run] done.")
        return

    conn = connect_tidb(args.host, args.port, args.user, password, args.database, ca_path)
    try:
        ensure_schema(conn)

        if args.reset:
            print("[reset] TRUNCATE submissions + questionnaires ...")
            reset_data(conn)
            print("[reset] done.")

        imported = 0
        for qf in qfiles:
            rel_path = qf.relative_to(banks_dir).as_posix()
            bank = rel_path.split("/", 1)[0]
            qid = rel_path  # 用相对路径作为全局唯一ID（简单且可溯源）

            meta, questions = load_questionnaire_jsonl(qf)
            payload = {
                "meta": meta,
                "questions": questions,
                "source": {"bank": bank, "rel_path": rel_path},
            }

            upsert_questionnaire(
                conn,
                qid=qid,
                bank=bank,
                rel_path=rel_path,
                payload_obj=payload,
                question_count=len(questions),
            )
            imported += 1

            if imported % args.batch == 0:
                conn.commit()
                print(f"[import] committed {imported}/{len(qfiles)}")

        conn.commit()
        print(f"[OK] imported questionnaires: {imported}")

        # 简单校验
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM questionnaires;")
            row = cur.fetchone()
        print(f"[OK] questionnaires table total rows: {row['cnt']}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
