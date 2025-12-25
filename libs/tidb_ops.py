# streamlit_annotator/libs/tidb_ops.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pymysql


@dataclass
class QuestionnaireRow:
    qid: str
    bank: str
    rel_path: str
    payload: Dict[str, Any]
    question_count: int
    status: str
    claimed_by: Optional[str]


class TiDBOps:
    """
    TiDB (MySQL 协议) 操作封装：
    - claim_one: 事务领取（行锁保证不冲突）
    - refresh_lock: 续租 TTL（防止做题中被回收）
    - abandon: 放弃问卷（改回 available）
    - submit: 提交答案（写 submissions + 标记 done）
    - reclaim_expired: 回收过期 in_progress
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        ca_path: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.ca_path = ca_path

    def connect(self):
        ssl = None
        if self.ca_path:
            ssl = {"ca": self.ca_path}

        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            ssl=ssl,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
        )

    @staticmethod
    def _parse_payload(payload_value) -> Dict[str, Any]:
        if isinstance(payload_value, dict):
            return payload_value
        if isinstance(payload_value, (bytes, bytearray)):
            payload_value = payload_value.decode("utf-8", errors="ignore")
        if isinstance(payload_value, str):
            return json.loads(payload_value)
        # fallback
        return json.loads(str(payload_value))

    def reclaim_expired(self, ttl_seconds: int) -> int:
        """
        回收已过期的 in_progress（lock_expires_at < NOW()）
        返回回收条数。
        """
        sql = """
        UPDATE questionnaires
        SET status='available',
            claimed_by=NULL,
            claimed_at=NULL,
            lock_expires_at=NULL
        WHERE status='in_progress'
          AND lock_expires_at IS NOT NULL
          AND lock_expires_at < NOW();
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                n = cur.rowcount
            conn.commit()
            return n
        finally:
            conn.close()

    def claim_one(self, sid: str, ttl_seconds: int, max_retries: int = 5) -> Optional[QuestionnaireRow]:
        """
        高并发安全领取一份问卷：
        - 先回收过期锁（不在事务里也行，这里简单起见每次领取前做一次）
        - 事务中 SELECT ... FOR UPDATE 锁定一条 available 记录
        - UPDATE 置为 in_progress + claimed_by + lock_expires_at
        """
        self.reclaim_expired(ttl_seconds)

        select_sql = """
        SELECT qid, bank, rel_path, payload, question_count, status, claimed_by
        FROM questionnaires
        WHERE status='available'
        ORDER BY RAND()
        LIMIT 1
        FOR UPDATE;
        """

        update_sql = """
        UPDATE questionnaires
        SET status='in_progress',
            claimed_by=%s,
            claimed_at=NOW(),
            lock_expires_at=DATE_ADD(NOW(), INTERVAL %s SECOND)
        WHERE qid=%s AND status='available';
        """

        for _ in range(max_retries):
            conn = self.connect()
            try:
                with conn.cursor() as cur:
                    conn.begin()
                    cur.execute(select_sql)
                    row = cur.fetchone()
                    if not row:
                        conn.rollback()
                        return None

                    qid = row["qid"]
                    cur.execute(update_sql, (sid, int(ttl_seconds), qid))
                    if cur.rowcount != 1:
                        conn.rollback()
                        continue

                    conn.commit()

                    payload = self._parse_payload(row["payload"])
                    return QuestionnaireRow(
                        qid=qid,
                        bank=row["bank"],
                        rel_path=row["rel_path"],
                        payload=payload,
                        question_count=int(row["question_count"]),
                        status="in_progress",
                        claimed_by=sid,
                    )
            finally:
                conn.close()

        return None

    def refresh_lock(self, qid: str, sid: str, ttl_seconds: int) -> bool:
        """
        做题过程中续租 TTL，避免中途被回收。
        只有当该问卷仍属于 sid（claimed_by=sid 且 status=in_progress）才会成功。
        """
        sql = """
        UPDATE questionnaires
        SET lock_expires_at=DATE_ADD(NOW(), INTERVAL %s SECOND)
        WHERE qid=%s AND status='in_progress' AND claimed_by=%s;
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (int(ttl_seconds), qid, sid))
                ok = (cur.rowcount == 1)
            conn.commit()
            return ok
        finally:
            conn.close()

    def abandon(self, qid: str, sid: str) -> bool:
        """
        放弃问卷：把 in_progress 且属于 sid 的问卷改回 available。
        """
        sql = """
        UPDATE questionnaires
        SET status='available',
            claimed_by=NULL,
            claimed_at=NULL,
            lock_expires_at=NULL
        WHERE qid=%s AND status='in_progress' AND claimed_by=%s;
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (qid, sid))
                ok = (cur.rowcount == 1)
            conn.commit()
            return ok
        finally:
            conn.close()

    def submit(self, qid: str, sid: str, answers: Dict[str, str]) -> bool:
        """
        提交：
        - 事务锁定该问卷行（FOR UPDATE）
        - 校验：必须 status=in_progress 且 claimed_by=sid
        - 写 submissions
        - 标记 questionnaires.status='done'
        """
        select_sql = """
        SELECT status, claimed_by
        FROM questionnaires
        WHERE qid=%s
        FOR UPDATE;
        """

        insert_sql = """
        INSERT INTO submissions (qid, sid, answers)
        VALUES (%s, %s, %s);
        """

        update_sql = """
        UPDATE questionnaires
        SET status='done',
            lock_expires_at=NULL
        WHERE qid=%s AND status='in_progress' AND claimed_by=%s;
        """

        conn = self.connect()
        try:
            with conn.cursor() as cur:
                conn.begin()
                cur.execute(select_sql, (qid,))
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    return False

                status = str(row["status"])
                claimed_by = row["claimed_by"]

                if status == "done":
                    # 已完成则认为提交成功（幂等）
                    conn.commit()
                    return True

                if status != "in_progress" or claimed_by != sid:
                    conn.rollback()
                    return False

                answers_json = json.dumps(answers, ensure_ascii=False)
                cur.execute(insert_sql, (qid, sid, answers_json))
                cur.execute(update_sql, (qid, sid))
                if cur.rowcount != 1:
                    conn.rollback()
                    return False

                conn.commit()
                return True
        finally:
            conn.close()
