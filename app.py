# streamlit_annotator/app_tidb.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, List

import streamlit as st

from libs.tidb_ops import TiDBOps


# =========================
# Config
# =========================
def validate_sid(sid: str) -> bool:
    return sid.isdigit() and len(sid) == 10


@st.cache_resource
def get_db():
    # 统一从 st.secrets 读取（云端/本地都通用）
    host = st.secrets["TIDB_HOST"]
    port = int(st.secrets["TIDB_PORT"])
    user = st.secrets["TIDB_USER"]
    database = st.secrets["TIDB_DATABASE"]
    password = st.secrets["TIDB_PASSWORD"]

    ca_path = None
    ca_pem = st.secrets.get("TIDB_CA_PEM", "")
    if ca_pem.strip():
        ca_path = Path("/tmp/tidb_ca.pem")
        ca_path.write_text(ca_pem, encoding="utf-8")

    return TiDBOps(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        ca_path=str(ca_path) if ca_path else None,
    )


# --- dialog compatibility ---
DIALOG_DECORATOR = getattr(st, "dialog", None) or getattr(st, "experimental_dialog", None)

if DIALOG_DECORATOR:
    @DIALOG_DECORATOR("⚠️ 还有题目未作答")
    def missing_dialog(missing_numbers: List[int], first_missing_page: int):
        st.warning("请先完成以下题目再提交：")
        if len(missing_numbers) <= 200:
            st.write("未作答题号：", ", ".join(map(str, missing_numbers)))
        else:
            st.write("未作答题号（前200题）：", ", ".join(map(str, missing_numbers[:200])))
            st.caption(f"（共 {len(missing_numbers)} 题未作答，已省略后续题号）")

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("跳转到第一题未做"):
                st.session_state["page"] = first_missing_page
                st.rerun()
        with c2:
            if st.button("继续作答"):
                st.rerun()
else:
    def missing_dialog(missing_numbers: List[int], first_missing_page: int):
        st.warning("还有题目未作答： " + ", ".join(map(str, missing_numbers[:200])))
        if st.button("跳转到第一题未做"):
            st.session_state["page"] = first_missing_page
            st.rerun()


# =========================
# State helpers
# =========================
def clear_questionnaire_state(keep_sid: bool = True):
    sid = st.session_state.get("sid") if keep_sid else None
    for k in [
        "qid", "bank", "rel_path", "payload", "questions",
        "answers", "page", "started_at",
        "show_missing_dialog", "missing_numbers", "missing_first_page",
    ]:
        st.session_state.pop(k, None)
    if keep_sid and sid:
        st.session_state["sid"] = sid


def start_doing_from_row(row):
    payload = row.payload
    questions = payload.get("questions", [])
    st.session_state["qid"] = row.qid
    st.session_state["bank"] = row.bank
    st.session_state["rel_path"] = row.rel_path
    st.session_state["payload"] = payload
    st.session_state["questions"] = questions
    st.session_state["answers"] = {}  # qid -> "A"/"B"/"C"
    st.session_state["page"] = 0
    st.session_state["started_at"] = time.time()
    st.session_state["stage"] = "doing"


# =========================
# UI
# =========================
st.set_page_config(page_title="问卷标注", layout="wide")
st.title("问卷标注系统")

if "stage" not in st.session_state:
    st.session_state["stage"] = "login"

stage = st.session_state["stage"]
db = get_db()


# -------------------------
# LOGIN
# -------------------------
if stage == "login":
    st.subheader("首页：输入学号领取问卷")

    sid_default = st.session_state.get("sid", "")
    sid = st.text_input("学号（10位数字）", value=sid_default, max_chars=10)

    col1, col2 = st.columns([1, 2])
    with col1:
        if st.button("领取问卷", type="primary"):
            if not validate_sid(sid):
                st.error("学号格式不正确：必须是 10 位数字。")
            else:
                row = db.claim_one(sid=sid, ttl_seconds=LOCK_TTL_SECONDS)
                if not row:
                    st.warning("当前没有可领取的问卷（可能都已完成或都在进行中）。稍后再试。")
                else:
                    st.session_state["sid"] = sid
                    start_doing_from_row(row)
                    st.rerun()

    with col2:
        st.info(
            f"TiDB：{TIDB_HOST}:{TIDB_PORT} / {TIDB_DATABASE}\n\n"
            f"- 领取使用数据库事务行锁，保证高并发不重复领取\n"
            f"- TTL：{LOCK_TTL_SECONDS//60} 分钟未续租会自动回收\n"
            f"- 每页显示题数：{QUESTIONS_PER_PAGE}"
        )


# -------------------------
# DOING
# -------------------------
elif stage == "doing":
    sid = st.session_state.get("sid", "")
    qid = st.session_state.get("qid", "")
    questions = st.session_state.get("questions", [])
    answers: Dict[str, str] = st.session_state.get("answers", {})

    if not sid or not qid:
        st.error("会话状态异常：缺少 sid 或 qid。请重新领取问卷。")
        clear_questionnaire_state(keep_sid=True)
        st.session_state["stage"] = "login"
        st.rerun()

    # 续租 TTL：如果失败（可能已过期被回收），强制回到首页
    ok = db.refresh_lock(qid=qid, sid=sid, ttl_seconds=LOCK_TTL_SECONDS)
    if not ok:
        st.error("该问卷已过期或不再属于你（可能超时被系统回收）。请重新领取。")
        clear_questionnaire_state(keep_sid=True)
        st.session_state["stage"] = "login"
        st.rerun()

    # 缺题弹窗触发
    if st.session_state.get("show_missing_dialog", False):
        missing_numbers = st.session_state.get("missing_numbers", [])
        first_missing_page = int(st.session_state.get("missing_first_page", 0))
        st.session_state["show_missing_dialog"] = False
        missing_dialog(missing_numbers, first_missing_page)

    bank = st.session_state.get("bank", "")
    rel_path = st.session_state.get("rel_path", "")
    st.subheader(f"正在填写：{bank} / {Path(rel_path).name}")
    st.caption(f"学号：{sid}   |   问卷ID：{qid}")

    top1, top2 = st.columns([1, 3])
    with top1:
        if st.button("放弃并退出"):
            db.abandon(qid=qid, sid=sid)
            clear_questionnaire_state(keep_sid=True)
            st.session_state["stage"] = "login"
            st.rerun()

    total = len(questions)
    per = QUESTIONS_PER_PAGE
    page_count = max(1, (total + per - 1) // per)
    page = int(st.session_state.get("page", 0))
    page = max(0, min(page, page_count - 1))
    start = page * per
    end = min(total, start + per)

    answered = sum(1 for q in questions if q.get("qid") in answers)
    st.write(f"页进度：第 {page+1}/{page_count} 页（本页 {start+1}-{end} / 总 {total} 题）")
    st.write(f"总体进度：已完成 {answered}/{total} 题")
    st.progress(answered / total if total else 0.0)

    with st.form(key="qa_form"):
        for i in range(start, end):
            q = questions[i]
            q_qid = q.get("qid", f"q{i}")
            prompt = q.get("prompt", "")
            opts = q.get("options", [])

            st.markdown("---")
            st.markdown(f"### 题目 {i+1}")
            st.text(prompt)

            opt_map = {o.get("key"): o.get("text", "") for o in opts}
            keys = [k for k in ["A", "B", "C"] if k in opt_map]
            labels = [f"{k}：{opt_map.get(k,'')}" for k in keys]
            choices = ["（未选择）"] + labels

            cur = answers.get(q_qid)
            cur_label = f"{cur}：{opt_map.get(cur,'')}" if cur in keys else "（未选择）"
            index = choices.index(cur_label) if cur_label in choices else 0

            sel = st.selectbox(
                "请选择：",
                options=choices,
                index=index,
                key=f"sel_{q_qid}",
            )

            if sel == "（未选择）":
                answers.pop(q_qid, None)
            else:
                answers[q_qid] = sel.split("：", 1)[0]

        st.markdown("---")
        answered_after = sum(1 for q in questions if q.get("qid") in answers)
        st.write(f"底部进度：已完成 {answered_after}/{total} 题")
        st.progress(answered_after / total if total else 0.0)

        c1, c2, c3, c4 = st.columns([1, 1, 2, 1])
        with c1:
            prev_clicked = st.form_submit_button("上一页") if page > 0 else False
        with c2:
            next_clicked = st.form_submit_button("下一页") if page < page_count - 1 else False
        with c4:
            submit_clicked = st.form_submit_button("提交整份问卷 ✅") if page == page_count - 1 else False

    if prev_clicked:
        st.session_state["page"] = max(0, page - 1)
        st.rerun()

    if next_clicked:
        st.session_state["page"] = min(page_count - 1, page + 1)
        st.rerun()

    if submit_clicked:
        # 缺题检查：弹出对话框并列出未做题号
        missing_idx = []
        for i, q in enumerate(questions):
            q_qid = q.get("qid")
            if q_qid not in answers:
                missing_idx.append(i)

        if missing_idx:
            missing_numbers = [i + 1 for i in missing_idx]
            st.session_state["missing_numbers"] = missing_numbers
            st.session_state["missing_first_page"] = missing_idx[0] // per
            st.session_state["show_missing_dialog"] = True
            st.rerun()
        else:
            ok_submit = db.submit(qid=qid, sid=sid, answers=answers)
            if not ok_submit:
                st.error("提交失败：可能问卷已超时或不再属于你。请重新领取。")
                clear_questionnaire_state(keep_sid=True)
                st.session_state["stage"] = "login"
                st.rerun()

            # 提交成功：进入完成页（保留 sid）
            clear_questionnaire_state(keep_sid=True)
            st.session_state["stage"] = "finished"
            st.rerun()


# -------------------------
# FINISHED
# -------------------------
elif stage == "finished":
    sid = st.session_state.get("sid", "")
    st.success("已提交完成！感谢参与。")

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("退出（返回首页）"):
            st.session_state["stage"] = "login"
            st.rerun()

    with col2:
        if st.button("再领取一份", type="primary"):
            if not sid or not validate_sid(sid):
                st.session_state["stage"] = "login"
                st.rerun()

            row = db.claim_one(sid=sid, ttl_seconds=LOCK_TTL_SECONDS)
            if not row:
                st.warning("当前没有可领取的问卷（可能都已完成或都在进行中）。稍后再试。")
                st.rerun()

            start_doing_from_row(row)
            st.rerun()
