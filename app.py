# streamlit_annotator/app.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

# =========================================================
# Config (from secrets.toml preferred; with sane defaults)
# =========================================================
# BANK_DIR: banks 根目录（会递归扫描 banks/**/questionnaire_*.jsonl）
BANK_DIR = Path(st.secrets.get("BANK_DIR", "streamlit_annotator/data/banks"))

# 锁超时（秒）：意外关页/断网后，锁多久自动回收
LOCK_TTL_SECONDS = int(st.secrets.get("LOCK_TTL_SECONDS", 2 * 60 * 60))  # 2h

# 每页显示多少题（防止一次渲染太多卡顿）
QUESTIONS_PER_PAGE = int(st.secrets.get("QUESTIONS_PER_PAGE", 20))


# =========================================================
# Utils: SID
# =========================================================
def validate_sid(sid: str) -> bool:
    return sid.isdigit() and len(sid) == 10


# =========================================================
# Utils: Questionnaire file IO (jsonl with optional meta header)
# =========================================================
def load_questionnaire_jsonl(path: Path) -> Tuple[dict, List[Dict[str, Any]]]:
    """
    读取问卷文件：
    - 如果第 1 行是 {"__meta__": {...}}，则视为 meta 行
    - 否则 meta={}
    返回 (meta, questions)
    """
    if not path.exists():
        return {}, []
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return {}, []

    meta: dict = {}
    start = 0
    first = lines[0].strip()
    try:
        obj0 = json.loads(first)
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


def is_done(meta: dict) -> bool:
    return str(meta.get("status", "")).lower() == "done"


def atomic_write_jsonl(path: Path, meta: dict, questions: List[Dict[str, Any]]) -> None:
    """
    原子写回：写到临时文件，再 replace 覆盖，避免写一半损坏。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp_{int(time.time() * 1000)}")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(json.dumps({"__meta__": meta}, ensure_ascii=False) + "\n")
        for q in questions:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")
    tmp.replace(path)


# =========================================================
# Utils: Lock file (atomic create) for high concurrency claim
# =========================================================
def lock_path_for(qfile: Path) -> Path:
    # lock 文件与问卷文件同目录同名后缀
    return qfile.with_suffix(qfile.suffix + ".lock")


def try_acquire_lock(lock_path: Path, sid: str) -> bool:
    """
    原子创建 lock 文件（成功=获得锁；失败=已被占用）
    lock 文件内容写入 sid 与 ts，便于 TTL 回收。
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
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
    更新锁时间戳，防止 TTL 回收（best-effort）
    """
    if not lock_path.exists():
        return
    try:
        lock_path.write_text(json.dumps({"sid": sid, "ts": time.time()}, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def is_lock_stale(lock_path: Path, ttl_seconds: int) -> bool:
    """
    判断锁是否过期（用于“意外关闭页面”后的回收）
    """
    info = read_lock(lock_path)
    if not info:
        return True
    ts = info.get("ts", 0)
    try:
        ts = float(ts)
    except Exception:
        return True
    return (time.time() - ts) > ttl_seconds


# =========================================================
# Utils: scan all questionnaires under banks root
# =========================================================
@st.cache_data(ttl=5)
def list_all_questionnaire_files(bank_root: str) -> List[str]:
    """
    递归扫描 banks 根目录下所有题库文件夹中的问卷文件。
    返回字符串路径列表（cache_data 需要可序列化）
    """
    root = Path(bank_root)
    files = sorted(root.rglob("questionnaire_*.jsonl"))
    return [str(p) for p in files]


def claim_random_questionnaire(bank_root: Path, sid: str) -> Optional[Tuple[Path, Path]]:
    """
    高并发安全领取问卷：
    - 递归扫描 banks 根目录下所有问卷
    - 跳过 done 的
    - 对过期锁自动回收
    - 尝试原子创建 lock，成功即领取
    """
    file_strs = list_all_questionnaire_files(str(bank_root))
    files = [Path(s) for s in file_strs]
    if not files:
        return None

    # 随机顺序尝试（避免热点）
    rng = random.Random(int(time.time() * 1000) ^ hash(sid))
    rng.shuffle(files)

    for qf in files:
        meta, _ = load_questionnaire_jsonl(qf)
        if is_done(meta):
            continue

        lp = lock_path_for(qf)
        if lp.exists() and is_lock_stale(lp, LOCK_TTL_SECONDS):
            release_lock(lp)

        if try_acquire_lock(lp, sid):
            return qf, lp

    return None


# =========================================================
# Dialog: show missing questions as a modal warning
# =========================================================
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
    # 兼容极老 Streamlit：没有 dialog，就用普通 warning 区域模拟（不会是模态）
    def missing_dialog(missing_numbers: List[int], first_missing_page: int):
        st.warning("还有题目未作答： " + ", ".join(map(str, missing_numbers[:200])))
        if st.button("跳转到第一题未做"):
            st.session_state["page"] = first_missing_page
            st.rerun()


# =========================================================
# Core actions: abandon / finish
# =========================================================
def abandon_current():
    """
    放弃当前问卷：释放锁 + 清理当前问卷相关状态
    """
    lp = st.session_state.get("lock_path")
    if lp:
        release_lock(Path(lp))

    for k in ["qfile_path", "lock_path", "meta", "questions", "answers", "page", "started_at"]:
        st.session_state.pop(k, None)


def finish_and_save(choice_map: Dict[str, str]):
    """
    提交完成：
    - 将答案和学号写回原问卷文件（meta + 每题 answer）
    - 释放锁
    """
    sid = st.session_state["sid"]
    qfile = Path(st.session_state["qfile_path"])
    lockfile = Path(st.session_state["lock_path"])

    meta, questions = load_questionnaire_jsonl(qfile)

    # 如果已经 done（极端情况），不覆盖
    if is_done(meta):
        release_lock(lockfile)
        return

    # 写入答案：每题记录 {"choice": "A"/"B"/"C"}
    for q in questions:
        qid = q.get("qid")
        q["answer"] = {"choice": choice_map.get(qid)}

    meta_out = dict(meta)
    meta_out.update({
        "status": "done",
        "filled_by_sid": sid,
        "filled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "question_count": len(questions),
        "bank_root": str(BANK_DIR),
        "bank_name": qfile.parent.name,
        "questionnaire_file": qfile.name,
    })

    atomic_write_jsonl(qfile, meta_out, questions)
    release_lock(lockfile)


# =========================================================
# Streamlit UI
# =========================================================
st.set_page_config(page_title="问卷标注", layout="wide")
st.title("问卷标注系统（Streamlit + JSON）")

# stage: login / doing / finished
if "stage" not in st.session_state:
    st.session_state["stage"] = "login"

stage = st.session_state["stage"]

# -------------------------
# LOGIN PAGE
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
                res = claim_random_questionnaire(BANK_DIR, sid)
                if not res:
                    st.warning("当前没有可领取的问卷（可能都已完成或都被占用）。稍后再试。")
                else:
                    qfile, lockfile = res
                    meta, questions = load_questionnaire_jsonl(qfile)

                    st.session_state["sid"] = sid
                    st.session_state["qfile_path"] = str(qfile)
                    st.session_state["lock_path"] = str(lockfile)
                    st.session_state["meta"] = meta
                    st.session_state["questions"] = questions
                    st.session_state["answers"] = {}  # qid -> "A"/"B"/"C"
                    st.session_state["page"] = 0
                    st.session_state["started_at"] = time.time()
                    st.session_state["stage"] = "doing"
                    st.rerun()

    with col2:
        st.info(
            f"问卷根目录（递归扫描）：{BANK_DIR}\n\n"
            f"- 高并发领取：通过 `.lock` 原子锁文件确保同一份问卷不会被重复领取\n"
            f"- 放弃/退出：立即释放锁，其他人可领取\n"
            f"- 意外关闭页面：锁在 {LOCK_TTL_SECONDS//60} 分钟后自动回收\n"
            f"- 每页显示题数：{QUESTIONS_PER_PAGE}"
        )

# -------------------------
# DOING PAGE
# -------------------------
elif stage == "doing":
    sid = st.session_state["sid"]
    qfile = Path(st.session_state["qfile_path"])
    lockfile = Path(st.session_state["lock_path"])
    questions = st.session_state["questions"]
    answers: Dict[str, str] = st.session_state["answers"]

    # keep lock alive
    refresh_lock(lockfile, sid)

    # 若上一轮触发缺题弹窗，则本轮打开一次（并清标记）
    if st.session_state.get("show_missing_dialog", False):
        missing_numbers = st.session_state.get("missing_numbers", [])
        first_missing_page = int(st.session_state.get("missing_first_page", 0))
        st.session_state["show_missing_dialog"] = False
        missing_dialog(missing_numbers, first_missing_page)

    st.subheader(f"正在填写：{qfile.parent.name} / {qfile.name}")
    st.caption(f"学号：{sid}")

    top1, top2, top3 = st.columns([1, 1, 2])
    with top1:
        if st.button("放弃并退出"):
            abandon_current()
            st.session_state["stage"] = "login"
            st.rerun()

    total = len(questions)
    per = QUESTIONS_PER_PAGE
    page = int(st.session_state.get("page", 0))
    page_count = (total + per - 1) // per
    page = max(0, min(page, page_count - 1))
    start = page * per
    end = min(total, start + per)

    answered = sum(1 for q in questions if q.get("qid") in answers)
    st.write(f"页进度：第 {page+1}/{page_count} 页（本页 {start+1}-{end} / 总 {total} 题）")
    st.write(f"总体进度：已完成 {answered}/{total} 题")
    st.progress(answered / total if total else 0.0)

    with st.form(key="qa_form"):
        # Render questions in current page
        for i in range(start, end):
            q = questions[i]
            qid = q.get("qid", f"q{i}")
            prompt = q.get("prompt", "")
            opts = q.get("options", [])

            st.markdown("---")
            st.markdown(f"### 题目 {i+1}")
            st.text(prompt)

            opt_map = {o.get("key"): o.get("text", "") for o in opts}
            keys = [k for k in ["A", "B", "C"] if k in opt_map]

            labels = [f"{k}：{opt_map.get(k,'')}" for k in keys]
            choices = ["（未选择）"] + labels

            cur = answers.get(qid)  # "A"/"B"/"C" or None
            cur_label = f"{cur}：{opt_map.get(cur,'')}" if cur in keys else "（未选择）"
            index = choices.index(cur_label) if cur_label in choices else 0

            sel = st.selectbox(
                "请选择：",
                options=choices,
                index=index,
                key=f"sel_{qid}",
            )

            # 将选择结果写入 answers（在 form submit 时生效）
            if sel == "（未选择）":
                answers.pop(qid, None)
            else:
                chosen_key = sel.split("：", 1)[0]
                answers[qid] = chosen_key

        st.markdown("---")
        # Bottom progress (page + total)
        answered_after = sum(1 for q in questions if q.get("qid") in answers)
        st.write(f"底部进度：已完成 {answered_after}/{total} 题")
        st.progress(answered_after / total if total else 0.0)

        # Buttons: show only when applicable
        c1, c2, c3, c4 = st.columns([1, 1, 2, 1])
        with c1:
            prev_clicked = st.form_submit_button("上一页") if page > 0 else False
        with c2:
            next_clicked = st.form_submit_button("下一页") if page < page_count - 1 else False
        with c4:
            submit_clicked = st.form_submit_button("提交整份问卷 ✅") if page == page_count - 1 else False

    # handle prev/next
    if prev_clicked:
        st.session_state["page"] = max(0, page - 1)
        st.rerun()

    if next_clicked:
        st.session_state["page"] = min(page_count - 1, page + 1)
        st.rerun()

    # handle submit (only last page has the button)
    if submit_clicked:
        missing_idx = []
        for i, q in enumerate(questions):
            qid = q.get("qid")
            if qid not in answers:
                missing_idx.append(i)

        if missing_idx:
            missing_numbers = [i + 1 for i in missing_idx]  # 1-based for display
            st.session_state["missing_numbers"] = missing_numbers
            st.session_state["missing_first_page"] = missing_idx[0] // per
            st.session_state["show_missing_dialog"] = True
            st.rerun()
        else:
            finish_and_save(answers)
            # 清理当前问卷状态，但保留 sid，方便继续领
            for k in ["qfile_path", "lock_path", "meta", "questions", "answers", "page", "started_at"]:
                st.session_state.pop(k, None)
            st.session_state["stage"] = "finished"
            st.rerun()

# -------------------------
# FINISHED PAGE
# -------------------------
elif stage == "finished":
    sid = st.session_state.get("sid", "")
    st.success("已提交完成！感谢参与。")

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("退出（返回首页）"):
            # 退出不清 sid，让用户回到首页时自动带出学号
            st.session_state["stage"] = "login"
            st.rerun()

    with col2:
        if st.button("再领取一份", type="primary"):
            if not sid or not validate_sid(sid):
                st.session_state["stage"] = "login"
                st.rerun()

            res = claim_random_questionnaire(BANK_DIR, sid)
            if not res:
                st.warning("当前没有可领取的问卷（可能都已完成或都被占用）。稍后再试。")
                st.rerun()

            qfile, lockfile = res
            meta, questions = load_questionnaire_jsonl(qfile)

            st.session_state["qfile_path"] = str(qfile)
            st.session_state["lock_path"] = str(lockfile)
            st.session_state["meta"] = meta
            st.session_state["questions"] = questions
            st.session_state["answers"] = {}
            st.session_state["page"] = 0
            st.session_state["started_at"] = time.time()
            st.session_state["stage"] = "doing"
            st.rerun()
