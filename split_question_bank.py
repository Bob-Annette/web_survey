# streamlit_annotator/scripts/split_question_bank.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import random
from pathlib import Path
from typing import Any, Dict, List


def load_json_or_jsonl(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text[0] == "[":
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError(f"{path} is JSON but not a list.")
        return [x for x in data if isinstance(x, dict)]
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return [x for x in rows if isinstance(x, dict)]


def write_jsonl(path: Path, items: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for obj in items:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def stable_hash(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def split_bank(
    bank: List[Dict[str, Any]],
    m: int,
    n: int,
    seed: int,
    max_attempts: int = 30,
) -> List[List[Dict[str, Any]]]:
    """
    将总题库拆分成 m 份，每题全局重复 n 次，每份题数严格相等 = N*n/m
    且每份问卷内同一 qid 不重复。

    算法：多次尝试（不同随机种子偏移），每次用“容量约束 + 最少负载优先”的贪心分配。
    """
    if m <= 0 or n <= 0:
        raise ValueError("m and n must be positive integers.")
    if n > m:
        raise ValueError(f"Invalid: n={n} > m={m}. This would force duplicates within a questionnaire.")

    N = len(bank)
    if N == 0:
        raise ValueError("Empty bank.")
    total = N * n
    if total % m != 0:
        raise ValueError(f"Invalid: N*n={total} not divisible by m={m}. Please adjust m/n.")
    target_len = total // m

    # basic qid validation
    for i, it in enumerate(bank):
        if "qid" not in it:
            raise ValueError(f"Bank item #{i} missing 'qid'. Please build Step1 with qid field.")
        if not isinstance(it["qid"], str) or not it["qid"]:
            raise ValueError(f"Bank item #{i} has invalid 'qid'.")

    # For reproducibility: use deterministic order of items, then shuffle per attempt
    base_items = list(bank)

    for attempt in range(max_attempts):
        rng = random.Random(seed + attempt * 10007)

        items = base_items[:]
        rng.shuffle(items)

        buckets: List[List[Dict[str, Any]]] = [[] for _ in range(m)]
        bucket_qids = [set() for _ in range(m)]
        counts = [0] * m

        ok = True

        # For each question, select n distinct questionnaires with remaining capacity
        # Prefer buckets with smaller counts to keep exact balance.
        for it in items:
            qid = it["qid"]

            # candidate indices where:
            # - not already have this qid (always true here if we assign at most once per bucket)
            # - not full
            candidates = [i for i in range(m) if counts[i] < target_len and qid not in bucket_qids[i]]
            if len(candidates) < n:
                ok = False
                break

            # sort by current load, tie-break random
            candidates.sort(key=lambda i: (counts[i], rng.random()))

            chosen = candidates[:n]
            for bi in chosen:
                # add a per-assignment unique id for traceability
                assigned = copy.deepcopy(it)
                assigned["instance_id"] = f"{qid}#{attempt}-{bi}-{counts[bi]}"
                buckets[bi].append(assigned)
                bucket_qids[bi].add(qid)
                counts[bi] += 1

        if not ok:
            continue

        # verify exact sizes and no duplicates
        if any(c != target_len for c in counts):
            continue
        for bi in range(m):
            qids = [x["qid"] for x in buckets[bi]]
            if len(qids) != len(set(qids)):
                ok = False
                break
        if not ok:
            continue

        # shuffle within each questionnaire
        for bi in range(m):
            rng_b = random.Random(seed + attempt * 10007 + bi * 97)
            rng_b.shuffle(buckets[bi])

        # ensure no identical questionnaires (same qid order)
        # extremely unlikely, but enforce by reshuffling colliding ones.
        seen = {}
        for bi in range(m):
            sig = stable_hash("|".join([x["qid"] for x in buckets[bi]]))
            if sig in seen:
                # collision, reshuffle a few times
                collision_ok = False
                for t in range(20):
                    rng_b = random.Random(seed + attempt * 10007 + bi * 97 + (t + 1) * 99991)
                    rng_b.shuffle(buckets[bi])
                    sig2 = stable_hash("|".join([x["qid"] for x in buckets[bi]]))
                    if sig2 not in seen:
                        seen[sig2] = bi
                        collision_ok = True
                        break
                if not collision_ok:
                    ok = False
                    break
            else:
                seen[sig] = bi

        if ok:
            return buckets

    raise RuntimeError(
        f"Failed to split bank after {max_attempts} attempts. "
        "Try changing --seed or adjusting m/n."
    )


def main():
    p = argparse.ArgumentParser(description="Step2: split question bank into m questionnaires with n repeats globally.")
    p.add_argument("--bank", required=True, type=str, help="Input bank file (jsonl or json).")
    p.add_argument("--m", required=True, type=int, help="Number of questionnaires (parts).")
    p.add_argument("--n", required=True, type=int, help="Global repeats per question (must be <= m).")
    p.add_argument("--seed", default=42, type=int, help="Random seed.")
    p.add_argument("--prefix", default="questionnaire", type=str, help="Output file prefix name.")
    p.add_argument("--max-attempts", default=30, type=int, help="Max attempts to find a valid split.")
    args = p.parse_args()

    bank_path = Path(args.bank)
    bank = load_json_or_jsonl(bank_path)
    N = len(bank)

    buckets = split_bank(bank, m=args.m, n=args.n, seed=args.seed, max_attempts=args.max_attempts)

    # output directory: same name as bank stem
    out_dir = bank_path.parent.parent / "questions" / bank_path.stem
    out_dir.mkdir(parents=True, exist_ok=True)

    # write m questionnaires
    for i, items in enumerate(buckets, start=1):
        out_path = out_dir / f"{args.prefix}_{i:03d}.jsonl"
        write_jsonl(out_path, items)

    total = N * args.n
    per = total // args.m
    print(f"[OK] Bank N={N}, repeats n={args.n}, parts m={args.m}")
    print(f"[OK] Total assignments={total}, per questionnaire={per}")
    print(f"[OK] Output dir: {out_dir}")
    print(f"[OK] Example: {args.prefix}_001.jsonl ... {args.prefix}_{args.m:03d}.jsonl")


if __name__ == "__main__":
    main()
