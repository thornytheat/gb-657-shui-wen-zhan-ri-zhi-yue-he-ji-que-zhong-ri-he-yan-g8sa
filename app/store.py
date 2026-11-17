"""SQLite 存储：批次、表计契约、读数、区间、月度分摊、账单、隔离、阶段、合计、清单。

约定：
- 用量与金额一律 TEXT（Decimal 字符串），不落 float；
- 一次运行的所有行在同一个事务里「先删后插」，失败整体回滚，不留半批数据；
- 页面永远只展示一个批次，按 batch_id 过滤。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    batch_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    sample TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    input_digest TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    note TEXT NOT NULL DEFAULT '',
    updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_key ON batches(batch_key, status);
CREATE TABLE IF NOT EXISTS meters (
    batch_id TEXT NOT NULL, meter_id TEXT NOT NULL, name TEXT NOT NULL,
    capacity TEXT NOT NULL, multiplier TEXT NOT NULL, unit TEXT NOT NULL,
    registers TEXT NOT NULL, replaced_at TEXT, replaced_by TEXT, tariff TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS readings (
    batch_id TEXT NOT NULL, lineno INTEGER NOT NULL, segment INTEGER NOT NULL,
    meter_id TEXT NOT NULL, register TEXT NOT NULL, read_at TEXT NOT NULL,
    reading_text TEXT NOT NULL, unit_raw TEXT NOT NULL, reading_norm TEXT,
    unit_norm TEXT, read_type TEXT NOT NULL, status TEXT NOT NULL,
    code TEXT, flags_json TEXT NOT NULL DEFAULT '[]', note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS intervals (
    batch_id TEXT NOT NULL, interval_id TEXT NOT NULL, meter_id TEXT NOT NULL,
    register TEXT NOT NULL, start_at TEXT NOT NULL, end_at TEXT NOT NULL,
    prev_reading TEXT NOT NULL, cur_reading TEXT NOT NULL, delta TEXT NOT NULL,
    multiplier TEXT NOT NULL, unit TEXT NOT NULL, usage_kwh TEXT NOT NULL,
    days TEXT NOT NULL, method TEXT NOT NULL, flags_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS allocations (
    batch_id TEXT NOT NULL, month TEXT NOT NULL, meter_id TEXT NOT NULL,
    register TEXT NOT NULL, usage_kwh TEXT NOT NULL, exact_share TEXT NOT NULL,
    tou_adjusted INTEGER NOT NULL DEFAULT 0, sources_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS bills (
    batch_id TEXT NOT NULL, month TEXT NOT NULL, meter_id TEXT NOT NULL,
    usage_kwh TEXT NOT NULL, amount_yuan TEXT NOT NULL,
    components_json TEXT NOT NULL, adjust_cents INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS quarantine (
    batch_id TEXT NOT NULL, lineno INTEGER NOT NULL, segment INTEGER NOT NULL,
    code TEXT NOT NULL, stage TEXT NOT NULL, level TEXT NOT NULL,
    field TEXT, raw_value TEXT, meter_id TEXT, detail TEXT
);
-- 同键冲突候选快照：原隔离结论仍在 quarantine 里原样保留，这里只存复核所需候选。
CREATE TABLE IF NOT EXISTS conflict_candidates (
    batch_id TEXT NOT NULL, group_key TEXT NOT NULL, lineno INTEGER NOT NULL,
    segment INTEGER NOT NULL, meter_id TEXT NOT NULL, register TEXT NOT NULL,
    read_at TEXT NOT NULL, reading_text TEXT NOT NULL, reading_norm TEXT NOT NULL,
    unit_raw TEXT NOT NULL, unit_norm TEXT NOT NULL, read_type TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT '',
    flags_json TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (batch_id, group_key, lineno)
);
-- 裁决记录只追加不改写：旧裁决降为 superseded / invalidated，保留证据链。
CREATE TABLE IF NOT EXISTS conflict_decisions (
    decision_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL, group_key TEXT NOT NULL,
    meter_id TEXT NOT NULL, register TEXT NOT NULL, read_at TEXT NOT NULL,
    selected_lineno INTEGER NOT NULL, selected_value TEXT NOT NULL,
    reason TEXT NOT NULL, status TEXT NOT NULL, revision INTEGER NOT NULL,
    input_digest TEXT NOT NULL, rule_version TEXT NOT NULL,
    evidence_digest TEXT NOT NULL,
    usage_before_kwh TEXT, usage_after_kwh TEXT,
    affected_json TEXT NOT NULL DEFAULT '[]',
    invalid_reason TEXT NOT NULL DEFAULT '',
    decided_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_decision_active
    ON conflict_decisions(batch_id, group_key) WHERE status='active';
CREATE INDEX IF NOT EXISTS idx_decision_batch ON conflict_decisions(batch_id, status);
-- 复核运行状态：baseline 是首次复核前的用量，reviewed 是最近一次成功重建的用量。
CREATE TABLE IF NOT EXISTS review_runs (
    batch_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    baseline_usage_kwh TEXT,
    reviewed_usage_kwh TEXT,
    baseline_interval_count INTEGER,
    reviewed_interval_count INTEGER,
    recovered_count INTEGER NOT NULL DEFAULT 0,
    original_quarantine_count INTEGER,
    reviewed_quarantine_count INTEGER,
    evidence_digest TEXT,
    month_usage_json TEXT NOT NULL DEFAULT '{}',
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS review_intervals (
    batch_id TEXT NOT NULL, interval_id TEXT NOT NULL, meter_id TEXT NOT NULL,
    register TEXT NOT NULL, start_at TEXT NOT NULL, end_at TEXT NOT NULL,
    prev_reading TEXT NOT NULL, cur_reading TEXT NOT NULL, delta TEXT NOT NULL,
    multiplier TEXT NOT NULL, unit TEXT NOT NULL, usage_kwh TEXT NOT NULL,
    days TEXT NOT NULL, method TEXT NOT NULL, flags_json TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_review_intervals_batch ON review_intervals(batch_id);
CREATE TABLE IF NOT EXISTS stages (
    batch_id TEXT NOT NULL, stage TEXT NOT NULL, in_count INTEGER NOT NULL,
    out_count INTEGER NOT NULL, quarantine_count INTEGER NOT NULL,
    status TEXT NOT NULL, elapsed_ms INTEGER NOT NULL, note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS totals (
    batch_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    batch_id TEXT PRIMARY KEY, json TEXT NOT NULL
);
"""


def _dec(text):
    return None if text is None else Decimal(str(text))


class StaleDecisionError(Exception):
    """并发裁决：提交时带的版本号不是当前版本，必须在事务内拒绝。"""

    def __init__(self, current_revision: int):
        super().__init__(f"当前裁决版本为 {current_revision}")
        self.current_revision = current_revision


class Store:
    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).resolve().parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            columns = {row["name"] for row in
                       conn.execute("PRAGMA table_info(batches)").fetchall()}
            if "updated_at" not in columns:
                conn.execute("ALTER TABLE batches ADD COLUMN updated_at TEXT")
            review_columns = {row["name"] for row in
                              conn.execute("PRAGMA table_info(review_runs)").fetchall()}
            if review_columns and "month_usage_json" not in review_columns:
                conn.execute("ALTER TABLE review_runs ADD COLUMN month_usage_json "
                             "TEXT NOT NULL DEFAULT '{}'")

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------- 批次 ----------
    def get_batch(self, batch_id: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM batches WHERE batch_id=?",
                               (batch_id,)).fetchone()
        return dict(row) if row else None

    def find_done_by_key(self, batch_key: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_key=? AND status='done' "
                "ORDER BY created_at LIMIT 1", (batch_key,)).fetchone()
        return dict(row) if row else None

    def find_by_key(self, batch_key: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_key=? ORDER BY created_at DESC LIMIT 1",
                (batch_key,)).fetchone()
        return dict(row) if row else None

    def latest_batch(self, status: str = "done") -> dict | None:
        with self.connect() as conn:
            order = "COALESCE(updated_at, created_at) DESC, rowid DESC"
            row = conn.execute(
                f"SELECT * FROM batches WHERE status=? ORDER BY {order} LIMIT 1",
                (status,)).fetchone()
            if row is None:
                row = conn.execute(
                    f"SELECT * FROM batches ORDER BY {order} LIMIT 1").fetchone()
        return dict(row) if row else None

    def list_batches(self, limit: int = 30) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM batches ORDER BY COALESCE(updated_at, created_at) DESC, "
                "rowid DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def save_attempt(self, *, batch: dict, stages: list[dict], note: str = "") -> None:
        """失败尝试：只写批次与阶段，不动已有批次的数据。"""
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO batches (batch_id,batch_key,run_id,sample,status,created_at,
                        finished_at,input_digest,rule_version,params_json,note,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     note=excluded.note, run_id=excluded.run_id,
                     updated_at=excluded.updated_at""",
                (batch["batch_id"], batch["batch_key"], batch["run_id"], batch["sample"],
                 batch["status"], batch["created_at"], batch.get("finished_at"),
                 batch["input_digest"], batch["rule_version"],
                 json.dumps(batch.get("params", {}), ensure_ascii=False),
                 note or batch.get("note", ""), batch.get("updated_at")))
            conn.execute("DELETE FROM stages WHERE batch_id=?", (batch["batch_id"],))
            for s in stages:
                conn.execute(
                    """INSERT INTO stages (batch_id,stage,in_count,out_count,
                        quarantine_count,status,elapsed_ms,note) VALUES (?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], s["stage"], s["in_count"], s["out_count"],
                     s["quarantine_count"], s["status"], s["elapsed_ms"],
                     s.get("note", "")))

    def touch_batch(self, batch_id: str, stamp: str) -> None:
        """命中已有批次时刷新「最近一次运行」，页面才会停在这次看的结果上。"""
        with self.connect() as conn:
            conn.execute("UPDATE batches SET updated_at=? WHERE batch_id=?",
                         (stamp, batch_id))

    # ---------- 一次运行整体落库 ----------
    def save_run(self, *, batch: dict, meters: list[dict], readings: list[dict],
                 intervals: list[dict], allocations: list[dict], bills: list[dict],
                 quarantine: list[dict], stages: list[dict], totals: dict,
                 manifest_json: dict, conflict_groups: list[dict] | None = None,
                 stamp: str | None = None) -> None:
        """一次运行整体落库。

        conflict_groups 是复核候选快照（每组含 group_key、evidence_digest、
        candidates）。人工裁决不被重跑抹掉：源数据未变则原样保留，只有源摘要
        或该组证据摘要变化时才降为 invalidated，理由与旧记录都留着。
        """
        b = batch["batch_id"]
        with self.connect() as conn:
            previous = conn.execute(
                "SELECT input_digest FROM batches WHERE batch_id=?", (b,)).fetchone()
            previous_digest = previous["input_digest"] if previous else None
            old_decisions = {
                row["group_key"]: row["evidence_digest"]
                for row in conn.execute(
                    "SELECT group_key, evidence_digest FROM conflict_decisions "
                    "WHERE batch_id=? AND status='active'", (b,)).fetchall()}
            previous_review = conn.execute(
                "SELECT * FROM review_runs WHERE batch_id=?", (b,)).fetchone()
            for table in ("meters", "readings", "intervals", "allocations", "bills",
                          "quarantine", "stages", "totals"):
                conn.execute(f"DELETE FROM {table} WHERE batch_id=?", (b,))
            for m in meters:
                conn.execute(
                    """INSERT INTO meters (batch_id,meter_id,name,capacity,multiplier,unit,
                        registers,replaced_at,replaced_by,tariff)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (b, m["meter_id"], m["name"], str(m["capacity"]),
                     str(m["multiplier"]), m["unit"], ";".join(m["registers"]),
                     m.get("replaced_at"), m.get("replaced_by"), m["tariff"]))
            for r in readings:
                conn.execute(
                    """INSERT INTO readings (batch_id,lineno,segment,meter_id,register,
                        read_at,reading_text,unit_raw,reading_norm,unit_norm,read_type,
                        status,code,flags_json,note)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (b, r["lineno"], r["segment"], r["meter_id"], r["register"],
                     r["read_at"], r["reading_text"], r["unit_raw"],
                     None if r.get("reading_norm") is None else str(r["reading_norm"]),
                     r.get("unit_norm"), r["read_type"], r["status"], r.get("code"),
                     json.dumps(r.get("flags", []), ensure_ascii=False),
                     r.get("note", "")))
            for i in intervals:
                conn.execute(
                    """INSERT INTO intervals (batch_id,interval_id,meter_id,register,
                        start_at,end_at,prev_reading,cur_reading,delta,multiplier,unit,
                        usage_kwh,days,method,flags_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (b, i["interval_id"], i["meter_id"], i["register"], i["start_at"],
                     i["end_at"], str(i["prev_reading"]), str(i["cur_reading"]),
                     str(i["delta"]), str(i["multiplier"]), i["unit"],
                     str(i["usage_kwh"]), str(i["days"]), i["method"],
                     json.dumps(i.get("flags", []), ensure_ascii=False)))
            for a in allocations:
                conn.execute(
                    """INSERT INTO allocations (batch_id,month,meter_id,register,usage_kwh,
                        exact_share,tou_adjusted,sources_json)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (b, a["month"], a["meter_id"], a["register"], str(a["usage_kwh"]),
                     str(a["exact_share"]), 1 if a.get("tou_adjusted") else 0,
                     json.dumps(a.get("sources", []), ensure_ascii=False)))
            for bill in bills:
                conn.execute(
                    """INSERT INTO bills (batch_id,month,meter_id,usage_kwh,amount_yuan,
                        components_json,adjust_cents) VALUES (?,?,?,?,?,?,?)""",
                    (b, bill["month"], bill["meter_id"], str(bill["usage_kwh"]),
                     str(bill["amount_yuan"]),
                     json.dumps(bill["components"], ensure_ascii=False),
                     int(bill.get("adjust_cents", 0))))
            for q in quarantine:
                conn.execute(
                    """INSERT INTO quarantine (batch_id,lineno,segment,code,stage,level,
                        field,raw_value,meter_id,detail) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (b, q["lineno"], q.get("segment", 1), q["code"], q["stage"],
                     q["level"], q.get("field"), q.get("raw_value"),
                     q.get("meter_id"), q.get("detail")))
            for s in stages:
                conn.execute(
                    """INSERT INTO stages (batch_id,stage,in_count,out_count,
                        quarantine_count,status,elapsed_ms,note) VALUES (?,?,?,?,?,?,?,?)""",
                    (b, s["stage"], s["in_count"], s["out_count"],
                     s["quarantine_count"], s["status"], s["elapsed_ms"],
                     s.get("note", "")))
            for key, value in totals.items():
                conn.execute("INSERT INTO totals (batch_id,key,value) VALUES (?,?,?)",
                             (b, key, str(value)))
            self._write_conflict_groups(conn, b, conflict_groups or [],
                                        previous_digest, batch["input_digest"],
                                        old_decisions, previous_review, totals, stamp)
            conn.execute(
                "INSERT INTO manifests (batch_id,json) VALUES (?,?) "
                "ON CONFLICT(batch_id) DO UPDATE SET json=excluded.json",
                (b, json.dumps(manifest_json, ensure_ascii=False)))
            conn.execute(
                """INSERT INTO batches (batch_id,batch_key,run_id,sample,status,created_at,
                        finished_at,input_digest,rule_version,params_json,note,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     run_id=excluded.run_id, note=excluded.note,
                     updated_at=excluded.updated_at""",
                (b, batch["batch_key"], batch["run_id"], batch["sample"],
                 batch["status"], batch["created_at"], batch.get("finished_at"),
                 batch["input_digest"], batch["rule_version"],
                 json.dumps(batch.get("params", {}), ensure_ascii=False),
                 batch.get("note", ""), batch.get("updated_at")))

    @staticmethod
    def _write_conflict_groups(conn, batch_id: str, groups: list[dict],
                               previous_digest: str | None, new_digest: str,
                               old_decisions: dict[str, str],
                               previous_review, totals: dict,
                               stamp: str | None) -> None:
        """写入候选快照并按证据摘要处理旧裁决与复核基线。"""
        stamp = stamp or ""
        conn.execute("DELETE FROM conflict_candidates WHERE batch_id=?", (batch_id,))
        group_digests: dict[str, str] = {}
        for group in groups:
            group_key = group["group_key"]
            group_digests[group_key] = group["evidence_digest"]
            for r in group["candidates"]:
                conn.execute(
                    """INSERT INTO conflict_candidates (batch_id,group_key,lineno,
                        segment,meter_id,register,read_at,reading_text,reading_norm,
                        unit_raw,unit_norm,read_type,note,detail,flags_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, group_key, r["lineno"], r.get("segment", 1),
                     r["meter_id"], r["register"], r["read_at"], r["reading_text"],
                     str(r["reading_norm"]), r["unit_raw"], r["unit_norm"],
                     r["read_type"], r.get("note", ""), r.get("detail", ""),
                     json.dumps(r.get("flags", []), ensure_ascii=False)))

        source_changed = (previous_digest is not None
                          and previous_digest != new_digest)
        invalidated = False
        for group_key, digest in old_decisions.items():
            if source_changed or group_digests.get(group_key) != digest:
                conn.execute(
                    "UPDATE conflict_decisions SET status='invalidated', "
                    "invalid_reason=?, updated_at=? WHERE batch_id=? AND group_key=? "
                    "AND status='active'",
                    ("E_EVIDENCE_CHANGED", stamp, batch_id, group_key))
                invalidated = True
        if source_changed:
            conn.execute("DELETE FROM review_intervals WHERE batch_id=?", (batch_id,))

        # 新一次运行重置复核基线；已成功重建的用量与证据留到下一次重建再覆盖。
        attempts = int(previous_review["attempts"]) if previous_review else 0
        reviewed_usage = previous_review["reviewed_usage_kwh"] if previous_review else None
        reviewed_count = (previous_review["reviewed_interval_count"]
                          if previous_review else None)
        evidence_digest = (previous_review["evidence_digest"]
                           if previous_review and not source_changed else None)
        last_error = ("" if previous_review is None or source_changed
                      else previous_review["last_error"])
        reviewed_quarantine = (previous_review["reviewed_quarantine_count"]
                               if previous_review else None)
        previous_status = previous_review["status"] if previous_review else "idle"
        if invalidated:
            status = "needs_review"
        elif (not source_changed and reviewed_usage is not None
              and previous_status == "ready"):
            # 同输入强制重试：原复核区间没被删，上次成功重建的状态可以沿用。
            status = "ready"
        else:
            status = "idle"
        conn.execute(
            """INSERT INTO review_runs (batch_id,status,attempts,baseline_usage_kwh,
                reviewed_usage_kwh,baseline_interval_count,reviewed_interval_count,
                recovered_count,original_quarantine_count,reviewed_quarantine_count,
                evidence_digest,last_error,updated_at)
               VALUES (?,?,?,?,?,?,?,0,?,?,?,?,?)
               ON CONFLICT(batch_id) DO UPDATE SET
                 status=excluded.status, attempts=excluded.attempts,
                 baseline_usage_kwh=excluded.baseline_usage_kwh,
                 reviewed_usage_kwh=excluded.reviewed_usage_kwh,
                 baseline_interval_count=excluded.baseline_interval_count,
                 reviewed_interval_count=excluded.reviewed_interval_count,
                 recovered_count=0,
                 original_quarantine_count=excluded.original_quarantine_count,
                 reviewed_quarantine_count=excluded.reviewed_quarantine_count,
                 evidence_digest=excluded.evidence_digest,
                 last_error=excluded.last_error, updated_at=excluded.updated_at""",
            (batch_id, status, attempts,
             str(totals.get("interval_usage", "")), reviewed_usage,
             int(totals.get("interval_count") or 0), reviewed_count,
             totals.get("quarantine"), reviewed_quarantine, evidence_digest,
             last_error, stamp))

    # ---------- 读取 ----------
    def load_meters(self, batch_id: str) -> list[dict]:
        rows = self._all("SELECT * FROM meters WHERE batch_id=? ORDER BY rowid", (batch_id,))
        for r in rows:
            r["capacity"] = _dec(r["capacity"])
            r["multiplier"] = _dec(r["multiplier"])
            r["registers"] = r["registers"].split(";")
        return rows

    def load_readings(self, batch_id: str, status: str | None = None) -> list[dict]:
        sql = "SELECT * FROM readings WHERE batch_id=?"
        args: list = [batch_id]
        if status:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY lineno"
        rows = self._all(sql, tuple(args))
        for r in rows:
            r["flags"] = json.loads(r.pop("flags_json") or "[]")
            r["reading_norm"] = _dec(r["reading_norm"])
        return rows

    def load_intervals(self, batch_id: str) -> list[dict]:
        rows = self._all(
            "SELECT * FROM intervals WHERE batch_id=? ORDER BY meter_id, register, start_at",
            (batch_id,))
        for r in rows:
            r["flags"] = json.loads(r.pop("flags_json") or "[]")
            for key in ("prev_reading", "cur_reading", "delta", "multiplier", "usage_kwh",
                        "days"):
                r[key] = _dec(r[key])
        return rows

    def load_allocations(self, batch_id: str) -> list[dict]:
        rows = self._all(
            "SELECT * FROM allocations WHERE batch_id=? ORDER BY month, meter_id, register",
            (batch_id,))
        for r in rows:
            r["usage_kwh"] = _dec(r["usage_kwh"])
            r["exact_share"] = _dec(r["exact_share"])
            r["sources"] = json.loads(r.pop("sources_json") or "[]")
            r["tou_adjusted"] = bool(r["tou_adjusted"])
        return rows

    def load_bills(self, batch_id: str) -> list[dict]:
        rows = self._all("SELECT * FROM bills WHERE batch_id=? ORDER BY month, meter_id",
                         (batch_id,))
        for r in rows:
            r["usage_kwh"] = _dec(r["usage_kwh"])
            r["amount_yuan"] = _dec(r["amount_yuan"])
            r["components"] = json.loads(r.pop("components_json") or "[]")
        return rows

    def load_quarantine(self, batch_id: str) -> list[dict]:
        return self._all(
            "SELECT * FROM quarantine WHERE batch_id=? ORDER BY lineno", (batch_id,))

    def load_stages(self, batch_id: str) -> list[dict]:
        from . import config
        rows = self._all("SELECT * FROM stages WHERE batch_id=? ORDER BY rowid", (batch_id,))
        for row in rows:
            row["label"] = config.STAGE_LABELS.get(row["stage"], row["stage"])
        return rows

    def load_totals(self, batch_id: str) -> dict:
        rows = self._all("SELECT key, value FROM totals WHERE batch_id=?", (batch_id,))
        return {r["key"]: r["value"] for r in rows}

    def load_manifest(self, batch_id: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute("SELECT json FROM manifests WHERE batch_id=?",
                               (batch_id,)).fetchone()
        return json.loads(row["json"]) if row else None

    def count_rows(self, batch_id: str) -> dict:
        tables = ("meters", "readings", "intervals", "allocations", "bills", "quarantine")
        with self.connect() as conn:
            return {t: conn.execute(
                f"SELECT COUNT(*) AS n FROM {t} WHERE batch_id=?", (batch_id,)
            ).fetchone()["n"] for t in tables}

    # ---------- 复核：候选快照 / 裁决 / 复核区间 ----------
    def save_conflict_candidates(self, batch_id: str, rows: list[dict]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM conflict_candidates WHERE batch_id=?", (batch_id,))
            for r in rows:
                conn.execute(
                    """INSERT INTO conflict_candidates (batch_id,group_key,lineno,segment,
                        meter_id,register,read_at,reading_text,reading_norm,unit_raw,
                        unit_norm,read_type,note,detail,flags_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, r["group_key"], r["lineno"], r.get("segment", 1),
                     r["meter_id"], r["register"], r["read_at"], r["reading_text"],
                     str(r["reading_norm"]), r["unit_raw"], r["unit_norm"],
                     r["read_type"], r.get("note", ""), r.get("detail", ""),
                     json.dumps(r.get("flags", []), ensure_ascii=False)))

    def load_conflict_candidates(self, batch_id: str) -> list[dict]:
        rows = self._all(
            "SELECT * FROM conflict_candidates WHERE batch_id=? "
            "ORDER BY group_key, lineno", (batch_id,))
        for r in rows:
            r["flags"] = json.loads(r.pop("flags_json") or "[]")
            r["reading_norm"] = _dec(r["reading_norm"])
        return rows

    def load_decisions(self, batch_id: str, status: str | None = None) -> list[dict]:
        sql = "SELECT * FROM conflict_decisions WHERE batch_id=?"
        args: list = [batch_id]
        if status:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY group_key, decided_at, decision_id"
        rows = self._all(sql, tuple(args))
        for r in rows:
            r["affected"] = json.loads(r.pop("affected_json") or "[]")
        return rows

    def active_decision(self, batch_id: str, group_key: str) -> dict | None:
        rows = self._all(
            "SELECT * FROM conflict_decisions WHERE batch_id=? AND group_key=? "
            "AND status='active' LIMIT 1", (batch_id, group_key))
        if not rows:
            return None
        rows[0]["affected"] = json.loads(rows[0].pop("affected_json") or "[]")
        return rows[0]

    def next_decision_revision(self, conn, batch_id: str, group_key: str) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(revision), 0) AS rev FROM conflict_decisions "
            "WHERE batch_id=? AND group_key=?", (batch_id, group_key)).fetchone()
        return int(row["rev"]) + 1

    def insert_decision(self, *, decision: dict, replace_active: bool = False,
                        expected_revision: int | None = None) -> dict:
        """在一个事务里插入裁决；并发或版本不符时整体回滚并抛 StaleDecisionError。

        - replace_active=True：先把当前 active 裁决降为 superseded，再插入新的 active；
        - expected_revision 不为空时必须等于当前 active 版本（没有 active 时为 0），
          否则说明有人抢先裁决，拒绝写入。
        """
        b = decision["batch_id"]
        g = decision["group_key"]
        with self.connect() as conn:
            # 先拿写锁再读当前版本：并发提交时第二个事务看到的是已提交的新版本，
            # 要么版本不符，要么被 active 唯一索引挡下，二者都转成 StaleDecisionError。
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute(
                "SELECT * FROM conflict_decisions WHERE batch_id=? AND group_key=? "
                "AND status='active' LIMIT 1", (b, g)).fetchone()
            current = int(active["revision"]) if active else 0
            if expected_revision is not None and expected_revision != current:
                raise StaleDecisionError(current)
            if active is not None:
                if not replace_active:
                    raise StaleDecisionError(current)
                conn.execute(
                    "UPDATE conflict_decisions SET status='superseded', updated_at=? "
                    "WHERE decision_id=?",
                    (decision["updated_at"], active["decision_id"]))
            revision = self.next_decision_revision(conn, b, g)
            decision = {**decision, "revision": revision}
            try:
                conn.execute(
                    """INSERT INTO conflict_decisions (decision_id,batch_id,group_key,
                        meter_id,register,read_at,selected_lineno,selected_value,reason,
                        status,revision,input_digest,rule_version,evidence_digest,
                        usage_before_kwh,usage_after_kwh,affected_json,invalid_reason,
                        decided_at,updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (decision["decision_id"], b, g, decision["meter_id"],
                     decision["register"], decision["read_at"],
                     int(decision["selected_lineno"]), str(decision["selected_value"]),
                     decision["reason"], "active", revision, decision["input_digest"],
                     decision["rule_version"], decision["evidence_digest"],
                     _opt_str(decision.get("usage_before_kwh")),
                     _opt_str(decision.get("usage_after_kwh")),
                     json.dumps(decision.get("affected", []), ensure_ascii=False),
                     decision.get("invalid_reason", ""), decision["decided_at"],
                     decision["updated_at"]))
            except sqlite3.IntegrityError as exc:
                raise StaleDecisionError(current) from exc
        return decision

    def invalidate_decisions(self, batch_id: str, *, reason: str, stamp: str,
                             group_keys: list[str] | None = None) -> int:
        """源数据变化：把 active 裁决降为 invalidated，保留原裁决与理由。"""
        with self.connect() as conn:
            sql = ("UPDATE conflict_decisions SET status='invalidated', "
                   "invalid_reason=?, updated_at=? WHERE batch_id=? AND status='active'")
            args: list = [reason, stamp, batch_id]
            if group_keys is not None:
                if not group_keys:
                    return 0
                placeholders = ",".join("?" for _ in group_keys)
                sql += f" AND group_key IN ({placeholders})"
                args.extend(group_keys)
            cursor = conn.execute(sql, tuple(args))
            return cursor.rowcount

    def close_decision(self, batch_id: str, *, group_key: str, status: str,
                       reason: str, stamp: str,
                       expected_revision: int | None = None) -> dict | None:
        """撤回/失效当前 active 裁决；记录保留，只改状态与原因。"""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute(
                "SELECT * FROM conflict_decisions WHERE batch_id=? AND group_key=? "
                "AND status='active' LIMIT 1", (batch_id, group_key)).fetchone()
            if active is None:
                return None
            if expected_revision is not None and \
                    int(active["revision"]) != int(expected_revision):
                raise StaleDecisionError(int(active["revision"]))
            conn.execute(
                "UPDATE conflict_decisions SET status=?, invalid_reason=?, "
                "updated_at=? WHERE decision_id=?",
                (status, reason, stamp, active["decision_id"]))
            row = dict(conn.execute(
                "SELECT * FROM conflict_decisions WHERE decision_id=?",
                (active["decision_id"],)).fetchone())
        row["affected"] = json.loads(row.pop("affected_json") or "[]")
        return row

    def update_decision_usages(self, batch_id: str, *, group_key: str,
                               usage_before, usage_after, affected: list[str],
                               stamp: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE conflict_decisions SET usage_before_kwh=?, usage_after_kwh=?, "
                "affected_json=?, updated_at=? WHERE batch_id=? AND group_key=? "
                "AND status='active'",
                (_opt_str(usage_before), _opt_str(usage_after),
                 json.dumps(affected, ensure_ascii=False), stamp, batch_id, group_key))

    def get_review(self, batch_id: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM review_runs WHERE batch_id=?",
                               (batch_id,)).fetchone()
        return dict(row) if row else None

    def upsert_review(self, batch_id: str, **fields) -> dict:
        current = self.get_review(batch_id) or {}
        merged = {**current, **fields}
        month_usage = merged.get("month_usage")
        if month_usage is None:
            month_usage = json.loads(merged.get("month_usage_json") or "{}")
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO review_runs (batch_id,status,attempts,baseline_usage_kwh,
                    reviewed_usage_kwh,baseline_interval_count,reviewed_interval_count,
                    recovered_count,original_quarantine_count,reviewed_quarantine_count,
                    evidence_digest,month_usage_json,last_error,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, attempts=excluded.attempts,
                     baseline_usage_kwh=excluded.baseline_usage_kwh,
                     reviewed_usage_kwh=excluded.reviewed_usage_kwh,
                     baseline_interval_count=excluded.baseline_interval_count,
                     reviewed_interval_count=excluded.reviewed_interval_count,
                     recovered_count=excluded.recovered_count,
                     original_quarantine_count=excluded.original_quarantine_count,
                     reviewed_quarantine_count=excluded.reviewed_quarantine_count,
                     evidence_digest=excluded.evidence_digest,
                     month_usage_json=excluded.month_usage_json,
                     last_error=excluded.last_error, updated_at=excluded.updated_at""",
                (batch_id, merged.get("status") or "idle",
                 int(merged.get("attempts") or 0),
                 _opt_str(merged.get("baseline_usage_kwh")),
                 _opt_str(merged.get("reviewed_usage_kwh")),
                 _opt_int(merged.get("baseline_interval_count")),
                 _opt_int(merged.get("reviewed_interval_count")),
                 int(merged.get("recovered_count") or 0),
                 _opt_int(merged.get("original_quarantine_count")),
                 _opt_int(merged.get("reviewed_quarantine_count")),
                 merged.get("evidence_digest"),
                 json.dumps(month_usage or {}, ensure_ascii=False, sort_keys=True),
                 merged.get("last_error") or "",
                 merged.get("updated_at") or ""))
        return self.get_review(batch_id) or {}

    def save_review_intervals(self, batch_id: str, intervals: list[dict]) -> None:
        """复核区间整批替换：与原始 intervals 分开存，原区间永远不动。"""
        with self.connect() as conn:
            conn.execute("DELETE FROM review_intervals WHERE batch_id=?", (batch_id,))
            for i in intervals:
                conn.execute(
                    """INSERT INTO review_intervals (batch_id,interval_id,meter_id,
                        register,start_at,end_at,prev_reading,cur_reading,delta,
                        multiplier,unit,usage_kwh,days,method,flags_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, i["interval_id"], i["meter_id"], i["register"],
                     i["start_at"], i["end_at"], str(i["prev_reading"]),
                     str(i["cur_reading"]), str(i["delta"]), str(i["multiplier"]),
                     i["unit"], str(i["usage_kwh"]), str(i["days"]), i["method"],
                     json.dumps(i.get("flags", []), ensure_ascii=False)))

    def load_review_intervals(self, batch_id: str) -> list[dict]:
        rows = self._all(
            "SELECT * FROM review_intervals WHERE batch_id=? "
            "ORDER BY meter_id, register, start_at", (batch_id,))
        for r in rows:
            r["flags"] = json.loads(r.pop("flags_json") or "[]")
            for key in ("prev_reading", "cur_reading", "delta", "multiplier",
                        "usage_kwh", "days"):
                r[key] = _dec(r[key])
        return rows

    def _all(self, sql: str, args: tuple) -> list[dict]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]


def _opt_str(value):
    return None if value is None else str(value)


def _opt_int(value):
    return None if value is None else int(value)
