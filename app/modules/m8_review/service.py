"""M8 按批次复核：同键冲突读数人工裁决后重建区间。

边界（与整批重跑的分工）：
- 原 `quarantine` 表逐行不动，原始行号、原值、原因码仍是检查证据；
- 主 `intervals` / `allocations` / `bills` 也不动，它们是「原隔离结论」时的账；
- 复核结果写 `review_intervals`，裁决写 `conflict_decisions`，两者只追加或
  整批替换，不回写原证据；
- 裁决带版本号与证据摘要：源数据变化、候选快照变化或并发改判都会让旧裁决
  失效（`invalidated` / `superseded`），保留原因而不是删除。
"""
from __future__ import annotations

import datetime as dt
import json
import uuid
from decimal import Decimal

from ... import config, errors, manifest
from ..m2_contract import contract as contract_mod
from ..m3_quality import rules as quality_rules
from ..m4_intervals import restore as restore_mod
from ..m4_intervals import rollover
from ..m4_intervals.service import records_from_store
from ..m5_months import allocate as allocate_mod
from ..m6_batches import resume
from ...store import StaleDecisionError

DECISION_ACTIVE = "active"
DECISION_SUPERSEDED = "superseded"
DECISION_INVALIDATED = "invalidated"

REVIEW_IDLE = "idle"
REVIEW_RUNNING = "running"
REVIEW_READY = "ready"
REVIEW_NEEDS_REVIEW = "needs_review"
REVIEW_FAILED = "failed"


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


# ---------- 候选快照 ----------
def snapshot_rows(conflicts: list[dict]) -> list[dict]:
    """把 M3 判出的同键冲突记录转成复核候选；顺序与行号保持原样。"""
    rows: list[dict] = []
    for item in conflicts:
        group_key = quality_rules.conflict_key(
            item["meter_id"], item["register"], item["read_at"])
        values = sorted(str(value) for value in item.get("_conflict_values", []))
        rows.append({
            "group_key": group_key,
            "lineno": item["lineno"],
            "segment": item.get("segment", 1),
            "meter_id": item["meter_id"],
            "register": item["register"],
            "read_at": item["read_at"],
            "reading_text": item["reading_text"],
            "reading_norm": item["reading"],
            "unit_raw": item.get("unit_raw", ""),
            "unit_norm": item.get("unit_norm") or "",
            "read_type": item.get("read_type", "N"),
            "note": item.get("note", ""),
            "detail": "同键取值不一致：" + " / ".join(values),
            "flags": list(item.get("flags") or []),
        })
    rows.sort(key=lambda row: (row["group_key"], row["lineno"]))
    return rows


def build_conflict_groups(conflicts: list[dict]) -> list[dict]:
    """从 M3 冲突记录生成落库用的分组快照（含稳定组键与证据摘要）。"""
    rows = snapshot_rows(conflicts)
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["group_key"], []).append(row)
    groups = []
    for group_key in sorted(grouped):
        candidates = sorted(grouped[group_key], key=lambda item: item["lineno"])
        groups.append({
            "group_key": group_key,
            "evidence_digest": group_evidence_digest(group_key, candidates),
            "candidates": candidates,
        })
    return groups


def group_evidence_digest(group_key: str, rows: list[dict]) -> str:
    """一组候选的证据摘要：行号、原值、折合读数、类型、单位、备注任一变化即变。"""
    parts: list[str] = [group_key]
    for row in sorted(rows, key=lambda item: (int(item["lineno"]),
                                              str(item["reading_norm"]))):
        parts.extend([
            str(row["lineno"]),
            str(row["reading_norm"]),
            row["reading_text"],
            row.get("unit_raw") or "",
            row.get("unit_norm") or "",
            row.get("read_type") or "",
            row.get("note") or "",
            row.get("detail") or "",
        ])
    return manifest.digest_parts(parts)


def group_map(store, batch_id: str) -> dict[str, dict]:
    """group_key → 候选、当前证据摘要、active 裁决、全部历史裁决。"""
    rows = store.load_conflict_candidates(batch_id)
    decisions = store.load_decisions(batch_id)
    groups: dict[str, dict] = {}
    for row in rows:
        group = groups.setdefault(row["group_key"], {
            "group_key": row["group_key"],
            "meter_id": row["meter_id"],
            "register": row["register"],
            "read_at": row["read_at"],
            "candidates": [],
            "decisions": [],
            "active": None,
            "evidence_digest": "",
        })
        group["candidates"].append(row)
    for group in groups.values():
        group["candidates"].sort(key=lambda item: item["lineno"])
        group["evidence_digest"] = group_evidence_digest(
            group["group_key"], group["candidates"])
    for decision in decisions:
        group = groups.get(decision["group_key"])
        if group is None:
            continue
        if decision["status"] == DECISION_ACTIVE:
            group["active"] = decision
        group["decisions"].append(decision)
    for group in groups.values():
        group["history"] = group["decisions"]
    return groups


def candidate_index(store, batch_id: str) -> dict[tuple, dict]:
    return {(row["group_key"], int(row["lineno"])): row
            for row in store.load_conflict_candidates(batch_id)}


# ---------- 源数据 / 证据校验 ----------
def current_input_digest(settings, batch: dict) -> str | None:
    """按当前磁盘上的样例重算输入摘要；读不到返回 None（按变化处理）。"""
    from ..m1_ingest import service as m1_service
    from pathlib import Path
    try:
        path = m1_service.sample_path(settings, batch["sample"])
        sample_bytes = path.read_bytes()
        meters_bytes = (Path(settings.sample_dir) / config.METERS_FILE).read_bytes()
        prices_bytes = (Path(settings.sample_dir) / config.PRICES_FILE).read_bytes()
    except Exception:
        return None
    return manifest.input_digest(batch["sample"], sample_bytes, meters_bytes,
                                 prices_bytes, settings.rule_version)


def evidence_status(settings, store, batch: dict) -> dict:
    """只读检查：源文件摘要是否与批次一致、活动裁决的证据摘要是否仍匹配。"""
    digest = current_input_digest(settings, batch)
    source_changed = digest != batch["input_digest"]
    groups = group_map(store, batch["batch_id"])
    stale_groups = [key for key, group in groups.items()
                    if group["active"] is not None
                    and group["active"]["evidence_digest"] != group["evidence_digest"]]
    return {
        "source_changed": source_changed,
        "current_digest": digest,
        "batch_digest": batch["input_digest"],
        "stale_groups": stale_groups,
        "ok": not source_changed and not stale_groups,
    }


def verify_evidence(settings, store, batch: dict) -> dict:
    """重建前校验；不成立的活动裁决降为 invalidated 并保留原记录。"""
    status = evidence_status(settings, store, batch)
    stamp = now_iso()
    invalidated: list[str] = []
    if status["source_changed"]:
        count = store.invalidate_decisions(
            batch["batch_id"], reason="E_EVIDENCE_CHANGED", stamp=stamp)
        if count:
            invalidated.extend(item["group_key"] for item in store.load_decisions(
                batch["batch_id"], status=DECISION_INVALIDATED)
                if item.get("invalid_reason") == "E_EVIDENCE_CHANGED")
    for group_key in status["stale_groups"]:
        store.invalidate_decisions(
            batch["batch_id"], reason="E_EVIDENCE_CHANGED", stamp=stamp,
            group_keys=[group_key])
        invalidated.append(group_key)
    return {**status, "invalidated_groups": sorted(set(invalidated)),
            "ok": not invalidated}


# ---------- 重建计算 ----------
def candidate_record(row: dict) -> dict:
    flags = list(row.get("flags") or [])
    if "K_CONFLICT_ADJUDICATED" not in flags:
        flags.append("K_CONFLICT_ADJUDICATED")
    return {
        "lineno": int(row["lineno"]),
        "segment": row.get("segment", 1),
        "meter_id": row["meter_id"],
        "register": row["register"],
        "read_at": row["read_at"],
        "reading": Decimal(str(row["reading_norm"])),
        "read_type": row.get("read_type") or "N",
        "flags": flags,
        "note": row.get("note", ""),
    }


def _base_readings(store, batch_id: str) -> tuple[list[dict], list[dict]]:
    return (records_from_store(store.load_readings(batch_id, status="valid"), "N"),
            records_from_store(store.load_readings(batch_id, status="estimate"), "E"))


def compute(settings, store, batch: dict, decisions: list[dict]) -> dict:
    """按给定裁决集合在内存里重建全部区间与月度分摊；不写库。"""
    contracts = contract_mod.load(settings)
    prices = contract_mod.load_prices(settings)
    base_valid, base_estimate = _base_readings(store, batch["batch_id"])
    candidates = candidate_index(store, batch["batch_id"])
    groups: dict[str, list[dict]] = {}
    for row in candidates.values():
        groups.setdefault(row["group_key"], []).append(row)
    selected_valid = list(base_valid)
    selected_estimate = list(base_estimate)
    for decision in decisions:
        group_key = decision["group_key"]
        row = candidates.get((group_key, int(decision["selected_lineno"])))
        if row is None:
            raise errors.Conflict(
                "裁决对应的候选行已不存在，需重新核验",
                code="E_EVIDENCE_CHANGED",
                detail={"group_key": group_key})
        if group_evidence_digest(group_key, groups.get(group_key, [])) != \
                decision["evidence_digest"]:
            raise errors.Conflict(
                "候选证据已变化，裁决失效", code="E_EVIDENCE_CHANGED",
                detail={"group_key": group_key})
        record = candidate_record(row)
        if record["read_type"] == "E":
            selected_estimate.append(record)
        else:
            selected_valid.append(record)
    try:
        intervals = restore_mod.build_intervals(selected_valid, selected_estimate,
                                                contracts)
    except rollover.ReverseMovement as exc:
        raise errors.BadRequest(str(exc), code="E_DECISION_INVALID") from exc
    stats = restore_mod.totals(intervals, contracts)
    allocation = allocate_mod.allocate(
        [{"interval_id": item["interval_id"], "meter_id": item["meter_id"],
          "register": item["register"], "start_at": item["start_at"],
          "end_at": item["end_at"], "usage_kwh": item["usage_kwh"],
          "days": item["days"]} for item in intervals], contracts, prices)
    return {"intervals": intervals, "stats": stats, "contracts": contracts,
            "base_valid": base_valid, "base_estimate": base_estimate,
            "candidates": candidates, "allocation": allocation,
            "month_usage": _month_usage(allocation["allocations"], contracts)}


def _month_usage(allocations: list[dict],
                 contracts: dict[str, contract_mod.MeterContract]) -> dict[str, str]:
    primary = {m.meter_id: m.primary_register for m in contracts.values()}
    months: dict[str, Decimal] = {}
    for item in allocations:
        if primary.get(item["meter_id"]) != item["register"]:
            continue
        months[item["month"]] = months.get(item["month"], Decimal("0")) + item["usage_kwh"]
    return {month: str(value.quantize(config.USAGE_QUANT))
            for month, value in sorted(months.items())}


def series_usage(records_valid: list[dict], records_estimate: list[dict],
                 contracts: dict, meter_id: str, register: str) -> tuple[Decimal, list[str]]:
    """只重建一组 (表号, 寄存器) 序列，用于裁决前后用量对照。"""
    valid = [item for item in records_valid
             if item["meter_id"] == meter_id and item["register"] == register]
    estimate = [item for item in records_estimate
                if item["meter_id"] == meter_id and item["register"] == register]
    intervals = restore_mod.build_intervals(valid, estimate, contracts)
    total = sum((item["usage_kwh"] for item in intervals
                 if item["meter_id"] == meter_id and item["register"] == register),
                Decimal("0"))
    return (total.quantize(config.USAGE_QUANT),
            [item["interval_id"] for item in intervals])


def decision_usage(result: dict, decision: dict) -> tuple[Decimal, Decimal, list[str]]:
    """单条裁决的对应关系：原证据序列用量 → 采信该候选后的序列用量。"""
    contracts = result["contracts"]
    candidates = result["candidates"]
    row = candidates[(decision["group_key"], int(decision["selected_lineno"]))]
    meter_id, register = row["meter_id"], row["register"]
    before, _ = series_usage(result["base_valid"], result["base_estimate"],
                             contracts, meter_id, register)
    record = candidate_record(row)
    valid = list(result["base_valid"])
    estimate = list(result["base_estimate"])
    if record["read_type"] == "E":
        estimate.append(record)
    else:
        valid.append(record)
    after, affected = series_usage(valid, estimate, contracts, meter_id, register)
    return before, after, affected


# ---------- 裁决写入 ----------
def decide(settings, store, batch_id: str, *, group_key: str, selected_lineno: int,
           reason: str, expected_revision: int = 0) -> dict:
    batch = resume.require_batch(store, batch_id)
    reason = (reason or "").strip()
    if not reason:
        raise errors.BadRequest("裁决必须写明理由", code="E_DECISION_INVALID",
                                detail={"field": "reason"})
    groups = group_map(store, batch_id)
    group = groups.get(group_key)
    if group is None:
        raise errors.BadRequest("复核组不存在", code="E_DECISION_INVALID",
                                detail={"group_key": group_key})
    row = next((item for item in group["candidates"]
                if int(item["lineno"]) == int(selected_lineno)), None)
    if row is None:
        raise errors.BadRequest("所选候选不属于该复核组", code="E_DECISION_INVALID",
                                detail={"group_key": group_key,
                                        "selected_lineno": selected_lineno})
    # 源数据已变化：先让旧裁决失效，再要求重新核验，不落这条新裁决。
    evidence = evidence_status(settings, store, batch)
    if evidence["source_changed"]:
        store.invalidate_decisions(batch_id, reason="E_EVIDENCE_CHANGED",
                                   stamp=now_iso())
        raise errors.Conflict(
            "源数据已变化，旧裁决已失效；请重新处理批次后再核验",
            code="E_EVIDENCE_CHANGED", detail=evidence)

    active = store.load_decisions(batch_id, status=DECISION_ACTIVE)
    trial = [item for item in active if item["group_key"] != group_key]
    trial.append({"group_key": group_key, "selected_lineno": int(selected_lineno),
                  "evidence_digest": group["evidence_digest"]})
    try:
        result = compute(settings, store, batch, trial)
    except errors.MeterbackError:
        raise
    except Exception as exc:  # 数值重建不成立：拒绝这条裁决，旧裁决不受影响
        raise errors.BadRequest(f"该候选无法重建区间：{exc}",
                                code="E_DECISION_INVALID") from exc
    probe = {"group_key": group_key, "selected_lineno": int(selected_lineno),
             "evidence_digest": group["evidence_digest"]}
    usage_before, usage_after, affected = decision_usage(result, probe)

    stamp = now_iso()
    decision = {
        "decision_id": "dec-" + uuid.uuid4().hex[:12],
        "batch_id": batch_id,
        "group_key": group_key,
        "meter_id": group["meter_id"],
        "register": group["register"],
        "read_at": group["read_at"],
        "selected_lineno": int(selected_lineno),
        "selected_value": str(row["reading_norm"]),
        "reason": reason,
        "status": DECISION_ACTIVE,
        "input_digest": batch["input_digest"],
        "rule_version": settings.rule_version,
        "evidence_digest": group["evidence_digest"],
        "usage_before_kwh": usage_before,
        "usage_after_kwh": usage_after,
        "affected": affected,
        "invalid_reason": "",
        "decided_at": stamp,
        "updated_at": stamp,
    }
    try:
        return store.insert_decision(decision=decision, replace_active=True,
                                     expected_revision=int(expected_revision))
    except StaleDecisionError as exc:
        raise errors.Conflict(
            f"裁决已被他人更新（当前版本 {exc.current_revision}），请刷新后重试",
            code="E_STALE_DECISION",
            detail={"current_revision": exc.current_revision}) from exc


def withdraw(settings, store, batch_id: str, *, group_key: str, reason: str,
             expected_revision: int | None = None) -> dict:
    """撤回当前裁决：组回到「未复核」，原隔离结论本来就还在。"""
    batch = resume.require_batch(store, batch_id)
    evidence = evidence_status(settings, store, batch)
    if evidence["source_changed"]:
        raise errors.Conflict("源数据已变化，需重新处理批次",
                              code="E_EVIDENCE_CHANGED", detail=evidence)
    reason = (reason or "").strip() or "人工撤回复核裁决"
    try:
        withdrawn = store.close_decision(
            batch_id, group_key=group_key, status=DECISION_SUPERSEDED,
            reason=reason, stamp=now_iso(), expected_revision=expected_revision)
    except StaleDecisionError as exc:
        raise errors.Conflict(
            f"裁决已被他人更新（当前版本 {exc.current_revision}），请刷新后重试",
            code="E_STALE_DECISION",
            detail={"current_revision": exc.current_revision}) from exc
    if withdrawn is None:
        raise errors.NotFound("该组没有生效裁决", code="E_DECISION_INVALID",
                              detail={"group_key": group_key})
    return withdrawn


# ---------- 重建落库 ----------
def rebuild(settings, store, batch_id: str) -> dict:
    batch = resume.require_batch(store, batch_id)
    review = store.get_review(batch_id) or {}
    attempts = int(review.get("attempts") or 0) + 1
    stamp = now_iso()
    totals = store.load_totals(batch_id)
    baseline_usage = review.get("baseline_usage_kwh") or totals.get("interval_usage")
    baseline_count = (review.get("baseline_interval_count")
                      or totals.get("interval_count"))
    original_quarantine = len(store.load_quarantine(batch_id))
    store.upsert_review(
        batch_id, status=REVIEW_RUNNING, attempts=attempts,
        baseline_usage_kwh=baseline_usage,
        baseline_interval_count=baseline_count,
        original_quarantine_count=original_quarantine,
        last_error="", updated_at=stamp)
    try:
        evidence = verify_evidence(settings, store, batch)
        if not evidence["ok"]:
            store.upsert_review(
                batch_id, status=REVIEW_NEEDS_REVIEW, last_error="E_EVIDENCE_CHANGED",
                updated_at=now_iso())
            raise errors.Conflict(
                "源数据或候选证据已变化，裁决失效；请重新核验后再计算",
                code="E_EVIDENCE_CHANGED", detail=evidence)

        decisions = store.load_decisions(batch_id, status=DECISION_ACTIVE)
        result = compute(settings, store, batch, decisions)
        usages = {item["group_key"]: decision_usage(result, item)
                  for item in decisions}

        # 受裁决影响的区间再补一个「复核重建」标记，复核区间证据里可逐段追溯。
        touched = {interval_id for _, _, affected in usages.values()
                   for interval_id in affected}
        for interval in result["intervals"]:
            if interval["interval_id"] in touched and \
                    "K_REVIEW_RANGE_REBUILT" not in interval["flags"]:
                interval["flags"].append("K_REVIEW_RANGE_REBUILT")

        # 先整批替换复核区间（单事务），再更新状态；中断时旧区间与上次用量仍在。
        store.save_review_intervals(batch_id, result["intervals"])
        recovered = len(decisions)
        store.upsert_review(
            batch_id, status=REVIEW_READY, attempts=attempts,
            baseline_usage_kwh=baseline_usage,
            baseline_interval_count=baseline_count,
            reviewed_usage_kwh=result["stats"]["primary_usage_kwh"],
            reviewed_interval_count=result["stats"]["interval_count"],
            recovered_count=recovered,
            original_quarantine_count=original_quarantine,
            reviewed_quarantine_count=max(0, original_quarantine - recovered),
            evidence_digest=batch["input_digest"],
            month_usage=result["month_usage"],
            last_error="", updated_at=now_iso())
        for decision in decisions:
            before, after, affected = usages[decision["group_key"]]
            store.update_decision_usages(
                batch_id, group_key=decision["group_key"],
                usage_before=before, usage_after=after, affected=affected,
                stamp=now_iso())
        return {
            "batch_id": batch_id,
            "status": REVIEW_READY,
            "review": store.get_review(batch_id),
            "intervals": result["intervals"],
            "stats": result["stats"],
            "decisions": store.load_decisions(batch_id, status=DECISION_ACTIVE),
        }
    except errors.MeterbackError as exc:
        current = store.get_review(batch_id) or {}
        status = (REVIEW_NEEDS_REVIEW if exc.code == "E_EVIDENCE_CHANGED"
                  else REVIEW_FAILED)
        store.upsert_review(
            batch_id, status=status, attempts=attempts,
            baseline_usage_kwh=current.get("baseline_usage_kwh") or baseline_usage,
            baseline_interval_count=(current.get("baseline_interval_count")
                                     or baseline_count),
            original_quarantine_count=(current.get("original_quarantine_count")
                                       if current.get("original_quarantine_count")
                                       is not None else original_quarantine),
            last_error=exc.code, updated_at=now_iso())
        raise
    except Exception as exc:
        store.upsert_review(
            batch_id, status=REVIEW_FAILED, attempts=attempts,
            baseline_usage_kwh=baseline_usage,
            baseline_interval_count=baseline_count,
            original_quarantine_count=original_quarantine,
            last_error=f"E_REVIEW_FAILED: {exc}", updated_at=now_iso())
        raise errors.Conflict(f"复核重建失败：{exc}", code="E_REVIEW_FAILED") from exc


# ---------- 页面 / 接口载荷 ----------
def _baseline_month_usage(settings, store, batch_id: str) -> dict[str, str]:
    """原隔离结论下的主寄存器月度用量（总表优先），从已落库分摊读。"""
    from ..m5_months import service as months_service
    allocations = store.load_allocations(batch_id)
    primary = months_service.primary_map(settings)
    months: dict[str, Decimal] = {}
    for item in allocations:
        if primary.get(item["meter_id"]) != item["register"]:
            continue
        months[item["month"]] = months.get(item["month"], Decimal("0")) + item["usage_kwh"]
    return {month: str(value.quantize(config.USAGE_QUANT))
            for month, value in sorted(months.items())}


def comparison(store, batch_id: str, groups: dict[str, dict] | None = None,
               settings=None, evidence: dict | None = None) -> dict:
    review = store.get_review(batch_id) or {}
    totals = store.load_totals(batch_id)
    groups = groups if groups is not None else group_map(store, batch_id)
    quarantine = store.load_quarantine(batch_id)
    decisions = store.load_decisions(batch_id)
    active = [item for item in decisions if item["status"] == DECISION_ACTIVE]
    invalidated = [item for item in decisions if item["status"] == DECISION_INVALIDATED]
    # 源数据或该组证据已变化时，active 裁决立即按失效显示，不等到重建才反映。
    stale_groups = set((evidence or {}).get("stale_groups") or [])
    source_changed = bool((evidence or {}).get("source_changed"))
    effective_active = [
        item for item in active
        if not source_changed and item["group_key"] not in stale_groups]
    effective_ids = {item["decision_id"] for item in effective_active}
    stale_active = [item for item in active if item["decision_id"] not in effective_ids]
    baseline_usage = review.get("baseline_usage_kwh") or totals.get("interval_usage")
    reviewed_usage = review.get("reviewed_usage_kwh")
    delta = None
    if reviewed_usage is not None and baseline_usage is not None:
        delta = (Decimal(str(reviewed_usage)) - Decimal(str(baseline_usage)))
    baseline_month_usage = (_baseline_month_usage(settings, store, batch_id)
                            if settings is not None else {})
    reviewed_month_usage = json.loads(review.get("month_usage_json") or "{}")
    months = sorted(set(baseline_month_usage) | set(reviewed_month_usage))
    month_rows = [{
        "month": month,
        "baseline": baseline_month_usage.get(month),
        "reviewed": reviewed_month_usage.get(month),
        "delta": (None if month not in reviewed_month_usage
                  or month not in baseline_month_usage
                  else str(Decimal(reviewed_month_usage[month])
                           - Decimal(baseline_month_usage[month]))),
    } for month in months]
    return {
        "review_status": review.get("status") or REVIEW_IDLE,
        "attempts": int(review.get("attempts") or 0),
        "last_error": review.get("last_error") or "",
        "baseline_usage_kwh": baseline_usage,
        "baseline_interval_count": (review.get("baseline_interval_count")
                                    or totals.get("interval_count")),
        "reviewed_usage_kwh": reviewed_usage,
        "reviewed_interval_count": review.get("reviewed_interval_count"),
        "usage_delta_kwh": None if delta is None else str(delta),
        "month_rows": month_rows,
        "original_quarantine_total": len(quarantine),
        "original_conflict_rows": sum(len(group["candidates"])
                                      for group in groups.values()),
        "conflict_group_count": len(groups),
        "recovered_rows": len(effective_active),
        "resolved_group_count": sum(1 for group in groups.values()
                                    if group["active"] is not None
                                    and group["active"]["decision_id"] in effective_ids),
        "pending_group_count": sum(
            1 for group in groups.values()
            if group["active"] is None
            or group["active"]["decision_id"] not in effective_ids),
        "invalidated_decisions": len(invalidated),
        "stale_active_decisions": len(stale_active),
        "reviewed_quarantine_total": review.get("reviewed_quarantine_count"),
        "evidence_digest": review.get("evidence_digest"),
    }


def group_payload(store, group: dict, quarantine_by_lineno: dict[int, list[dict]],
                  *, stale: bool = False) -> dict:
    return {
        "group_key": group["group_key"],
        "meter_id": group["meter_id"],
        "register": group["register"],
        "read_at": group["read_at"],
        "evidence_digest": group["evidence_digest"],
        "candidates": [{
            "lineno": row["lineno"],
            "segment": row["segment"],
            "reading_text": row["reading_text"],
            "reading_norm": str(row["reading_norm"]),
            "unit_raw": row["unit_raw"],
            "unit_norm": row["unit_norm"],
            "read_type": row["read_type"],
            "note": row["note"],
            "detail": row["detail"],
            "quarantine": quarantine_by_lineno.get(row["lineno"], []),
        } for row in group["candidates"]],
        "active": group["active"],
        "active_effective": group["active"] is not None and not stale,
        "history": group["decisions"],
    }


def payload(settings, store, batch_id: str, *, with_intervals: bool = False) -> dict:
    batch = resume.require_batch(store, batch_id)
    groups = group_map(store, batch_id)
    quarantine = store.load_quarantine(batch_id)
    evidence = evidence_status(settings, store, batch)
    by_lineno: dict[int, list[dict]] = {}
    for row in quarantine:
        by_lineno.setdefault(row["lineno"], []).append(row)
    body = {
        "batch_id": batch_id,
        "evidence": evidence,
        "groups": [group_payload(
            store, group, by_lineno,
            stale=evidence["source_changed"]
            or group["group_key"] in (evidence.get("stale_groups") or []))
                   for group in groups.values()],
        "comparison": comparison(store, batch_id, groups, settings=settings,
                                 evidence=evidence),
    }
    review = store.get_review(batch_id)
    if with_intervals and review and review.get("reviewed_usage_kwh") is not None:
        body["intervals"] = [{
            "interval_id": item["interval_id"],
            "meter_id": item["meter_id"],
            "register": item["register"],
            "start_at": item["start_at"],
            "end_at": item["end_at"],
            "prev_reading": str(item["prev_reading"]),
            "cur_reading": str(item["cur_reading"]),
            "usage_kwh": str(item["usage_kwh"]),
            "method": item["method"],
            "flags": item["flags"],
        } for item in store.load_review_intervals(batch_id)]
    return body


def metric(value) -> str:
    return "—" if value is None else str(value)


__all__ = ["decide", "rebuild", "payload", "comparison", "group_map", "withdraw",
           "snapshot_rows", "build_conflict_groups", "verify_evidence",
           "evidence_status", "now_iso", "DECISION_ACTIVE", "DECISION_INVALIDATED",
           "DECISION_SUPERSEDED", "REVIEW_IDLE", "REVIEW_READY", "REVIEW_FAILED",
           "REVIEW_NEEDS_REVIEW", "REVIEW_RUNNING"]
