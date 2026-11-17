"""M8 按批次复核：候选保留、逐条裁决、证据变化失效、中断恢复、并发拒绝。"""
from __future__ import annotations

import shutil
import threading
from decimal import Decimal

import pytest

from app import config, errors
from app.modules.m3_quality import service as m3_service
from app.modules.m6_batches import service as batch_service
from app.modules.m8_review import service as review
from app.store import Store


def _review_setup(settings, store):
    result = batch_service.run(settings, store, sample="readings_dirty.csv")
    batch_id = result["batch"]["batch_id"]
    payload = review.payload(settings, store, batch_id)
    return batch_id, payload


def _group(payload: dict) -> dict:
    assert len(payload["groups"]) == 1
    return payload["groups"][0]


def _candidate(group: dict, value: str) -> dict:
    return next(item for item in group["candidates"] if item["reading_norm"] == value)


def test_conflict_group_exposes_original_candidates(settings, store):
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    assert group["meter_id"] == "007"
    assert group["register"] == "TOTAL"
    assert [item["reading_norm"] for item in group["candidates"]] == ["210.0", "200.0"]
    assert [item["lineno"] for item in group["candidates"]] == [13, 58]
    # 原隔离结论逐行保留，候选只是快照，不改原行。
    quarantine = store.load_quarantine(batch_id)
    conflict_rows = [row for row in quarantine if row["code"] == "Q_DUPLICATE_CONFLICT"]
    assert {row["raw_value"] for row in conflict_rows} == {"210.0", "200.0"}
    assert {row["lineno"] for row in conflict_rows} == {13, 58}
    assert all(row["detail"].startswith("同键取值不一致") for row in conflict_rows)


def test_decision_requires_reason_and_known_candidate(settings, store):
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    with pytest.raises(errors.BadRequest) as excinfo:
        review.decide(settings, store, batch_id, group_key=group["group_key"],
                      selected_lineno=58, reason="   ")
    assert excinfo.value.code == "E_DECISION_INVALID"
    with pytest.raises(errors.BadRequest):
        review.decide(settings, store, batch_id, group_key=group["group_key"],
                      selected_lineno=999, reason="不存在的行")
    assert store.load_decisions(batch_id) == []


def test_decision_records_usage_before_and_after(settings, store):
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    selected = _candidate(group, "200.0")
    decision = review.decide(settings, store, batch_id, group_key=group["group_key"],
                             selected_lineno=selected["lineno"],
                             reason="现场核验：200.0 为实抄")
    assert decision["revision"] == 1
    assert decision["selected_value"] == "200.0"
    assert Decimal(str(decision["usage_before_kwh"])) == Decimal("200.000")
    assert Decimal(str(decision["usage_after_kwh"])) == Decimal("200.000")
    assert decision["affected"] == ["007:TOTAL:01", "007:TOTAL:02"]


def test_rebuild_keeps_original_batch_and_adds_review_intervals(settings, store):
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    original_intervals = store.load_intervals(batch_id)
    original_quarantine = store.load_quarantine(batch_id)
    original_totals = store.load_totals(batch_id)
    review.decide(settings, store, batch_id, group_key=group["group_key"],
                  selected_lineno=58, reason="现场核验")
    result = review.rebuild(settings, store, batch_id)

    assert result["stats"]["interval_count"] == 33
    assert len(store.load_intervals(batch_id)) == len(original_intervals) == 32
    assert len(store.load_review_intervals(batch_id)) == 33
    assert store.load_quarantine(batch_id) == original_quarantine
    assert store.load_totals(batch_id) == original_totals

    comparison = review.comparison(store, batch_id, settings=settings)
    assert comparison["review_status"] == "ready"
    assert comparison["reviewed_usage_kwh"] == "4499.900"
    assert comparison["baseline_usage_kwh"] == "4499.900"
    assert comparison["reviewed_quarantine_total"] == 13
    assert comparison["original_quarantine_total"] == 14
    # 逐月对照覆盖全部月份；样例里两条冲突值不同但采信后序列差量与月合计一致。
    assert [row["month"] for row in comparison["month_rows"]] == [
        "2025-12", "2026-01", "2026-02", "2026-03", "2026-04"]
    assert all(row["delta"] is not None for row in comparison["month_rows"])
    rows = store.load_decisions(batch_id)
    assert [(row["status"], row["selected_lineno"]) for row in rows] == [("active", 58)]


def test_pages_without_batch_render_review_states(client):
    page = client.get("/readings")
    assert page.status_code == 200
    assert "按批次复核" not in page.text
    run = client.post("/api/runs", json={"sample": "readings_clean.csv"}).json()
    clean = client.get("/readings")
    assert clean.status_code == 200
    assert "无需复核" in clean.text
    assert client.get(f"/api/batches/{run['batch_id']}/review").json()["groups"] == []


def test_readings_page_shows_review_comparison(settings, store):
    from fastapi.testclient import TestClient
    from app.main import create_app
    app = create_app(settings)
    client = TestClient(app)
    run = client.post("/api/runs", json={"sample": "readings_dirty.csv"}).json()
    batch_id = run["batch_id"]
    page = client.get("/readings")
    assert page.status_code == 200
    assert "按批次复核" in page.text
    assert "原隔离结论" in page.text and "复核后" in page.text
    assert "行 58" in page.text

    body = client.get(f"/api/batches/{batch_id}/review").json()
    group = body["groups"][0]
    saved = client.post(f"/api/batches/{batch_id}/review/decisions",
                        json={"group_key": group["group_key"], "selected_lineno": 58,
                              "reason": "现场核验", "expected_revision": 0})
    assert saved.status_code == 200
    rebuilt = client.post(f"/api/batches/{batch_id}/review/rebuild", json={})
    assert rebuilt.status_code == 200
    assert rebuilt.json()["review"]["review_status"] == "ready"
    assert rebuilt.json()["review"]["reviewed_usage_kwh"] == "4499.900"
    csv = client.get(f"/api/batches/{batch_id}/review/intervals.csv")
    assert csv.status_code == 200
    assert "007" in csv.text and "K_CONFLICT_ADJUDICATED" in csv.text
    assert "K_REVIEW_RANGE_REBUILT" in csv.text


def test_rerun_and_force_retry_preserve_manual_decision(settings, store):
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    review.decide(settings, store, batch_id, group_key=group["group_key"],
                  selected_lineno=58, reason="现场核验")
    replay = batch_service.run(settings, store, sample="readings_dirty.csv")
    assert replay["idempotent_replay"] is True
    retried = batch_service.retry(settings, store, batch_id)
    assert retried["status"] == "done"
    rows = store.load_decisions(batch_id)
    assert [(row["status"], row["selected_lineno"]) for row in rows] == [("active", 58)]
    rebuilt = review.rebuild(settings, store, batch_id)
    assert rebuilt["stats"]["interval_count"] == 33


def test_source_change_invalidates_decision_and_requires_reverification(tmp_path):
    sample_dir = tmp_path / "samples"
    shutil.copytree(config.PROJECT_ROOT / "sample_data", sample_dir)
    settings = config.Settings(db_path=str(tmp_path / "db.sqlite"),
                               sample_dir=str(sample_dir))
    store = Store(settings.db_path)
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    review.decide(settings, store, batch_id, group_key=group["group_key"],
                  selected_lineno=58, reason="现场核验")
    review.rebuild(settings, store, batch_id)

    # 现场把源文件改了一个读数：批次摘要不再匹配。
    dirty = sample_dir / "readings_dirty.csv"
    dirty.write_text(dirty.read_text(encoding="utf-8-sig").replace(
        "007,TOTAL,2026-02-01T00:00:00+08:00,100.0", "007,TOTAL,2026-02-01T00:00:00+08:00,110.0"),
        encoding="utf-8-sig")
    # 还没点重建，读取接口就已经把该组显示为「证据已变、需重核」。
    before = review.payload(settings, store, batch_id)
    assert before["evidence"]["source_changed"] is True
    assert before["groups"][0]["active_effective"] is False
    assert before["comparison"]["stale_active_decisions"] == 1
    assert before["comparison"]["resolved_group_count"] == 0
    assert before["comparison"]["pending_group_count"] == 1
    with pytest.raises(errors.Conflict) as excinfo:
        review.rebuild(settings, store, batch_id)
    assert excinfo.value.code == "E_EVIDENCE_CHANGED"

    status = review.evidence_status(settings, store, store.get_batch(batch_id))
    assert status["source_changed"] is True
    decisions = store.load_decisions(batch_id)
    assert decisions[0]["status"] == "invalidated"
    assert decisions[0]["invalid_reason"] == "E_EVIDENCE_CHANGED"
    assert store.get_review(batch_id)["status"] == "needs_review"
    # 上次成功重建的用量仍可读，裁决记录也还在。
    assert store.get_review(batch_id)["reviewed_usage_kwh"] == "4499.900"
    assert store.load_decisions(batch_id)[0]["reason"] == "现场核验"

    # 数据员重新处理（新批次）后，新批次可以重新核验。
    fresh = batch_service.run(settings, store, sample="readings_dirty.csv")
    assert fresh["batch"]["batch_id"] != batch_id
    fresh_group = review.payload(settings, store, fresh["batch"]["batch_id"])["groups"][0]
    saved = review.decide(settings, store, fresh["batch"]["batch_id"],
                          group_key=fresh_group["group_key"], selected_lineno=58,
                          reason="重新核验")
    assert saved["revision"] == 1


def test_candidate_evidence_change_invalidates_only_that_group(settings, store):
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    review.decide(settings, store, batch_id, group_key=group["group_key"],
                  selected_lineno=58, reason="现场核验")
    # 模拟候选快照被改：同组证据摘要变化，源文件摘要没变。
    with store.connect() as conn:
        conn.execute(
            "UPDATE conflict_candidates SET note='备注被改' WHERE batch_id=? AND lineno=?",
            (batch_id, 58))
    status = review.evidence_status(settings, store, store.get_batch(batch_id))
    assert status["source_changed"] is False
    assert status["stale_groups"] == [group["group_key"]]
    with pytest.raises(errors.Conflict) as excinfo:
        review.rebuild(settings, store, batch_id)
    assert excinfo.value.code == "E_EVIDENCE_CHANGED"
    assert store.load_decisions(batch_id)[0]["status"] == "invalidated"


def test_concurrent_decisions_reject_the_loser(settings, store):
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    outcomes: list[tuple] = []
    barrier = threading.Barrier(2)

    def worker(lineno: int, value: str, label: str):
        barrier.wait()
        try:
            decision = review.decide(settings, store, batch_id,
                                     group_key=group["group_key"],
                                     selected_lineno=lineno, reason=label,
                                     expected_revision=0)
            outcomes.append(("ok", value, decision["revision"]))
        except errors.Conflict as exc:
            outcomes.append(("stale", value, exc.code, exc.detail["current_revision"]))

    threads = [threading.Thread(target=worker, args=(13, "210.0", "并发甲")),
               threading.Thread(target=worker, args=(58, "200.0", "并发乙"))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(item[0] for item in outcomes) == ["ok", "stale"]
    stale = next(item for item in outcomes if item[0] == "stale")
    assert stale[2] == "E_STALE_DECISION"
    assert stale[3] == 1
    active = store.load_decisions(batch_id, status="active")
    assert len(active) == 1 and active[0]["revision"] == 1
    winner = next(item for item in outcomes if item[0] == "ok")
    assert active[0]["selected_value"] == winner[1]


def test_stale_decision_over_http_returns_409(client):
    run = client.post("/api/runs", json={"sample": "readings_dirty.csv"}).json()
    batch_id = run["batch_id"]
    group = client.get(f"/api/batches/{batch_id}/review").json()["groups"][0]
    first = client.post(f"/api/batches/{batch_id}/review/decisions",
                        json={"group_key": group["group_key"], "selected_lineno": 58,
                              "reason": "先到", "expected_revision": 0})
    assert first.status_code == 200
    stale = client.post(f"/api/batches/{batch_id}/review/decisions",
                        json={"group_key": group["group_key"], "selected_lineno": 13,
                              "reason": "后到", "expected_revision": 0})
    assert stale.status_code == 409
    assert stale.json()["error"] == "E_STALE_DECISION"
    assert stale.json()["detail"]["current_revision"] == 1
    assert client.get(f"/api/batches/{batch_id}/review").json()["groups"][0][
        "active"]["selected_lineno"] == 58


def test_failed_rebuild_keeps_last_usage_and_retry_does_not_duplicate(settings, store,
                                                                      monkeypatch):
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    review.decide(settings, store, batch_id, group_key=group["group_key"],
                  selected_lineno=58, reason="现场核验")
    first = review.rebuild(settings, store, batch_id)
    assert first["review"]["attempts"] == 1
    assert first["review"]["reviewed_usage_kwh"] == "4499.900"

    original_save = store.save_review_intervals

    def boom(batch_id_arg, intervals):
        raise RuntimeError("写到一半断电")

    monkeypatch.setattr(store, "save_review_intervals", boom)
    with pytest.raises(errors.Conflict) as excinfo:
        review.rebuild(settings, store, batch_id)
    assert excinfo.value.code == "E_REVIEW_FAILED"
    failed = store.get_review(batch_id)
    assert failed["status"] == "failed"
    assert failed["attempts"] == 2
    assert "写到一半断电" in failed["last_error"]
    # 失败仍能读到上次用量；旧复核区间没有被半批覆盖。
    assert failed["reviewed_usage_kwh"] == "4499.900"
    assert len(store.load_review_intervals(batch_id)) == 33

    monkeypatch.setattr(store, "save_review_intervals", original_save)
    retried = review.rebuild(settings, store, batch_id)
    assert retried["review"]["status"] == "ready"
    assert retried["review"]["attempts"] == 3
    assert retried["review"]["reviewed_usage_kwh"] == "4499.900"
    assert len(store.load_review_intervals(batch_id)) == 33
    assert len(store.load_decisions(batch_id, status="active")) == 1


def test_interrupted_rebuild_can_resume_without_double_counting(settings, store,
                                                                monkeypatch):
    """裁决保存后进程被杀：状态停在 running，重试沿用同一份裁决与上次用量。"""
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    review.decide(settings, store, batch_id, group_key=group["group_key"],
                  selected_lineno=58, reason="现场核验")
    first = review.rebuild(settings, store, batch_id)
    assert first["review"]["attempts"] == 1

    def killed(batch_id_arg, intervals):
        raise KeyboardInterrupt("选手动中断")

    monkeypatch.setattr(store, "save_review_intervals", killed)
    with pytest.raises(KeyboardInterrupt):
        review.rebuild(settings, store, batch_id)
    interrupted = store.get_review(batch_id)
    assert interrupted["status"] == "running"
    assert interrupted["attempts"] == 2
    # 中断没有碰上次成功重建的用量与区间。
    assert interrupted["reviewed_usage_kwh"] == "4499.900"
    assert len(store.load_review_intervals(batch_id)) == 33
    assert len(store.load_decisions(batch_id, status="active")) == 1


def test_rebuild_before_any_decision_refused_without_touching_state(settings, store):
    batch_id, _ = _review_setup(settings, store)
    result = review.rebuild(settings, store, batch_id)
    assert result["stats"]["interval_count"] == 32
    assert store.get_review(batch_id)["status"] == "ready"
    assert store.load_decisions(batch_id) == []
    assert len(store.load_quarantine(batch_id)) == 14


def test_withdraw_returns_group_to_pending_and_keeps_history(settings, store):
    batch_id, payload = _review_setup(settings, store)
    group = _group(payload)
    review.decide(settings, store, batch_id, group_key=group["group_key"],
                  selected_lineno=58, reason="现场核验")
    withdrawn = review.withdraw(settings, store, batch_id,
                                group_key=group["group_key"], reason="核验有误",
                                expected_revision=1)
    assert withdrawn["status"] == "superseded"
    assert withdrawn["invalid_reason"] == "核验有误"
    body = review.payload(settings, store, batch_id)
    assert body["groups"][0]["active"] is None
    assert body["groups"][0]["history"][0]["status"] == "superseded"
    assert body["comparison"]["pending_group_count"] == 1
    # 撤回后可重新裁决，版本号继续递增而不是复用。
    again = review.decide(settings, store, batch_id, group_key=group["group_key"],
                         selected_lineno=13, reason="重新核验", expected_revision=0)
    assert again["revision"] == 2


def test_m3_reports_conflicts_but_quarantine_unchanged(settings):
    from app.modules.m1_ingest import service as m1_service
    from app.modules.m2_contract import contract as contract_mod
    from app.modules.m2_contract import service as m2_service
    ingested = m1_service.ingest(settings, "readings_dirty.csv")
    contracts = contract_mod.load(settings)
    records, _, _ = m2_service.interpret_rows(ingested.rows, contracts)
    quality = m3_service.evaluate(records, contracts)
    assert len(quality["conflicts"]) == 2
    assert {item["reading"] for item in quality["conflicts"]} == {
        Decimal("210.0"), Decimal("200.0")}
    codes = [item["code"] for item in quality["quarantine"]]
    assert codes.count("Q_DUPLICATE_CONFLICT") == 2
