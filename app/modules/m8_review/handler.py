"""M8 入口：读数诊断页上的按批次复核。

接口：
- GET  /api/batches/{batch_id}/review          候选、裁决历史、用量对照
- POST /api/batches/{batch_id}/review/decisions 选定候选 + 理由（带版本号）
- DELETE /api/batches/{batch_id}/review/decisions/{group_key} 撤回裁决
- POST /api/batches/{batch_id}/review/rebuild   重建复核区间与用量
- GET  /api/batches/{batch_id}/review/intervals.csv 复核区间证据
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Response

from ...web import Ctx, get_ctx
from ..m3_quality import quarantine as quarantine_mod
from ..m6_batches import resume as resume_mod
from ..m7_export import csv_out, evidence
from . import service

router = APIRouter(tags=["M8 按批次复核"])


@router.get("/api/batches/{batch_id}/review")
def api_review(batch_id: str, with_intervals: int = 0,
               ctx: Ctx = Depends(get_ctx)) -> dict:
    resume_mod.require_batch(ctx.store, batch_id)
    return service.payload(ctx.settings, ctx.store, batch_id,
                           with_intervals=bool(with_intervals))


@router.post("/api/batches/{batch_id}/review/decisions")
def api_decide(batch_id: str, payload: dict | None = None,
               ctx: Ctx = Depends(get_ctx)) -> dict:
    body = payload or {}
    decision = service.decide(
        ctx.settings, ctx.store, batch_id,
        group_key=str(body.get("group_key") or ""),
        selected_lineno=int(body.get("selected_lineno") or 0),
        reason=str(body.get("reason") or ""),
        expected_revision=int(body.get("expected_revision") or 0))
    return {"batch_id": batch_id, "decision": decision,
            "review": service.comparison(ctx.store, batch_id, settings=ctx.settings)}


@router.delete("/api/batches/{batch_id}/review/decisions/{group_key}")
def api_withdraw(batch_id: str, group_key: str,
                 payload: dict | None = None,
                 ctx: Ctx = Depends(get_ctx)) -> dict:
    body = payload or {}
    decision = service.withdraw(
        ctx.settings, ctx.store, batch_id, group_key=group_key,
        reason=str(body.get("reason") or ""),
        expected_revision=(None if body.get("expected_revision") is None
                           else int(body.get("expected_revision"))))
    return {"batch_id": batch_id, "withdrawn": decision,
            "review": service.comparison(ctx.store, batch_id, settings=ctx.settings)}


@router.post("/api/batches/{batch_id}/review/rebuild")
def api_rebuild(batch_id: str, payload: dict | None = None,
                ctx: Ctx = Depends(get_ctx)) -> dict:
    resume_mod.require_batch(ctx.store, batch_id)
    result = service.rebuild(ctx.settings, ctx.store, batch_id)
    body = service.payload(ctx.settings, ctx.store, batch_id, with_intervals=True)
    body["review"] = service.comparison(ctx.store, batch_id, settings=ctx.settings)
    body["rebuilt"] = {
        "usage_kwh": str(result["stats"]["primary_usage_kwh"]),
        "interval_count": result["stats"]["interval_count"],
    }
    return body


@router.get("/api/batches/{batch_id}/review/intervals.csv")
def api_review_intervals_csv(batch_id: str,
                             ctx: Ctx = Depends(get_ctx)) -> Response:
    resume_mod.require_batch(ctx.store, batch_id)
    rows = ctx.store.load_review_intervals(batch_id)
    body = csv_out.render(evidence.INTERVAL_HEADER,
                          evidence.interval_rows(rows))
    filename = f"review-intervals-{batch_id}.csv"
    return Response(content=body, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


def page_context(ctx: Ctx, batch: dict | None) -> dict:
    if batch is None:
        return {"review": None, "groups": [], "evidence": None, "review_intervals": [],
                "comparison": None, "qsummary": None}
    batch_id = batch["batch_id"]
    body = service.payload(ctx.settings, ctx.store, batch_id, with_intervals=True)
    quarantine = ctx.store.load_quarantine(batch_id)
    review_rows = ctx.store.get_review(batch_id)
    comparison = body["comparison"]
    return {
        "review": review_rows,
        "groups": body["groups"],
        "evidence": body["evidence"],
        "review_intervals": body.get("intervals") or [],
        "comparison": comparison,
        "qsummary": quarantine_mod.summary(quarantine),
        "has_rebuild": comparison["reviewed_usage_kwh"] is not None,
    }
