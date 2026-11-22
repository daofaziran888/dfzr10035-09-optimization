"""Schema 刷新与缓存版本协同工作流路由。

固定路径，需在通用资源路由 /api/{module} 之前注册。
所有业务判断都在 SchemaRefreshCoordinator 中完成，路由层只做协议转换。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.schema_refresh import SchemaRefreshError, coordinator

router = APIRouter(prefix="/api/schema-refresh", tags=["Schema刷新协同"])


class PreparePayload(BaseModel):
    endpoint: str
    regions: list[str] = Field(default_factory=lambda: ["东区", "西区"])
    primary_key: str = "record_id"


class SchemaChangePayload(BaseModel):
    endpoint: str
    new_fields: list[str]


class SamplePayload(BaseModel):
    endpoint: str
    version_ref: str
    region: str
    sample: dict[str, Any]
    receipt_ref: str | None = None


class ReceiptPayload(BaseModel):
    receipt_ref: str
    accepted: bool


class ClaimPayload(BaseModel):
    endpoint: str
    subscriber: str
    version_ref: str | None = None
    regions: list[str] | None = None


class SendPayload(BaseModel):
    delivery_ref: str
    region: str


class RollbackPayload(BaseModel):
    endpoint: str
    reason: str = "validation_failed"
    interrupt: bool = False


class ResumePayload(BaseModel):
    rollback_ref: str


class RevalidatePayload(BaseModel):
    endpoint: str
    version_ref: str
    region: str
    fixed_sample: dict[str, Any]


class DeliveryRefPayload(BaseModel):
    delivery_ref: str


def _run(call):
    try:
        return {"ok": True, "data": call()}
    except SchemaRefreshError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/endpoints/prepare")
def prepare_endpoint(payload: PreparePayload):
    return _run(lambda: coordinator.prepare_endpoint(
        payload.endpoint, payload.regions, payload.primary_key))


@router.post("/schema-change")
def mark_schema_change(payload: SchemaChangePayload):
    return _run(lambda: coordinator.mark_schema_change(
        payload.endpoint, payload.new_fields))


@router.post("/samples")
def submit_sample(payload: SamplePayload):
    return _run(lambda: coordinator.submit_sample(
        payload.endpoint, payload.version_ref, payload.region,
        payload.sample, payload.receipt_ref))


@router.post("/receipts/handle")
def handle_receipt(payload: ReceiptPayload):
    return _run(lambda: coordinator.handle_receipt(
        payload.receipt_ref, payload.accepted))


@router.post("/deliveries/claim")
def claim_delivery(payload: ClaimPayload):
    return _run(lambda: coordinator.claim_delivery(
        payload.endpoint, payload.subscriber,
        payload.version_ref, payload.regions))


@router.post("/deliveries/send-partition")
def send_partition(payload: SendPayload):
    return _run(lambda: coordinator.send_partition(
        payload.delivery_ref, payload.region))


@router.post("/deliveries/pause")
def pause_delivery(payload: DeliveryRefPayload):
    return _run(lambda: coordinator.pause_delivery(payload.delivery_ref))


@router.post("/rollbacks/request")
def request_rollback(payload: RollbackPayload):
    return _run(lambda: coordinator.request_rollback(
        payload.endpoint, payload.reason, payload.interrupt))


@router.post("/rollbacks/resume")
def resume_rollback(payload: ResumePayload):
    return _run(lambda: coordinator.resume_rollback(payload.rollback_ref))


@router.post("/revalidate")
def revalidate(payload: RevalidatePayload):
    return _run(lambda: coordinator.revalidate(
        payload.endpoint, payload.version_ref, payload.region,
        payload.fixed_sample))


@router.post("/deliveries/complete")
def complete_delivery(payload: DeliveryRefPayload):
    return _run(lambda: coordinator.complete_delivery(payload.delivery_ref))


@router.get("/endpoints/{endpoint}/manifest")
def get_manifest(endpoint: str):
    return _run(lambda: coordinator.get_manifest(endpoint))


@router.get("/deliveries")
def list_deliveries(endpoint: str | None = None):
    return _run(lambda: {"items": coordinator.list_deliveries(endpoint)})


@router.get("/deliveries/{delivery_ref}")
def get_delivery(delivery_ref: str):
    return _run(lambda: coordinator.get_delivery_record(delivery_ref))


@router.get("/rollbacks")
def list_rollbacks(endpoint: str | None = None):
    return _run(lambda: {"items": coordinator.list_rollbacks(endpoint)})


@router.get("/events")
def event_log():
    return _run(lambda: {"items": coordinator.event_log()})
