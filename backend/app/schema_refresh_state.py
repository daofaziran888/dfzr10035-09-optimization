"""Schema 刷新与缓存版本协同的内存状态仓库。

承载跨 data_cache / schema_diff / sample_check / data_delivery / data_version
五个既有模块之上的协同状态，保证：

1. 端点 Schema 变更后缓存被置为「待刷新」，旧字段版本仍可投递；
2. 字段版本以分区为最小可用单元，缺主键的样本 / 未通过校验的分区不进投递；
3. 一笔投递的所有分区必须来自同一个字段版本；
4. 刷新回滚可中断重试，缓存恢复原可用状态，迟到回执不能让已撤回刷新自行生效。

所有表都是内存结构，与 app.store.store 中的通用模块表双向同步。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any


def now_iso() -> str:
    return datetime(2026, 11, 22, 10, 0, 0).isoformat()


def next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(r.get("id", 0)) for r in rows), default=0) + 1


class SchemaRefreshStore:
    """按端点聚合的字段版本 / 缓存分区 / 校验 / 投递 / 回滚状态。"""

    def __init__(self) -> None:
        # endpoint -> 端点协同视图
        self.endpoints: dict[str, dict[str, Any]] = {}
        # 字段版本：version_ref -> 版本对象
        self.field_versions: dict[str, dict[str, Any]] = {}
        # 缓存分区：partition_ref -> 分区对象
        self.partitions: dict[str, dict[str, Any]] = {}
        # 校验回执：receipt_ref -> 回执对象
        self.receipts: dict[str, dict[str, Any]] = {}
        # 投递单：delivery_ref -> 投递单（含分区领用 / 快照）
        self.deliveries: dict[str, dict[str, Any]] = {}
        # 回滚工单：rollback_ref -> 回滚对象（含中断恢复点）
        self.rollbacks: dict[str, dict[str, Any]] = {}
        # 事件流，便于审计核对
        self.events: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 端点视图与版本
    # ------------------------------------------------------------------
    def ensure_endpoint(self, endpoint: str) -> dict[str, Any]:
        view = self.endpoints.get(endpoint)
        if view is None:
            view = {
                "endpoint": endpoint,
                "active_version": None,      # 当前可用字段版本
                "pending_refresh": False,    # 缓存是否待刷新
                "refresh_version": None,     # 正在刷新的新版本
                "manifest": [],              # 可用缓存清单（分区引用）
                "todo": [],                  # 投递待办（分区引用，与 manifest 同步）
                "history_versions": [],      # 历史版本引用（兼容历史版本）
            }
            self.endpoints[endpoint] = view
        return view

    def reset(self) -> None:
        self.__init__()

    def log_event(self, kind: str, payload: dict[str, Any]) -> None:
        self.events.append({
            "id": len(self.events) + 1,
            "time": now_iso(),
            "kind": kind,
            "payload": payload,
        })


refresh_store = SchemaRefreshStore()
