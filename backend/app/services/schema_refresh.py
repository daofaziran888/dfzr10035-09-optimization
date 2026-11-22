"""Schema 刷新 / 缓存版本 / 投递恢复协同领域服务。

把「端点 Schema 变更 → 缓存待刷新 → 样本校验 → 分区可用/回滚 → 版本一致投递」
这条跨模块流程收敛在一个协调器里，并与 app.store.store 中的通用模块表
（schema_diff / data_cache / sample_check / data_delivery / data_version /
retry_queue / pipeline_audit / call_log）双向同步。

关键不变量：
- 缺主键的样本直接判定校验失败，其分区不进入可用缓存清单；
- 一笔投递领用的所有分区必须来自同一个字段版本；
- 未通过校验的分区不会随刷新进入投递；
- 回滚中断后可幂等重试，缓存恢复到刷新前的可用状态；
- 已领用的 v1 投递在回滚后仍能按原分区补投；
- v2 的迟到校验回执在刷新被撤回后不能使其自行生效；
- 完成投递按可用缓存版本取实际分区并写快照，交付记录保留字段版本与分区。
"""
from __future__ import annotations

import copy
from typing import Any

from app.schema_refresh_state import next_id, now_iso, refresh_store
from app.store import store

# 分区 / 版本 / 回执等状态枚举
VERSION_STATES = ("provisioning", "refreshing", "active", "withdrawn", "failed")
PARTITION_STATES = ("collecting", "awaiting_validation", "valid", "invalid", "withdrawn")
RECEIPT_STATES = ("pending", "accepted", "rejected", "ignored")
DELIVERY_STATES = ("claimed", "partial", "completed", "failed")
ROLLBACK_STATES = ("interrupted", "restoring", "done")

# 历史版本（v1）默认主键字段；v2 复验补齐的主键也写入这里
DEFAULT_PRIMARY_KEY = "record_id"


class SchemaRefreshError(ValueError):
    """业务规则违例，路由层转成 409。"""


class SchemaRefreshCoordinator:
    # ==================================================================
    # 基础工具：与通用模块表同步，保留历史版本兼容
    # ==================================================================
    def _sync_table(self, module: str, row: dict[str, Any]) -> dict[str, Any]:
        rows = store.rows(module)
        existed = next((r for r in rows if r.get("协同引用") == row["协同引用"]), None)
        if existed is not None:
            existed.update(row)
            return existed
        row = dict(row)
        row.setdefault("id", next_id(rows))
        rows.append(row)
        return row

    def _snapshot_endpoint(self, endpoint: str) -> dict[str, Any]:
        view = refresh_store.ensure_endpoint(endpoint)
        return {
            "endpoint": endpoint,
            "active_version": view["active_version"],
            "pending_refresh": view["pending_refresh"],
            "refresh_version": view["refresh_version"],
            "history_versions": list(view["history_versions"]),
            "manifest": [self._partition_brief(refresh_store.partitions[p]) for p in view["manifest"]],
            "todo": [self._partition_brief(refresh_store.partitions[p]) for p in view["todo"]],
        }

    def _partition_brief(self, part: dict[str, Any]) -> dict[str, Any]:
        return {
            "partition_ref": part["partition_ref"],
            "region": part["region"],
            "version_ref": part["version_ref"],
            "state": part["state"],
            "primary_key": part.get("primary_key"),
        }

    def _require_version(self, version_ref: str) -> dict[str, Any]:
        version = refresh_store.field_versions.get(version_ref)
        if version is None:
            raise SchemaRefreshError(f"字段版本 {version_ref} 不存在")
        return version

    def _require_partition(self, partition_ref: str) -> dict[str, Any]:
        part = refresh_store.partitions.get(partition_ref)
        if part is None:
            raise SchemaRefreshError(f"缓存分区 {partition_ref} 不存在")
        return part

    # ==================================================================
    # 步骤 0：准备端点 —— 初始化含东区/西区的 v1 可用缓存与投递待办
    # ==================================================================
    def prepare_endpoint(
        self,
        endpoint: str,
        regions: list[str] | None = None,
        primary_key: str = DEFAULT_PRIMARY_KEY,
    ) -> dict[str, Any]:
        regions = regions or ["东区", "西区"]
        view = refresh_store.ensure_endpoint(endpoint)
        if view["active_version"] is not None:
            return self._snapshot_endpoint(endpoint)

        version_ref = f"{endpoint}#v1"
        version = {
            "version_ref": version_ref,
            "endpoint": endpoint,
            "version_no": "v1",
            "fields": [primary_key, "region", "payload"],
            "primary_key": primary_key,
            "state": "active",
            "supersedes": None,
            "created_at": now_iso(),
        }
        refresh_store.field_versions[version_ref] = version
        view["active_version"] = version_ref
        view["history_versions"].append(version_ref)

        for region in regions:
            self._create_partition(endpoint, version_ref, region, primary_key, valid=True)
        self._rebuild_manifest_and_todo(endpoint)

        # 同步通用模块：缓存策略 + 数据版本快照（历史版本兼容）
        self._sync_table("data_cache", {
            "id": next_id(store.rows("data_cache")),
            "协同引用": f"CACHE::{endpoint}#v1",
            "策略编号": f"CACHE-{endpoint}-V1",
            "目标端点": endpoint,
            "缓存TTL": "3600",
            "缓存键": f"{endpoint}:v1",
            "最大条目": "10000",
            "淘汰算法": "LRU",
            "命中次数": "0",
            "最后刷新": now_iso(),
            "status": "生效中", "pending": False, "abnormal": False,
            "字段版本": version_ref,
        })
        self._sync_table("data_version", {
            "id": next_id(store.rows("data_version")),
            "协同引用": f"VER::{endpoint}#v1",
            "版本编号": f"FIELD-{endpoint}-V1",
            "数据来源": endpoint,
            "行数统计": len(regions),
            "版本标签": "field_version_v1",
            "Schema摘要": f"primary_key={primary_key}",
            "创建原因": "端点初始化可用字段版本",
            "依赖版本": "v0",
            "创建时间": "2026-11-22",
            "status": "可用", "pending": False, "abnormal": False,
            "字段版本": version_ref,
        })
        refresh_store.log_event("endpoint_prepared", {"endpoint": endpoint, "version_ref": version_ref})
        return self._snapshot_endpoint(endpoint)

    def _create_partition(
        self, endpoint: str, version_ref: str, region: str,
        primary_key: str | None, valid: bool,
    ) -> dict[str, Any]:
        partition_ref = f"{endpoint}#{version_ref.split('#')[-1]}:{region}"
        part = {
            "partition_ref": partition_ref,
            "endpoint": endpoint,
            "version_ref": version_ref,
            "region": region,
            "primary_key": primary_key,
            "state": "valid" if valid else "invalid",
            "failure_reason": None if valid else "missing_primary_key",
            "claimed_by": None,
            "created_at": now_iso(),
        }
        refresh_store.partitions[partition_ref] = part
        return part

    def _rebuild_manifest_and_todo(self, endpoint: str) -> None:
        """可用缓存清单与投递待办同步：只收录当前可用版本的 valid 分区。"""
        view = refresh_store.ensure_endpoint(endpoint)
        active = view["active_version"]
        valid_refs = [
            ref for ref, part in refresh_store.partitions.items()
            if part["endpoint"] == endpoint
            and part["version_ref"] == active
            and part["state"] == "valid"
        ]
        view["manifest"] = list(valid_refs)
        # 待办 = 清单中尚未被在途投递领用的分区
        on_the_way = {
            p["partition_ref"]
            for d in refresh_store.deliveries.values()
            if d["endpoint"] == endpoint and d["state"] in ("claimed", "partial")
            for p in d["partitions"]
        }
        view["todo"] = [ref for ref in valid_refs if ref not in on_the_way]

    # ==================================================================
    # 步骤 1：端点发出 Schema 变更 —— 缓存置待刷新，创建 v2 刷新版本
    # ==================================================================
    def mark_schema_change(self, endpoint: str, new_fields: list[str]) -> dict[str, Any]:
        view = refresh_store.ensure_endpoint(endpoint)
        if view["active_version"] is None:
            raise SchemaRefreshError(f"端点 {endpoint} 尚未初始化可用字段版本")
        if view["pending_refresh"]:
            raise SchemaRefreshError(f"端点 {endpoint} 已存在进行中的刷新，不能重复发起")

        old_ref = view["active_version"]
        old_version = refresh_store.field_versions[old_ref]
        new_ref = f"{endpoint}#v2"
        refresh_store.field_versions[new_ref] = {
            "version_ref": new_ref,
            "endpoint": endpoint,
            "version_no": "v2",
            "fields": list(new_fields),
            # 发起时新样本是否带主键未知，先不声明主键
            "primary_key": None,
            "state": "refreshing",
            "supersedes": old_ref,
            "created_at": now_iso(),
        }
        view["pending_refresh"] = True
        view["refresh_version"] = new_ref

        # 同步通用模块：Schema 差异 + 缓存策略置待刷新
        self._sync_table("schema_diff", {
            "id": next_id(store.rows("schema_diff")),
            "协同引用": f"DIFF::{endpoint}#v2",
            "记录编号": f"SCHE-{endpoint}-V2",
            "关联端点": endpoint,
            "变更字段": ",".join(new_fields),
            "旧定义": ",".join(old_version["fields"]),
            "新定义": ",".join(new_fields),
            "差异等级": "MAJOR",
            "影响评估": "端点Schema变更，缓存置待刷新，v1仍为可用版本",
            "检测时间": "2026-11-22",
            "status": "已检测", "pending": True, "abnormal": True,
            "字段版本": new_ref,
        })
        for cache in store.rows("data_cache"):
            if cache.get("目标端点") == endpoint and cache.get("status") == "生效中":
                cache["status"] = "容量告警"
                cache["pending"] = True
                cache["abnormal"] = True
                cache["待刷新版本"] = new_ref
        refresh_store.log_event("schema_changed", {"endpoint": endpoint, "new_version": new_ref})
        return self._snapshot_endpoint(endpoint)

    # ==================================================================
    # 步骤 2：采集样本并校验 —— 缺主键即失败，失败分区不进投递
    # ==================================================================
    def submit_sample(
        self,
        endpoint: str,
        version_ref: str,
        region: str,
        sample: dict[str, Any],
        receipt_ref: str | None = None,
    ) -> dict[str, Any]:
        version = self._require_version(version_ref)
        if version["endpoint"] != endpoint:
            raise SchemaRefreshError("样本端点与字段版本不匹配")
        if version["state"] not in ("refreshing", "withdrawn", "failed"):
            raise SchemaRefreshError(f"版本 {version_ref} 状态 {version['state']} 不接收样本")

        receipt_ref = receipt_ref or f"RCP-{endpoint}-{version['version_no']}-{region}"
        declared_pk = version.get("primary_key")
        missing_pk = declared_pk is None or not str(sample.get(declared_pk, "")).strip()

        receipt = {
            "receipt_ref": receipt_ref,
            "endpoint": endpoint,
            "version_ref": version_ref,
            "region": region,
            "sample": dict(sample),
            "missing_primary_key": missing_pk,
            "state": "rejected" if missing_pk else "pending",
            "received_at": now_iso(),
            "applied": False,
        }
        refresh_store.receipts[receipt_ref] = receipt

        partition_ref = f"{endpoint}#{version['version_no']}:{region}"
        part = refresh_store.partitions.get(partition_ref)
        if part is None:
            part = self._create_partition(
                endpoint, version_ref, region,
                primary_key=declared_pk, valid=False,
            )
        part["state"] = "invalid" if missing_pk else "awaiting_validation"
        part["failure_reason"] = "missing_primary_key" if missing_pk else None
        part["receipt_ref"] = receipt_ref

        # 同步通用模块：抽样计划 + 调用日志（失败样本留痕，便于核对失败字段来源）
        plan_id = next_id(store.rows("sample_check"))
        self._sync_table("sample_check", {
            "id": plan_id,
            "协同引用": f"SAMP::{receipt_ref}",
            "计划编号": f"SAMP-{endpoint}-{version['version_no']}-{region}",
            "目标端点": endpoint,
            "抽样比例": "100%",
            "校验字段": version.get("primary_key") or "(未声明主键)",
            "参考基线": version.get("supersedes") or "",
            "偏差阈值": "primary_key_required",
            "样本数量": 1,
            "最近采样": "2026-11-22",
            "status": "偏差告警" if missing_pk else "采样中",
            "pending": True, "abnormal": missing_pk,
            "字段版本": version_ref,
            "失败原因": part["failure_reason"] or "",
        })
        self._sync_table("call_log", {
            "id": next_id(store.rows("call_log")),
            "协同引用": f"CALL::{receipt_ref}",
            "记录编号": f"CALL-SAMP-{plan_id}",
            "接口端点": endpoint,
            "请求参数": f"sample:{receipt_ref}",
            "响应状态": "422" if missing_pk else "202",
            "响应耗时(ms)": 80,
            "返回行数": 0 if missing_pk else 1,
            "错误信息": "样本缺少主键字段，分区校验失败" if missing_pk else "",
            "调用时间": "2026-11-22",
            "status": "偏差告警" if missing_pk else "采样中",
            "pending": True, "abnormal": missing_pk,
        })
        refresh_store.log_event("sample_submitted", {
            "endpoint": endpoint, "version_ref": version_ref, "region": region,
            "missing_primary_key": missing_pk, "receipt_ref": receipt_ref,
        })
        return {"receipt": receipt, "partition": self._partition_brief(part)}

    # ==================================================================
    # 步骤 3：校验回执处理 —— 迟到回执不能让已撤回刷新自行生效
    # ==================================================================
    def handle_receipt(self, receipt_ref: str, accepted: bool) -> dict[str, Any]:
        receipt = refresh_store.receipts.get(receipt_ref)
        if receipt is None:
            raise SchemaRefreshError(f"校验回执 {receipt_ref} 不存在")
        version = self._require_version(receipt["version_ref"])

        # 刷新已撤回（回滚完成）：任何迟到回执都只能被忽略，不能自行生效
        if version["state"] == "withdrawn":
            receipt["state"] = "ignored"
            receipt["applied"] = False
            receipt["ignored_reason"] = "version_withdrawn"
            refresh_store.log_event("late_receipt_ignored", {
                "receipt_ref": receipt_ref, "version_ref": version["version_ref"],
            })
            return {"receipt": receipt, "applied": False,
                    "message": "刷新已撤回，迟到回执已忽略，缓存保持回滚后的可用状态"}

        if receipt["state"] == "rejected":
            # 缺主键样本的回执永远不接受
            receipt["applied"] = False
            return {"receipt": receipt, "applied": False,
                    "message": "样本缺少主键，回执不能通过校验"}

        receipt["state"] = "accepted" if accepted else "rejected"
        partition_ref = f"{receipt['endpoint']}#{version['version_no']}:{receipt['region']}"
        part = self._require_partition(partition_ref)
        if accepted:
            part["state"] = "valid"
            part["failure_reason"] = None
            receipt["applied"] = True
            # 同版本所有分区均 valid 才能整体切换为可用版本
            if self._version_all_partitions_valid(version["version_ref"]):
                self._activate_version(version)
        else:
            part["state"] = "invalid"
            part["failure_reason"] = "validation_rejected"
            receipt["applied"] = False
        self._rebuild_manifest_and_todo(receipt["endpoint"])
        refresh_store.log_event("receipt_handled", {
            "receipt_ref": receipt_ref, "accepted": accepted, "applied": receipt["applied"],
        })
        return {"receipt": receipt, "partition": self._partition_brief(part),
                "applied": receipt["applied"]}

    def _version_all_partitions_valid(self, version_ref: str) -> bool:
        parts = [p for p in refresh_store.partitions.values() if p["version_ref"] == version_ref]
        return bool(parts) and all(p["state"] == "valid" for p in parts)

    def _activate_version(self, version: dict[str, Any]) -> None:
        endpoint = version["endpoint"]
        view = refresh_store.ensure_endpoint(endpoint)
        old_ref = view["active_version"]
        version["state"] = "active"
        view["active_version"] = version["version_ref"]
        view["pending_refresh"] = False
        view["refresh_version"] = None
        if version["version_ref"] not in view["history_versions"]:
            view["history_versions"].append(version["version_ref"])
        # 旧版本分区保留（历史版本兼容 + 在途投递补投），但退出清单
        self._rebuild_manifest_and_todo(endpoint)
        for cache in store.rows("data_cache"):
            if cache.get("目标端点") == endpoint:
                cache["status"] = "生效中"
                cache["pending"] = False
                cache["abnormal"] = False
                cache["字段版本"] = version["version_ref"]
                cache.pop("待刷新版本", None)
        refresh_store.log_event("version_activated", {
            "endpoint": endpoint, "version_ref": version["version_ref"], "old_ref": old_ref,
        })

    # ==================================================================
    # 步骤 4：投递领用 —— 一笔投递的所有分区必须同一字段版本
    # ==================================================================
    def claim_delivery(
        self, endpoint: str, subscriber: str,
        version_ref: str | None = None, regions: list[str] | None = None,
    ) -> dict[str, Any]:
        view = refresh_store.ensure_endpoint(endpoint)
        target_ref = version_ref or view["active_version"]
        if target_ref is None:
            raise SchemaRefreshError(f"端点 {endpoint} 没有可用字段版本")
        target = self._require_version(target_ref)
        if target["state"] != "active":
            raise SchemaRefreshError(
                f"字段版本 {target_ref} 状态为 {target['state']}，不可领用；"
                "字段校验失败时投递恢复到有效分区所在版本")

        candidates = [
            p for ref, p in refresh_store.partitions.items()
            if p["endpoint"] == endpoint
            and p["version_ref"] == target_ref
            and p["state"] == "valid"
            and p["claimed_by"] is None
            and (regions is None or p["region"] in regions)
        ]
        if not candidates:
            raise SchemaRefreshError(f"版本 {target_ref} 没有可领用的有效分区")

        delivery_ref = f"DELIV-{endpoint}-{len(refresh_store.deliveries) + 1}"
        delivery = {
            "delivery_ref": delivery_ref,
            "endpoint": endpoint,
            "subscriber": subscriber,
            "version_ref": target_ref,
            "primary_key": target.get("primary_key"),
            "partitions": [],
            "state": "claimed",
            "snapshot_ref": None,
            "created_at": now_iso(),
        }
        for part in candidates:
            part["claimed_by"] = delivery_ref
            delivery["partitions"].append({
                "partition_ref": part["partition_ref"],
                "region": part["region"],
                "state": "claimed",
                "sent_at": None,
            })
        refresh_store.deliveries[delivery_ref] = delivery
        self._rebuild_manifest_and_todo(endpoint)

        # 同步通用模块：投递管道待办
        self._sync_table("data_delivery", {
            "id": next_id(store.rows("data_delivery")),
            "协同引用": f"DELIV::{delivery_ref}",
            "管道编号": f"PIPE-{delivery_ref}",
            "数据来源": endpoint,
            "目标地址": f"subscriber://{subscriber}",
            "投递格式": "JSON",
            "分区策略": "|".join(p["partition_ref"] for p in delivery["partitions"]),
            "投递频率": "on_demand",
            "压缩方式": "snappy",
            "最后投递": "",
            "status": "投递中", "pending": True, "abnormal": False,
            "字段版本": target_ref,
            "订阅方": subscriber,
        })
        refresh_store.log_event("delivery_claimed", {
            "delivery_ref": delivery_ref, "version_ref": target_ref,
            "partitions": [p["partition_ref"] for p in delivery["partitions"]],
        })
        return {"delivery_ref": delivery_ref, "version_ref": target_ref,
                "partitions": delivery["partitions"], "state": delivery["state"]}

    def send_partition(self, delivery_ref: str, region: str) -> dict[str, Any]:
        delivery = self._require_delivery(delivery_ref)
        if delivery["state"] not in ("claimed", "partial"):
            raise SchemaRefreshError(f"投递单 {delivery_ref} 状态 {delivery['state']} 不可发送")
        item = next((p for p in delivery["partitions"] if p["region"] == region), None)
        if item is None:
            raise SchemaRefreshError(f"投递单 {delivery_ref} 不含分区 {region}")
        if item["state"] == "sent":
            return {"delivery_ref": delivery_ref, "region": region, "duplicate": True}
        item["state"] = "sent"
        item["sent_at"] = now_iso()
        delivery["state"] = "partial"
        refresh_store.log_event("partition_sent", {
            "delivery_ref": delivery_ref, "region": region,
            "partition_ref": item["partition_ref"], "version_ref": delivery["version_ref"],
        })
        return {"delivery_ref": delivery_ref, "region": region,
                "partition_ref": item["partition_ref"], "state": "sent"}

    def pause_delivery(self, delivery_ref: str) -> dict[str, Any]:
        delivery = self._require_delivery(delivery_ref)
        delivery["state"] = "partial"
        refresh_store.log_event("delivery_paused", {"delivery_ref": delivery_ref})
        return {"delivery_ref": delivery_ref, "state": delivery["state"],
                "sent": [p["region"] for p in delivery["partitions"] if p["state"] == "sent"],
                "pending": [p["region"] for p in delivery["partitions"] if p["state"] != "sent"]}

    def _require_delivery(self, delivery_ref: str) -> dict[str, Any]:
        delivery = refresh_store.deliveries.get(delivery_ref)
        if delivery is None:
            raise SchemaRefreshError(f"投递单 {delivery_ref} 不存在")
        return delivery

    # ==================================================================
    # 步骤 5：回滚 —— 可中断、幂等重试，恢复刷新前可用状态
    # ==================================================================
    def request_rollback(
        self, endpoint: str, reason: str = "validation_failed",
        interrupt: bool = False,
    ) -> dict[str, Any]:
        view = refresh_store.ensure_endpoint(endpoint)
        if not view["pending_refresh"] or view["refresh_version"] is None:
            raise SchemaRefreshError(f"端点 {endpoint} 没有进行中的刷新，无需回滚")
        new_ref = view["refresh_version"]

        rollback_ref = f"RB-{endpoint}-{len(refresh_store.rollbacks) + 1}"
        rollback = refresh_store.rollbacks.get(rollback_ref)
        if rollback is None:
            rollback = {
                "rollback_ref": rollback_ref,
                "endpoint": endpoint,
                "refresh_version": new_ref,
                "restore_version": view["active_version"],
                "reason": reason,
                "state": "interrupted" if interrupt else "restoring",
                "steps_done": [],
                "created_at": now_iso(),
            }
            refresh_store.rollbacks[rollback_ref] = rollback
            refresh_store.log_event("rollback_requested", {
                "rollback_ref": rollback_ref, "endpoint": endpoint,
                "refresh_version": new_ref, "interrupt": interrupt,
            })
            # 同步通用模块：重放队列工单
            self._sync_table("retry_queue", {
                "id": next_id(store.rows("retry_queue")),
                "协同引用": f"RB::{rollback_ref}",
                "任务编号": f"RETR-{rollback_ref}",
                "失败端点": endpoint,
                "失败原因": reason,
                "重试次数": 0,
                "最大重试": 3,
                "退避策略": "linear:5s",
                "最后重试": "",
                "队列状态": "rollback_interrupted" if interrupt else "rollback_running",
                "status": "待重试" if interrupt else "重试中",
                "pending": True, "abnormal": True,
            })
        if interrupt:
            return {"rollback_ref": rollback_ref, "state": rollback["state"],
                    "message": "回滚已中断，等待重试恢复"}
        return self.resume_rollback(rollback_ref)

    def resume_rollback(self, rollback_ref: str) -> dict[str, Any]:
        """幂等恢复：按步骤推进，已完成的步骤不重复执行。"""
        rollback = refresh_store.rollbacks.get(rollback_ref)
        if rollback is None:
            raise SchemaRefreshError(f"回滚工单 {rollback_ref} 不存在")
        if rollback["state"] == "done":
            return {"rollback_ref": rollback_ref, "state": "done", "idempotent": True,
                    "steps_done": list(rollback["steps_done"])}

        rollback["state"] = "restoring"
        endpoint = rollback["endpoint"]
        new_ref = rollback["refresh_version"]
        view = refresh_store.ensure_endpoint(endpoint)

        # 步骤 1：撤回刷新版本及其分区（缺主键/未通过校验的分区不得进入投递）
        if "withdraw_refresh_version" not in rollback["steps_done"]:
            version = refresh_store.field_versions.get(new_ref)
            if version is not None:
                version["state"] = "withdrawn"
            for part in refresh_store.partitions.values():
                if part["version_ref"] == new_ref and part["state"] != "withdrawn":
                    part["state"] = "withdrawn"
                    part["failure_reason"] = part["failure_reason"] or "rollback_withdrawn"
                    # 被领用的刷新分区释放（本场景 v2 分区未领用，仍做防御处理）
                    part["claimed_by"] = None
            # 所有在途 / 迟到的 v2 回执标记为未生效，后续迟到回执直接 ignored
            for receipt in refresh_store.receipts.values():
                if receipt["version_ref"] == new_ref and receipt["state"] == "pending":
                    receipt["state"] = "ignored"
                    receipt["ignored_reason"] = "version_withdrawn"
            rollback["steps_done"].append("withdraw_refresh_version")

        # 步骤 2：缓存恢复原可用版本（v1 分区保持 valid，已领用关系不动）
        if "restore_cache_manifest" not in rollback["steps_done"]:
            view["pending_refresh"] = False
            view["refresh_version"] = None
            view["active_version"] = rollback["restore_version"]
            self._rebuild_manifest_and_todo(endpoint)
            for cache in store.rows("data_cache"):
                if cache.get("目标端点") == endpoint:
                    cache["status"] = "生效中"
                    cache["pending"] = False
                    cache["abnormal"] = False
                    cache["字段版本"] = rollback["restore_version"]
                    cache.pop("待刷新版本", None)
            rollback["steps_done"].append("restore_cache_manifest")

        # 步骤 3：回滚工单收尾（审计留痕）
        if "close_rollback" not in rollback["steps_done"]:
            rollback["state"] = "done"
            rollback["steps_done"].append("close_rollback")
            for task in store.rows("retry_queue"):
                if task.get("协同引用") == f"RB::{rollback_ref}":
                    task["status"] = "已成功"
                    task["pending"] = False
                    task["abnormal"] = False
                    task["队列状态"] = "rollback_done"
                    task["最后重试"] = "2026-11-22"
                    task["重试次数"] = int(task.get("重试次数", 0)) + 1
            self._sync_table("pipeline_audit", {
                "id": next_id(store.rows("pipeline_audit")),
                "协同引用": f"AUDIT::{rollback_ref}",
                "记录编号": f"PIPE-{rollback_ref}",
                "管道名称": f"schema_refresh:{endpoint}",
                "操作人": "system",
                "操作类型": "回滚",
                "变更摘要": f"撤回{new_ref}，缓存恢复{rollback['restore_version']}",
                "操作时间": "2026-11-22",
                "来源节点": "schema_refresh",
                "审计结论": "已恢复原可用状态",
                "status": "已记录", "pending": False, "abnormal": False,
            })
            refresh_store.log_event("rollback_done", {
                "rollback_ref": rollback_ref, "endpoint": endpoint,
                "restore_version": rollback["restore_version"],
            })

        return {"rollback_ref": rollback_ref, "state": rollback["state"],
                "steps_done": list(rollback["steps_done"]),
                "snapshot": self._snapshot_endpoint(endpoint)}

    # ==================================================================
    # 步骤 6：补齐主键并复验 —— 判断是否需要重新采集
    # ==================================================================
    def revalidate(
        self, endpoint: str, version_ref: str, region: str,
        fixed_sample: dict[str, Any],
    ) -> dict[str, Any]:
        version = self._require_version(version_ref)
        if version["state"] != "withdrawn":
            raise SchemaRefreshError(
                f"版本 {version_ref} 状态 {version['state']}，仅已撤回版本可补齐主键复验")
        if not str(fixed_sample.get(DEFAULT_PRIMARY_KEY, "")).strip():
            raise SchemaRefreshError("复验样本仍缺少主键，不能发起复验")

        old_receipts = [
            r for r in refresh_store.receipts.values()
            if r["version_ref"] == version_ref and r["region"] == region
        ]
        # 原样本缺主键，无法靠回执补救，必须重新采集；这里以 fixed_sample 作为重新采集样本
        need_recollect = any(r.get("missing_primary_key") for r in old_receipts)

        version["state"] = "refreshing"
        version["primary_key"] = DEFAULT_PRIMARY_KEY
        if DEFAULT_PRIMARY_KEY not in version["fields"]:
            version["fields"].insert(0, DEFAULT_PRIMARY_KEY)
        view = refresh_store.ensure_endpoint(endpoint)
        view["pending_refresh"] = True
        view["refresh_version"] = version_ref

        result = self.submit_sample(
            endpoint, version_ref, region, fixed_sample,
            receipt_ref=f"RCP-{endpoint}-{version['version_no']}-{region}-RETRY",
        )
        receipt = result["receipt"]
        # 补齐主键后样本自身完整，直接出具通过回执并激活（全部分区 valid 时）
        handled = self.handle_receipt(receipt["receipt_ref"], accepted=True)
        refresh_store.log_event("revalidated", {
            "endpoint": endpoint, "version_ref": version_ref, "region": region,
            "need_recollect": need_recollect,
        })
        return {
            "version_ref": version_ref,
            "region": region,
            "need_recollect": need_recollect,
            "reason": "原样本缺少主键，须重新采集带主键样本后复验" if need_recollect
                      else "原样本可复用，仅需重发校验回执",
            "receipt": handled["receipt"],
            "partition": handled.get("partition"),
            "version_state": version["state"],
        }

    # ==================================================================
    # 步骤 7：完成投递 —— 按可用缓存版本取实际分区并写快照
    # ==================================================================
    def complete_delivery(self, delivery_ref: str) -> dict[str, Any]:
        delivery = self._require_delivery(delivery_ref)
        endpoint = delivery["endpoint"]
        view = refresh_store.ensure_endpoint(endpoint)

        # 版本一致性最终防线：所有分区必须来自同一字段版本
        version_refs = {
            self._require_partition(p["partition_ref"])["version_ref"]
            for p in delivery["partitions"]
        }
        if len(version_refs) != 1 or next(iter(version_refs)) != delivery["version_ref"]:
            raise SchemaRefreshError("一笔投递的所有分区必须来自同一个字段版本")
        for item in delivery["partitions"]:
            part = self._require_partition(item["partition_ref"])
            if part["state"] != "valid":
                raise SchemaRefreshError(f"分区 {item['partition_ref']} 未通过校验，不能投递")
            if item["state"] != "sent":
                item["state"] = "sent"
                item["sent_at"] = now_iso()

        delivery["state"] = "completed"
        snapshot_ref = f"SNAP-{delivery_ref}"
        snapshot = {
            "snapshot_ref": snapshot_ref,
            "endpoint": endpoint,
            "subscriber": delivery["subscriber"],
            "field_version": delivery["version_ref"],
            "primary_key": delivery["primary_key"],
            "partitions": [
                {
                    "partition_ref": p["partition_ref"],
                    "region": p["region"],
                    "sent_at": p["sent_at"],
                }
                for p in delivery["partitions"]
            ],
            "row_count": len(delivery["partitions"]),
            "written_at": now_iso(),
        }
        delivery["snapshot_ref"] = snapshot_ref
        self._rebuild_manifest_and_todo(endpoint)

        # 同步通用模块：数据版本快照 + 投递管道完成 + 交付记录（保留字段与分区）
        self._sync_table("data_version", {
            "id": next_id(store.rows("data_version")),
            "协同引用": f"SNAP::{snapshot_ref}",
            "版本编号": f"DATA-{snapshot_ref}",
            "数据来源": endpoint,
            "行数统计": snapshot["row_count"],
            "版本标签": "delivery_snapshot",
            "Schema摘要": f"field_version={delivery['version_ref']};pk={delivery['primary_key']}",
            "创建原因": f"投递单{delivery_ref}完成，订阅方{delivery['subscriber']}",
            "依赖版本": delivery["version_ref"],
            "创建时间": "2026-11-22",
            "status": "可用", "pending": False, "abnormal": False,
            "字段版本": delivery["version_ref"],
            "分区清单": "|".join(p["partition_ref"] for p in snapshot["partitions"]),
            "订阅方": delivery["subscriber"],
        })
        for row in store.rows("data_delivery"):
            if row.get("协同引用") == f"DELIV::{delivery_ref}":
                row["status"] = "已完成"
                row["pending"] = False
                row["最后投递"] = "2026-11-22"
                row["分区策略"] = "|".join(p["partition_ref"] for p in snapshot["partitions"])
        refresh_store.log_event("delivery_completed", {
            "delivery_ref": delivery_ref, "snapshot_ref": snapshot_ref,
            "field_version": delivery["version_ref"],
        })
        return {"delivery_ref": delivery_ref, "state": "completed", "snapshot": snapshot}

    # ==================================================================
    # 查询：可用清单 / 待办同步 / 交付记录 / 事件流
    # ==================================================================
    def get_manifest(self, endpoint: str) -> dict[str, Any]:
        view = refresh_store.ensure_endpoint(endpoint)
        return {
            "endpoint": endpoint,
            "active_version": view["active_version"],
            "pending_refresh": view["pending_refresh"],
            "manifest": [self._partition_brief(refresh_store.partitions[r]) for r in view["manifest"]],
            "todo": [self._partition_brief(refresh_store.partitions[r]) for r in view["todo"]],
            "in_sync": self._manifest_todo_in_sync(endpoint),
            "history_versions": list(view["history_versions"]),
        }

    def _manifest_todo_in_sync(self, endpoint: str) -> bool:
        view = refresh_store.ensure_endpoint(endpoint)
        on_the_way = {
            p["partition_ref"]
            for d in refresh_store.deliveries.values()
            if d["endpoint"] == endpoint and d["state"] in ("claimed", "partial")
            for p in d["partitions"]
        }
        expect_todo = [ref for ref in view["manifest"] if ref not in on_the_way]
        return expect_todo == view["todo"]

    def get_delivery_record(self, delivery_ref: str) -> dict[str, Any]:
        delivery = self._require_delivery(delivery_ref)
        return copy.deepcopy(delivery)

    def list_deliveries(self, endpoint: str | None = None) -> list[dict[str, Any]]:
        return [
            copy.deepcopy(d) for d in refresh_store.deliveries.values()
            if endpoint is None or d["endpoint"] == endpoint
        ]

    def list_rollbacks(self, endpoint: str | None = None) -> list[dict[str, Any]]:
        return [
            copy.deepcopy(r) for r in refresh_store.rollbacks.values()
            if endpoint is None or r["endpoint"] == endpoint
        ]

    def event_log(self) -> list[dict[str, Any]]:
        return list(refresh_store.events)


coordinator = SchemaRefreshCoordinator()
