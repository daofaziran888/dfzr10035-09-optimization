"""Schema 刷新 / 缓存版本 / 投递恢复协同 —— 全链路场景核对脚本。

覆盖题述完整剧情：
1. 领用含东区、西区的 v1，发出东区后暂停；
2. 以缺主键样本刷新 v2 西区，校验失败分区不进投递；
3. 回滚中断后重试，缓存恢复原可用状态；
4. 迟到的 v2 回执不能让已撤回刷新自行生效；
5. 已领用 v1 仍按原分区补投西区；
6. 补齐主键发起复验，核对是否需要重新采集；
7. 完成新投递并检查快照；同时核对清单/待办同步、历史版本兼容与交付记录。

直接运行：.venv/bin/python scripts/schema_refresh_demo.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.schema_refresh_state import refresh_store
from app.services.schema_refresh import SchemaRefreshError, coordinator
from app.store import store

ENDPOINT = "ENDP-DEMO-0001"
SUBSCRIBER = "SUBS-DEMO-东区西区订阅"

passed: list[str] = []
failed: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        passed.append(name)
        print(f"  ✅ {name} {detail}")
    else:
        failed.append(f"{name} {detail}")
        print(f"  ❌ {name} {detail}")


def section(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> int:
    # ---- 步骤 0：初始化含东区/西区的 v1 可用缓存 ----------------------
    section("步骤0：初始化端点，v1 含东区/西区两个有效分区")
    snap = coordinator.prepare_endpoint(ENDPOINT)
    check("v1 为当前可用字段版本", snap["active_version"] == f"{ENDPOINT}#v1")
    check("可用清单含东区/西区",
          [p["region"] for p in snap["manifest"]] == ["东区", "西区"])
    check("清单与投递待办初始同步", snap["manifest"] and snap["todo"] == snap["manifest"])

    # ---- 步骤 1：领用 v1 并发出东区后暂停 -----------------------------
    section("步骤1：领用 v1 投递（东区+西区），发出东区后暂停")
    claim = coordinator.claim_delivery(ENDPOINT, SUBSCRIBER)
    d1 = claim["delivery_ref"]
    check("领用版本为 v1", claim["version_ref"] == f"{ENDPOINT}#v1")
    check("一笔投递领用同一版本的两个分区",
          len({p["partition_ref"].rsplit(":", 1)[0] for p in claim["partitions"]}) == 1)
    coordinator.send_partition(d1, "东区")
    paused = coordinator.pause_delivery(d1)
    check("暂停后投递为 partial，东区已发/西区待发",
          paused["sent"] == ["东区"] and paused["pending"] == ["西区"])
    manifest = coordinator.get_manifest(ENDPOINT)
    check("领用后待办收缩但清单不变（清单/待办同步）",
          manifest["todo"] == [] and len(manifest["manifest"]) == 2
          and manifest["in_sync"] is True)

    # ---- 步骤 2：Schema 变更，缓存置待刷新，v1 仍可用 ------------------
    section("步骤2：端点发出 Schema 变更，v2 开始刷新，缓存置待刷新")
    changed = coordinator.mark_schema_change(
        ENDPOINT, ["region", "payload", "event_time"])
    check("缓存进入待刷新", changed["pending_refresh"] is True
          and changed["refresh_version"] == f"{ENDPOINT}#v2")
    check("待刷新期间 v1 仍是可用版本", changed["active_version"] == f"{ENDPOINT}#v1")
    cache_rows = [c for c in store.rows("data_cache") if c.get("目标端点") == ENDPOINT]
    check("通用缓存表同步为待刷新状态",
          any(c.get("待刷新版本") == f"{ENDPOINT}#v2" and c["status"] == "容量告警"
              for c in cache_rows))

    # ---- 步骤 3：v2 西区样本缺主键，校验失败 ---------------------------
    section("步骤3：提交缺主键的 v2 西区样本，字段校验失败")
    sample_result = coordinator.submit_sample(
        ENDPOINT, f"{ENDPOINT}#v2", "西区",
        {"region": "西区", "payload": "no-primary-key"})
    bad_receipt = sample_result["receipt"]["receipt_ref"]
    check("缺主键样本被拒", sample_result["receipt"]["state"] == "rejected"
          and sample_result["receipt"]["missing_primary_key"] is True)
    check("对应分区判定 invalid", sample_result["partition"]["state"] == "invalid")
    manifest = coordinator.get_manifest(ENDPOINT)
    check("失败分区不进入可用清单",
          all(p["state"] == "valid" and p["version_ref"] == f"{ENDPOINT}#v1"
              for p in manifest["manifest"]))
    check("失败分区不进入投递待办",
          all(p["state"] != "invalid" for p in manifest["todo"]))

    # 尝试直接领用刷新中的 v2 —— 必须被拒，投递只能回到有效分区版本
    try:
        coordinator.claim_delivery(ENDPOINT, SUBSCRIBER, version_ref=f"{ENDPOINT}#v2")
        check("刷新中/校验失败的 v2 不可领用", False)
    except SchemaRefreshError as exc:
        check("刷新中/校验失败的 v2 不可领用，提示恢复有效版本", True, f"（{exc}）")

    # ---- 步骤 4：回滚中断，再重试恢复 ----------------------------------
    section("步骤4：回滚先中断，再幂等重试，缓存恢复原可用状态")
    interrupted = coordinator.request_rollback(ENDPOINT, interrupt=True)
    check("回滚工单落为中断态", interrupted["state"] == "interrupted")
    resumed = coordinator.resume_rollback(interrupted["rollback_ref"])
    check("重试后回滚完成", resumed["state"] == "done")
    check("回滚步骤含撤回新版本/恢复清单/收尾",
          resumed["steps_done"] == [
              "withdraw_refresh_version", "restore_cache_manifest", "close_rollback"])
    # 幂等：再次重试不重复执行
    again = coordinator.resume_rollback(interrupted["rollback_ref"])
    check("回滚重试幂等", again.get("idempotent") is True and again["state"] == "done")

    snap = coordinator.get_manifest(ENDPOINT)
    check("缓存恢复 v1 可用、待刷新标记清除",
          snap["active_version"] == f"{ENDPOINT}#v1" and snap["pending_refresh"] is False)
    check("可用清单恢复为 v1 东区/西区",
          [(p["version_ref"], p["state"]) for p in snap["manifest"]] == [
              (f"{ENDPOINT}#v1", "valid"), (f"{ENDPOINT}#v1", "valid")])
    v2 = refresh_store.field_versions[f"{ENDPOINT}#v2"]
    check("v2 已撤回", v2["state"] == "withdrawn")
    v2_west = refresh_store.partitions[f"{ENDPOINT}#v2:西区"]
    check("v2 缺主键分区保持 withdrawn，不随刷新混入投递",
          v2_west["state"] == "withdrawn")

    # ---- 步骤 5：迟到的 v2 校验回执 ------------------------------------
    section("步骤5：迟到的 v2 校验回执到达，不能让撤回的刷新自行生效")
    late = coordinator.handle_receipt(bad_receipt, accepted=True)
    check("迟到回执被忽略且未生效",
          late["applied"] is False and late["receipt"]["state"] == "ignored")
    check("v2 仍为 withdrawn", refresh_store.field_versions[f"{ENDPOINT}#v2"]["state"]
          == "withdrawn")
    snap = coordinator.get_manifest(ENDPOINT)
    check("可用清单未被迟到回执改变",
          [p["version_ref"] for p in snap["manifest"]] == [f"{ENDPOINT}#v1"] * 2)

    # ---- 步骤 6：已领用 v1 按原分区补投西区 ----------------------------
    section("步骤6：已领用的 v1 投递按原分区补投西区")
    supplement = coordinator.send_partition(d1, "西区")
    check("补投分区仍是 v1 西区",
          supplement["partition_ref"] == f"{ENDPOINT}#v1:西区"
          and supplement["state"] == "sent")
    d1_record = coordinator.get_delivery_record(d1)
    first_rows = [(p["region"], p["partition_ref"], p["state"]) for p in d1_record["partitions"]]
    check("第一次补投/已发行全部来自 v1",
          all(ref.startswith(f"{ENDPOINT}#v1:") for _, ref, _ in first_rows),
      str(first_rows))

    # 失败字段来源核对：抽样计划与调用日志均留痕
    samp_rows = [s for s in store.rows("sample_check")
                 if s.get("协同引用") == f"SAMP::{bad_receipt}"]
    check("失败字段来源可追溯到缺主键抽样记录",
          bool(samp_rows) and samp_rows[0]["失败原因"] == "missing_primary_key"
          and samp_rows[0]["status"] == "偏差告警")
    log_rows = [l for l in store.rows("call_log")
                if l.get("协同引用") == f"CALL::{bad_receipt}"]
    check("失败样本写入调用日志（422）",
          bool(log_rows) and log_rows[0]["响应状态"] == "422")

    # ---- 步骤 7：补齐主键并发起复验 ------------------------------------
    section("步骤7：补齐主键后对 v2 西区发起复验")
    reval = coordinator.revalidate(
        ENDPOINT, f"{ENDPOINT}#v2", "西区",
        {"record_id": "PK-1001", "region": "西区", "payload": "fixed", "event_time": "2026-11-22"})
    check("原样本缺主键，复验必须重新采集",
          reval["need_recollect"] is True, str(reval["reason"]))
    check("复验回执通过且生效",
          reval["receipt"]["state"] == "accepted" and reval["receipt"]["applied"] is True)
    check("复验后 v2 西区分区恢复 valid", reval["partition"]["state"] == "valid")
    check("v2 全分区通过后重新激活", reval["version_state"] == "active")
    snap = coordinator.get_manifest(ENDPOINT)
    check("v1 保留在历史版本（兼容历史版本）",
          f"{ENDPOINT}#v1" in snap["history_versions"]
          and f"{ENDPOINT}#v2" in snap["history_versions"])

    # ---- 步骤 8：完成 v1 老投递，再完成一笔 v2 新投递并检查快照 --------
    section("步骤8：完成 v1 老投递与 v2 新投递，核对快照与交付记录")
    completed_d1 = coordinator.complete_delivery(d1)
    check("v1 老投递完成并写快照",
          completed_d1["snapshot"]["field_version"] == f"{ENDPOINT}#v1"
          and {p["region"] for p in completed_d1["snapshot"]["partitions"]} == {"东区", "西区"})
    check("快照保留主键与分区明细",
          completed_d1["snapshot"]["primary_key"] == "record_id"
          and len(completed_d1["snapshot"]["partitions"]) == 2)

    d2 = coordinator.claim_delivery(ENDPOINT, "SUBS-DEMO-新订阅")
    check("新投递按当前可用版本 v2 取实际分区",
          d2["version_ref"] == f"{ENDPOINT}#v2"
          and [p["region"] for p in d2["partitions"]] == ["西区"])
    completed_d2 = coordinator.complete_delivery(d2["delivery_ref"])
    snap2 = completed_d2["snapshot"]
    check("新投递快照为 v2 且仅含有效西区分区",
          snap2["field_version"] == f"{ENDPOINT}#v2"
          and snap2["partitions"][0]["partition_ref"] == f"{ENDPOINT}#v2:西区"
          and snap2["row_count"] == 1)

    # 数据版本表中可核对数据发给了哪个订阅、用了哪些字段与分区
    version_rows = [v for v in store.rows("data_version")
                    if v.get("协同引用") in (
                        f"SNAP::{completed_d1['snapshot']['snapshot_ref']}",
                        f"SNAP::{snap2['snapshot_ref']}")]
    check("交付记录保留字段版本/分区/订阅方",
          len(version_rows) == 2
          and all(v.get("字段版本") and v.get("分区清单") and v.get("订阅方")
                  for v in version_rows))

    # ---- 步骤 9：最终一致性核对 ----------------------------------------
    section("步骤9：清单/待办同步与跨模块留痕总检")
    manifest = coordinator.get_manifest(ENDPOINT)
    check("最终清单与待办同步", manifest["in_sync"] is True
          and [p["partition_ref"] for p in manifest["todo"]] == [f"{ENDPOINT}#v2:西区"])
    check("回滚工单与重放队列留痕",
          any(r.get("队列状态") == "rollback_done" and r["status"] == "已成功"
              for r in store.rows("retry_queue") if r.get("失败端点") == ENDPOINT))
    check("管道审计记录回滚恢复",
          any(a.get("操作类型") == "回滚" and ENDPOINT in a.get("管道名称", "")
              for a in store.rows("pipeline_audit")))
    check("Schema 差异记录保留变更轨迹",
          any(d.get("协同引用") == f"DIFF::{ENDPOINT}#v2" for d in store.rows("schema_diff")))
    check("两笔投递单均保留字段版本与分区",
          all(d.get("字段版本") and d.get("分区策略") for d in store.rows("data_delivery")
              if d.get("数据来源") == ENDPOINT))

    print("\n" + "#" * 72)
    print(f"通过 {len(passed)} 项，失败 {len(failed)} 项")
    if failed:
        for item in failed:
            print("  - " + item)
        return 1
    print("全部场景核对通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
