"""HTTP 层端到端冒烟：通过真实 uvicorn 服务走一遍协同工作流接口。"""
from __future__ import annotations

import json
import urllib.request

BASE = "http://127.0.0.1:8011/api/schema-refresh"
ENDPOINT = "ENDP-HTTP-0001"


def call(method: str, path: str, payload: dict | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def main() -> int:
    s, r = call("POST", "/endpoints/prepare", {"endpoint": ENDPOINT})
    assert s == 200 and r["data"]["active_version"] == f"{ENDPOINT}#v1", r
    print("prepare OK:", r["data"]["active_version"])

    s, r = call("POST", "/deliveries/claim",
                {"endpoint": ENDPOINT, "subscriber": "SUBS-HTTP"})
    assert s == 200
    d1 = r["data"]["delivery_ref"]
    print("claim OK:", d1, [p["region"] for p in r["data"]["partitions"]])

    s, r = call("POST", "/deliveries/send-partition",
                {"delivery_ref": d1, "region": "东区"})
    assert s == 200 and r["data"]["state"] == "sent", r
    print("send 东区 OK")

    s, r = call("POST", "/schema-change",
                {"endpoint": ENDPOINT, "new_fields": ["region", "payload"]})
    assert s == 200 and r["data"]["pending_refresh"] is True, r
    print("schema-change OK, pending refresh")

    s, r = call("POST", "/samples", {
        "endpoint": ENDPOINT, "version_ref": f"{ENDPOINT}#v2",
        "region": "西区", "sample": {"region": "西区", "payload": "x"}})
    assert s == 200 and r["data"]["receipt"]["state"] == "rejected", r
    receipt = r["data"]["receipt"]["receipt_ref"]
    print("missing-pk sample rejected:", receipt)

    # 业务违例应返回 409
    s, r = call("POST", "/deliveries/claim",
                {"endpoint": ENDPOINT, "subscriber": "SUBS-HTTP",
                 "version_ref": f"{ENDPOINT}#v2"})
    assert s == 409, (s, r)
    print("claim invalid v2 -> 409:", r["detail"][:40], "...")

    s, r = call("POST", "/rollbacks/request",
                {"endpoint": ENDPOINT, "interrupt": True})
    assert s == 200 and r["data"]["state"] == "interrupted", r
    rb = r["data"]["rollback_ref"]
    print("rollback interrupted:", rb)

    s, r = call("POST", "/rollbacks/resume", {"rollback_ref": rb})
    assert s == 200 and r["data"]["state"] == "done", r
    print("rollback resumed, steps:", r["data"]["steps_done"])

    s, r = call("POST", "/receipts/handle",
                {"receipt_ref": receipt, "accepted": True})
    assert s == 200 and r["data"]["applied"] is False
    assert r["data"]["receipt"]["state"] == "ignored", r
    print("late receipt ignored")

    s, r = call("POST", "/deliveries/send-partition",
                {"delivery_ref": d1, "region": "西区"})
    assert s == 200 and r["data"]["partition_ref"] == f"{ENDPOINT}#v1:西区", r
    print("v1 西区 supplemental delivery OK")

    s, r = call("POST", "/revalidate", {
        "endpoint": ENDPOINT, "version_ref": f"{ENDPOINT}#v2", "region": "西区",
        "fixed_sample": {"record_id": "PK-9", "region": "西区", "payload": "y"}})
    assert s == 200 and r["data"]["need_recollect"] is True, r
    print("revalidate OK, need_recollect=True, state:", r["data"]["version_state"])

    s, r = call("POST", "/deliveries/complete", {"delivery_ref": d1})
    assert s == 200 and r["data"]["snapshot"]["field_version"] == f"{ENDPOINT}#v1", r
    print("d1 completed, snapshot:", r["data"]["snapshot"]["snapshot_ref"])

    s, r = call("GET", f"/endpoints/{ENDPOINT}/manifest")
    assert s == 200 and r["data"]["in_sync"] is True, r
    print("manifest in sync, todo:", [p["partition_ref"] for p in r["data"]["todo"]])

    s, r = call("GET", "/deliveries")
    assert s == 200 and len(r["data"]["items"]) == 1, r
    print("deliveries list OK")

    s, r = call("GET", "/events")
    assert s == 200 and len(r["data"]["items"]) >= 10, r
    print("event log OK, events:", len(r["data"]["items"]))

    print("\nHTTP 端到端冒烟全部通过 ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
