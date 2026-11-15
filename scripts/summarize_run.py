"""Сводка одного прогона опыта B по всем клиентским JSONL: p50/p95 TTFC,
распределение по Pod, hit-rate кеша, доли ошибок. Печатает одну JSON-строку.
"""
import json
import sys
from pathlib import Path


def pctl(values, q):
    if not values:
        return None
    data = sorted(values)
    idx = min(len(data) - 1, max(0, round(q * (len(data) - 1))))
    return round(data[idx], 4)


def main():
    out_dir, label = Path(sys.argv[1]), sys.argv[2]
    rows = []
    for path in sorted(out_dir.glob(f"requests_c*_{label}.jsonl")):
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    ok = [r for r in rows if r["outcome"] == "ok"]
    ttfc = [r["ttfc_seconds"] for r in ok if r.get("ttfc_seconds") is not None]
    by_pod = {}
    for r in ok:
        pod = r.get("pod") or "?"
        d = by_pod.setdefault(pod, {"ok": 0, "ttfc": [], "hits": 0, "total_cache": 0,
                                    "sessions": set()})
        d["ok"] += 1
        if r.get("ttfc_seconds") is not None:
            d["ttfc"].append(r["ttfc_seconds"])
        if r.get("cache_result") in ("hit", "miss"):
            d["total_cache"] += 1
            d["hits"] += 1 if r["cache_result"] == "hit" else 0
        d["sessions"].add(r.get("session_id"))
    pods_summary = {}
    for pod, d in sorted(by_pod.items()):
        pods_summary[pod] = {
            "ok": d["ok"],
            "ttfc_p50": pctl(d["ttfc"], 0.5), "ttfc_p95": pctl(d["ttfc"], 0.95),
            "cache_hit_rate": round(d["hits"] / max(1, d["total_cache"]), 3),
            "sessions": len(d["sessions"]),
        }
    planned = sum(1 for r in rows if r.get("attempt") == 1)
    print(json.dumps({
        "run": label, "records": len(rows), "planned_first": planned,
        "ok": len(ok), "errors": len(rows) - len(ok),
        "error_rate": round((len(rows) - len(ok)) / max(1, len(rows)), 4),
        "ttfc_p50": pctl(ttfc, 0.5), "ttfc_p95": pctl(ttfc, 0.95),
        "ttfc_n": len(ttfc), "by_pod": pods_summary,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
