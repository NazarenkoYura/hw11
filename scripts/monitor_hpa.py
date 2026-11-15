"""Монитор опыта C: каждые 5с пишет ленту HPA (реплики, CPU метрика, условия)
и CPU по подам. python scripts/monitor_hpa.py evidence/exp_c_hpa_timeline.csv
"""
import json
import subprocess
import sys
import time

OUT = sys.argv[1] if len(sys.argv) > 1 else "evidence/exp_c_hpa_timeline.csv"


def kubectl_json(args):
    result = subprocess.run(["kubectl"] + args, capture_output=True, text=True, timeout=20)
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        return json.loads(result.stdout)
    except ValueError:
        return None


def main():
    while True:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        hpa = kubectl_json(["get", "hpa", "hw11", "-o", "json"])
        top = kubectl_json(["top", "pods", "-l", "app=hw11", "--containers"])
        cpu_by_pod = {}
        if top:
            for item in top.get("items", []):
                cpu_by_pod[item["metadata"]["name"]] = item["containers"][0]["usage"]["cpu"]
        if hpa:
            status = hpa.get("status", {})
            current = status.get("currentReplicas")
            desired = status.get("desiredReplicas")
            metric = ""
            for m in status.get("currentMetrics", []):
                if m.get("type") == "Resource" and m["resource"]["name"] == "cpu":
                    cur = m["resource"]["current"]
                    metric = f"{cur.get('averageUtilization')}%({cur.get('averageValue')})"
            conds = ";".join(
                f"{c['type']}={c['status']}:{c.get('reason','')}"
                for c in status.get("conditions", []))
            pods = ";".join(f"{k}={v}" for k, v in sorted(cpu_by_pod.items()))
            with open(OUT, "a", encoding="utf-8") as f:
                f.write(f"{now};current={current};desired={desired};cpu={metric};{conds};{pods}\n")
        else:
            with open(OUT, "a", encoding="utf-8") as f:
                f.write(f"{now};hpa_missing\n")
        time.sleep(5)


if __name__ == "__main__":
    main()
