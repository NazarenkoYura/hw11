"""Мониторинг Ready/EndpointSlice во время опытов: CSV-лента раз в секунду.

Запускается на хосте: python scripts/monitor_ready.py evidence/expA_timeline.csv
Столбцы: utc, phase, pod, ready, endpoints_ready_ips
"""
import json
import subprocess
import sys
import time

OUT = sys.argv[1] if len(sys.argv) > 1 else "evidence/timeline.csv"


def kubectl_json(args):
    result = subprocess.run(["kubectl"] + args, capture_output=True, text=True,
                            timeout=30)
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        return json.loads(result.stdout)
    except ValueError:
        return None


def snapshot():
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    pods = kubectl_json(["get", "pods", "-l", "app=hw11", "-o", "json"]) or {}
    ready_flag = {}
    for item in pods.get("items", []):
        name = item["metadata"]["name"]
        phase = item.get("status", {}).get("phase")
        cond = {c["type"]: c["status"]
                for c in item.get("status", {}).get("conditions", [])}
        ready_flag[name] = (phase, cond.get("Ready", "Unknown"))
    eps = kubectl_json(["get", "endpointslices", "-n", "default",
                        "-l", "kubernetes.io/service-name=hw11-svc", "-o", "json"]) or {}
    ips = []
    for ep_item in eps.get("items", []):
        for e in ep_item.get("endpoints", []):
            if e.get("conditions", {}).get("ready"):
                ips.extend(e.get("addresses", []))
    return now, ready_flag, ",".join(sorted(ips))


def main():
    while True:
        try:
            now, ready_flag, ips = snapshot()
            for name, (phase, ready) in sorted(ready_flag.items()):
                with open(OUT, "a", encoding="utf-8") as f:
                    f.write(f"{now};{phase};{name};{ready};{ips}\n")
        except Exception as error:
            with open(OUT, "a", encoding="utf-8") as f:
                f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())};"
                        f"monitor_error;;;;{error}\n")
        time.sleep(1)


if __name__ == "__main__":
    main()
