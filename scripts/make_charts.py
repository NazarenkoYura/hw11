"""Построение четырёх графиков ДЗ 11 (charts/*.png).

1. charts/chart1_rollout_ready.png    — прогрев/rollout и Ready по времени (опыт A)
2. charts/chart2_pod_ttfc_cache.png   — задержка и нагрузка по репликам (опыт B, affin+skew)
3. charts/chart3_policies.png         — сравнение политик по трём прогонам (опыт B)
4. charts/chart4_hpa.png              — масштабирование 2→3→2 с нагрузкой (опыт C)

Все величины подписаны с единицами; условия прогонов указаны в подписи к осям.
"""
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
EVID = ROOT / "evidence"
RES = ROOT / "results" / "eB"
OUT = ROOT / "charts"
OUT.mkdir(exist_ok=True)

plt.rcParams.update({"font.size": 10, "figure.dpi": 130})


def parse_ts(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


# ---------- график 1: Ready-реплики по времени (опыт A) ----------

def chart1():
    rows = []
    for path in (EVID / "exp_a1_timeline.csv",):
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                parts = line.split(";")
                if len(parts) >= 5 and parts[0].startswith("20"):
                    rows.append(parts)
    if not rows:
        print("chart1: нет данных"); return
    times = sorted({parse_ts(r[0]) for r in rows})
    t0 = times[0]
    by_time = defaultdict(set)
    for r in rows:
        by_time[parse_ts(r[0])].add((r[2], r[3]))
    ready, pods_names = [], set()
    for t in times:
        ready.append(sum(1 for _, st in by_time[t] if st == "True"))
        pods_names |= {p for p, _ in by_time[t]}
    fig, ax = plt.subplots(figsize=(10.5, 4.2))
    ax.step(times, ready, where="post", color="#4c72b0", lw=1.8, label="Ready-реплик")
    ax.axhline(2, color="gray", ls=":", lw=1, label="Номинал (2 реплики)")
    phases = [
        ("2026-11-15T17:06:24Z", "A.1 rollout\nREADY_EAGER=1"),
        ("2026-11-15T17:13:30Z", "исправление\nreadyz"),
        ("2026-11-15T17:14:59Z", "A.1 повтор\nдефекта"),
        ("2026-11-15T17:16:48Z", "исправление"),
        ("2026-11-15T17:21:55Z", "A.2 rollout\nA→B"),
        ("2026-11-15T17:26:54Z", "A.3 неготовая\nB"),
        ("2026-11-15T17:28:23Z", "A.3 откат\nhelm rollback"),
        ("2026-11-15T17:35:45Z", "A.4 kill\nконтейнера"),
    ]
    for ts, label in phases:
        try:
            t = parse_ts(ts)
        except ValueError:
            continue
        if t0 <= t <= times[-1]:
            ax.axvline(t, color="#c44e52", ls="--", lw=1)
            ax.annotate(label, (t, 2.62), rotation=90, fontsize=7.2,
                        ha="right", va="bottom", color="#555555")
    ax.set_ylim(-0.3, 3.3)
    ax.set_ylabel("Число Ready-реплик (из 2)")
    ax.set_xlabel("UTC (стенд: 2 реплики, прогрев 20с; события — фазы опыта A)")
    ax.set_title("Опыт A: Ready-реплики и фазы выпуска/отката во времени")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.legend(loc="lower left", fontsize=8)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT / "chart1_rollout_ready.png")
    plt.close(fig)
    print("chart1 OK")


# ---------- график 2: задержка и распределение по репликам (опыт B affin+skew) ----------

def chart2():
    recs = []
    for run in ("affin-skew-noka-1", "affin-skew-noka-2", "affin-skew-noka-3"):
        for path in RES.glob(f"requests_c*_{run}.jsonl"):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    recs.append(json.loads(line))
    if not recs:
        print("chart2: нет данных"); return
    ok = [r for r in recs if r["outcome"] == "ok"]
    pods = sorted({r["pod"] for r in ok})
    p50 = [sorted([r["ttfc_seconds"] for r in ok if r["pod"] == p])[
        round(0.5 * (sum(1 for r in ok if r["pod"] == p) - 1))] for p in pods]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    ax = axes[0]
    xs = range(len(pods))
    data = [[r["ttfc_seconds"] for r in ok if r["pod"] == p] for p in pods]
    bp = ax.boxplot(data, labels=[p.split("-")[-1] for p in pods], showfliers=False,
                    patch_artist=True)
    for box in bp["boxes"]:
        box.set(facecolor="#9fb8cc")
    ax.set_ylabel("TTFC, секунд (клиент; включает очередь)")
    ax.set_xlabel("Реплика (суффикс Pod); опыт B: sessionAffinity, трафик 80/20")
    ax.set_title("TTFC по репликам: горячая реплика ждёт,\nвторая свободна")
    ax.grid(alpha=0.3, axis="y")
    ax = axes[1]
    counts = [sum(1 for r in ok if r["pod"] == p) for p in pods]
    hit = [statistics.mean([1 if r.get("cache_result") == "hit" else 0
                            for r in ok if r["pod"] == p]) for p in pods]
    bars = ax.bar(xs, counts, color="#4c72b0", width=0.55, label="запросов (шт)")
    for i, (c, h) in enumerate(zip(counts, hit)):
        ax.text(i, c + max(counts) * 0.02, f"{c}\nhit {h*100:.0f}%", ha="center", fontsize=8)
    ax.set_ylabel("Успешных запросов за 3 прогона × 120с")
    ax.set_xlabel("Реплика; подпись — доля cache hit имитатора (hit 0.05с / miss 0.4с)")
    ax.set_title("Распределение запросов и локальность кеша\nпо репликам (affinity, 80/20)")
    ax.set_xticks(list(xs))
    ax.set_xticklabels([p.split("-")[-1] for p in pods])
    ax.grid(alpha=0.3, axis="y")
    fig.suptitle("Опыт B: нагрузка 6 rps, 32 токена, stream, keep-alive off, 3×120с",
                 fontsize=9, y=0.02, va="bottom")
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(OUT / "chart2_pod_ttfc_cache.png")
    plt.close(fig)
    print("chart2 OK")


# ---------- график 3: сравнение политик по трём прогонам ----------

def read_run_summaries():
    """Собирает сводки из evidence/exp_b_runs.log (JSON-строки)."""
    out = defaultdict(list)
    for line in (EVID / "exp_b_runs.log").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith('{"run"'):
            d = json.loads(line)
            combo = d["run"].rsplit("-", 1)[0]
            out[combo].append(d)
    return out


def chart3():
    runs = read_run_summaries()
    order = [("plain-uniform-ka", "plain Service\nuniform, keep-alive ON"),
             ("plain-skew-ka", "plain Service\n80/20, keep-alive ON"),
             ("affin-uniform-noka", "ClientIP affinity\nuniform"),
             ("affin-skew-noka", "ClientIP affinity\n80/20"),
             ("plain-skew-noka", "plain Service\n80/20, keep-alive OFF")]
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.4))
    ax = axes[0]
    xs, means, errs, labels = [], [], [], []
    for i, (key, label) in enumerate(order):
        rs = runs.get(key, [])
        if not rs:
            continue
        p95s = [r["ttfc_p95"] for r in rs if r["ttfc_p95"] is not None]
        xs.append(i); means.append(statistics.mean(p95s))
        errs.append(max(p95s) - min(p95s))
        labels.append(label)
    ax.bar(xs, means, yerr=errs, capsize=4, color="#55a868", width=0.6)
    for x, m in zip(xs, means):
        ax.text(x, m + 0.15, f"{m:.2f}s", ha="center", fontsize=8)
    ax.set_xticks(xs); ax.set_xticklabels(labels, fontsize=7.2)
    ax.set_ylabel("TTFC p95, секунд (среднее по 3 прогонам,\nпланка — разброс прогонов)")
    ax.set_title("Задержка до первой части (p95) по политикам")
    ax.grid(alpha=0.3, axis="y")
    ax = axes[1]
    xs, ratios, labels = [], [], []
    for i, (key, label) in enumerate(order):
        rs = runs.get(key, [])
        if not rs:
            continue
        rr = []
        for r in rs:
            counts = [d["ok"] for d in r["by_pod"].values()]
            if len(counts) == 2 and min(counts) > 0:
                rr.append(max(counts) / min(counts))
        if rr:
            xs.append(i); ratios.append(statistics.mean(rr)); labels.append(label)
    ax.bar(xs, ratios, color="#c44e52", width=0.6)
    ax.axhline(1.0, color="gray", ls=":", lw=1)
    ax.text(xs[-1] + 0.35, 1.0, "равномерно", fontsize=8, va="center")
    ax.set_xticks(xs); ax.set_xticklabels(labels, fontsize=7.2)
    ax.set_ylabel("Отношение нагрузки самой загруженной\nреплики к менее загруженной (раз)")
    ax.set_title("Перекос распределения запросов по репликам")
    ax.grid(alpha=0.3, axis="y")
    fig.suptitle("Опыт B: 5 комбинаций × 3 прогона × 120с; 6 rps; одинаковые входы и расписание",
                 fontsize=9, y=0.02, va="bottom")
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(OUT / "chart3_policies.png")
    plt.close(fig)
    print("chart3 OK")


# ---------- график 4: HPA 2→3→2 ----------

def chart4():
    path = EVID / "exp_c_hpa_timeline.csv"
    if not path.exists():
        print("chart4: нет данных"); return
    times, current, desired, cpu = [], [], [], []
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split(";")
        if not parts[0].startswith("20") or len(parts) < 3:
            continue
        d = dict(p.split("=", 1) for p in parts[1:] if "=" in p)
        if "current" not in d:
            continue
        times.append(parse_ts(parts[0]))
        current.append(int(d.get("current", 0) or 0))
        desired.append(int(d.get("desired", 0) or 0))
        c = d.get("cpu", "")
        try:
            cpu.append(float(c.split("%")[0]))
        except (ValueError, IndexError):
            cpu.append(math.nan)
    if not times:
        print("chart4: пустая лента"); return
    fig, ax = plt.subplots(figsize=(10.5, 4.4))
    ax2 = ax.twinx()
    ax.step(times, desired, where="post", color="#c44e52", lw=2, label="desired реплик (HPA)")
    ax.step(times, current, where="post", color="#4c72b0", lw=1.4, ls="--",
            label="current реплик (Running)")
    ax2.plot(times, cpu, color="#55a868", lw=1.2, alpha=0.85, label="CPU, % от request")
    ax2.axhline(70, color="#55a868", ls=":", lw=1)
    ax2.text(times[0] + (times[-1] - times[0]) * 0.01, 72, "цель 70%", fontsize=8,
             color="#55a868")
    ax.set_ylim(0, 5.2)
    ax2.set_ylim(0, max(120, max((c for c in cpu if not math.isnan(c)), default=100) * 1.15))
    ax.set_ylabel("Число реплик Deployment")
    ax2.set_ylabel("CPU утилизация относительно request, %")
    ax.set_xlabel("UTC; нагрузка: фаза 1 — 2.5 rps с вычислительной нагрузкой 40мс/токен,\n"
                  "фаза 2 — 12 rps (перегрузка), затем снятие; окно стабилизации scale-down 60с")
    ax.set_title("Опыт C: HPA autoscaling/v2 — масштабирование 2→3→2 по CPU")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT / "chart4_hpa.png")
    plt.close(fig)
    print("chart4 OK")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("1", "all"):
        chart1()
    if which in ("2", "all"):
        chart2()
    if which in ("3", "all"):
        chart3()
    if which in ("4", "all"):
        chart4()
