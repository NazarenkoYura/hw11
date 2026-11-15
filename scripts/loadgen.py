"""Генератор нагрузки hw11: клиентский Pod с расписанием, монотонные часы.

Роль в опыте B: минимум 4 клиента с разными IP; расписание поступлений
запланировано заранее, фиксируется и запланированное, и фактическое время
отправки (замедление самого генератора остаётся заметным). Keep-alive
включается/выключается флагом (влияет на распределение по Pod у обычного
Service — привязка на уровне соединения). Доля трафика клиента задаётся
общим расписанием (клиент 0 — «горячий» при skew-профиле).

Каждый запрос — отдельная запись JSONL (results/requests_*.jsonl):
  client, request_no, session_id, turn, prompt_chars, max_new_tokens,
  scheduled_send_offset_s, scheduled_send_utc, send_utc (фактическая),
  first_chunk_utc, end_utc, ttfc_seconds, answer_seconds, inter_chunk_mean,
  status, outcome (ok|http_4xx|http_5xx|timeout|interrupted|retry),
  attempt, pod, model_version, backend_mode, chunks, cache_result, stream.

Сводка (p50/p95, n, доли ошибок) печатается и пишется рядом (summary JSON).
"""
import argparse
import csv
import hashlib
import json
import os
import socket
import threading
import time
import urllib.parse
from pathlib import Path

import requests

# Те же 8 MCQ-промптов, что в bench ДЗ 8+9+10 (сопоставимость нагрузки).
BASE_PROMPTS = [
    "Сформулируй короткий ответ: что такое overfitting?",
    "Сформулируй короткий ответ: зачем нужен validation set?",
    "Сформулируй короткий ответ: что делает tokenizer?",
    "Сформулируй короткий ответ: чем precision отличается от recall?",
    "Сформулируй короткий ответ: что такое learning rate?",
    "Сформулируй короткий ответ: зачем нужен early stopping?",
    "Сформулируй короткий ответ: что такое batching при инференсе?",
    "Сформулируй короткий ответ: что такое KV-cache?",
]

# Контрольные строки для проверки изоляции диалогов (проверка №5)
CONTROL_SESSIONS = {
    "check-alpha": "АЛЬФА-КОНТРОЛЬ",
    "check-beta": "БЕТА-КОНТРОЛЬ",
}


def pctl(values, q):
    if not values:
        return None
    data = sorted(values)
    idx = min(len(data) - 1, max(0, round(q * (len(data) - 1))))
    return round(data[idx], 4)


def build_schedule(client_id: int, n_clients: int, profile: str, total_rate: float,
                   duration_s: float, sessions_per_client: int):
    """Детерминированное расписание: интервалы появления запросов клиента.

    profile=uniform: каждый клиент даёт total_rate/n_clients;
    profile=skew: клиент 0 даёт 80% total_rate, остальные поровну.
    Возвращает список моментов отправки (offset от старта) с атрибутами запроса.
    """
    if profile == "skew":
        rate = total_rate * 0.8 if client_id == 0 else total_rate * 0.2 / (n_clients - 1)
    else:
        rate = total_rate / n_clients
    seed = hashlib.sha256(f"hw11-sched-{client_id}-{profile}-{total_rate}".encode()).digest()
    entries = []
    # Регулярная сетка с детерминированным дрожанием ±20% интервала.
    interval = 1.0 / rate if rate > 0 else float("inf")
    t = interval / 2
    rng = int.from_bytes(seed[:8], "big")
    n_req = int(duration_s * rate)
    for i in range(n_req):
        rng = (rng * 6364136223846793005 + 1442695040888963407) % (1 << 64)
        jitter = (rng >> 33) / float(1 << 31) * 0.4 - 0.2
        send_at = t + jitter * interval
        session_idx = i % sessions_per_client
        turn = i // sessions_per_client
        session_id = f"c{client_id}-s{session_idx}"
        # История диалога передаётся в запросе (сервис без состояния).
        history = [{"role": "user",
                    "content": f"{BASE_PROMPTS[(client_id + session_idx) % len(BASE_PROMPTS)]} (ход {k})"}
                   for k in range(turn)]
        prompt = f"{BASE_PROMPTS[(client_id + session_idx) % len(BASE_PROMPTS)]} (ход {turn})"
        history = history[-19:]  # сервер принимает максимум 20 элементов истории
        entries.append({
            "send_at": send_at, "session_id": session_id, "turn": turn,
            "prompt": prompt, "history": history,
            "prompt_chars": len(prompt) + sum(len(h["content"]) for h in history),
        })
        t += interval
    return entries


def run_worker(client_id: int, worker_id: int, workers: int, args, out_file,
               stop_event, ready_event):
    """Поток-воркер клиента: своя доля расписания и своё соединение.

    Несколько воркеров делают нагрузку клиента КОНКУРЕНТНОЙ (последовательный
    клиент ограничен ~1/время_ответа и не способен создать очередь на сервере).
    При keep-alive каждый воркер держит собственное соединение — как реальный
    многопоточный клиент."""
    session_local = requests.Session() if args.keepalive else None
    start_wall = time.time()
    start_mono = time.perf_counter()
    schedule = build_schedule(client_id, args.clients, args.profile, args.rate,
                              args.duration, args.sessions)[worker_id::workers]
    ready_event.set()
    records = []
    for entry in schedule:
        if stop_event.is_set():
            break
        # Ожидание запланированного момента (микрополлинг для точности).
        while True:
            now = time.perf_counter() - start_mono
            wait = entry["send_at"] - now
            if wait <= 0:
                break
            if stop_event.is_set():
                break
            time.sleep(min(wait, 0.05))
        if stop_event.is_set():
            break
        scheduled_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(
            start_wall + entry["send_at"]))
        payload = {"prompt": entry["prompt"], "max_new_tokens": args.tokens,
                   "session_id": entry["session_id"], "turn": entry["turn"],
                   "history": entry["history"], "stream": args.stream}
        attempt = 1
        max_attempts = args.retries + 1
        while attempt <= max_attempts:
            rec = {
                "client": client_id, "request_no": len(records) + 1,
                "session_id": entry["session_id"], "turn": entry["turn"],
                "prompt_chars": entry["prompt_chars"],
                "max_new_tokens": args.tokens, "stream": args.stream,
                "scheduled_send_offset_s": round(entry["send_at"], 3),
                "scheduled_send_utc": scheduled_utc,
                "attempt": attempt,
            }
            send_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            t_send = time.perf_counter()
            rec["send_utc"] = send_utc
            rec["send_delay_s"] = round(t_send - start_mono - entry["send_at"], 4)
            status, outcome = None, "ok"
            pod = version = mode = None
            cache_result = None
            unwarmed = None
            queue_s = None
            chunks = 0
            ttfc = itl_mean = answer_s = None
            try:
                if args.stream:
                    # Потоковый ответ: монотонные часы на клиенте, части через SSE.
                    # ВАЖНО: потребляем ответ ДО КОНЦА (не break по done) — иначе
                    # urllib3 считает соединение «неочищенным», отбрасывает его и
                    # keep-alive (привязка к соединению) молча перестаёт работать.
                    opener = session_local.post if session_local else requests.post
                    with opener(args.url + "/generate", json=payload, timeout=args.timeout,
                                stream=True) as resp:
                        status = resp.status_code
                        if status == 200:
                            seen_done = False
                            for line in resp.iter_lines(decode_unicode=False):
                                if not line or not line.startswith(b"data: "):
                                    continue
                                ev = json.loads(line[6:])
                                now_mono = time.perf_counter()
                                if ev.get("done"):
                                    answer_s = now_mono - t_send
                                    pod, version, mode = ev.get("pod"), ev.get(
                                        "model_version"), ev.get("backend_mode")
                                    cache_result = ev.get("cache_result")
                                    unwarmed = ev.get("unwarmed")
                                    chunks = ev.get("chunks", 0)
                                    queue_s = ev.get("queue_seconds")
                                    seen_done = True
                                    continue
                                if ev.get("chunk_index") == 0:
                                    ttfc = now_mono - t_send
                                    chunks += 1
                                else:
                                    chunks += 1
                            if not seen_done:
                                outcome = "interrupted"
                        else:
                            outcome = f"http_{status}"
                else:
                    opener = session_local.post if session_local else requests.post
                    resp = opener(args.url + "/generate", json=payload,
                                  timeout=args.timeout)
                    status = resp.status_code
                    now_mono = time.perf_counter()
                    answer_s = now_mono - t_send
                    if status == 200:
                        body = resp.json()
                        pod, version, mode = body.get("pod"), body.get(
                            "model_version"), body.get("backend_mode")
                        cache_result = body.get("cache_result")
                        unwarmed = body.get("unwarmed")
                        chunks = body.get("chunks", 0)
                        ttfc = body.get("ttfc_seconds")
                    else:
                        outcome = f"http_{status}"
            except requests.Timeout:
                status, outcome = None, "timeout"
            except (requests.ConnectionError, requests.ChunkedEncodingError) as error:
                status, outcome = None, "interrupted"
            t_end = time.perf_counter()
            rec.update({
                "end_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "status": status, "outcome": outcome,
                "ttfc_seconds": round(ttfc, 4) if ttfc is not None else None,
                "answer_seconds": round(answer_s, 4) if answer_s is not None else None,
                "pod": pod, "model_version": version, "backend_mode": mode,
                "chunks": chunks, "cache_result": cache_result,
                "unwarmed": unwarmed,
                "queue_seconds_server": round(queue_s, 4) if queue_s is not None else None,
            })
            if outcome in ("ok", "interrupted"):
                records.append(rec)
                with out_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                break
            if attempt < max_attempts and args.retries > 0:
                # Повтор учитывается ОТДЕЛЬНОЙ попыткой: новая запись.
                rec["outcome"] = f"retry_of_{outcome}"
                records.append(rec)
                with out_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                attempt += 1
                time.sleep(0.2)
                continue
            records.append(rec)
            with out_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            break
    return records


def summarize(out_dir: Path, run_label: str, duration: float):
    """Сводка по успешным ответам: p50/p95 с числом наблюдений и долями ошибок."""
    import math
    files = sorted(out_dir.glob("requests_*.jsonl"))
    rows = []
    for path in files:
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    if not rows:
        return None
    ok = [r for r in rows if r["outcome"] == "ok"]
    ttfc = [r["ttfc_seconds"] for r in ok if r.get("ttfc_seconds") is not None]
    ans = [r["answer_seconds"] for r in ok]
    planned = sum(1 for r in rows if r.get("attempt") == 1)
    summary = {
        "run": run_label, "duration_s": duration,
        "records": len(rows), "planned_first_attempts": planned,
        "ok": len(ok),
        "errors": len(rows) - len(ok),
        "error_rate": round((len(rows) - len(ok)) / max(1, len(rows)), 4),
        "ttfc_p50_s": pctl(ttfc, 0.50), "ttfc_p95_s": pctl(ttfc, 0.95),
        "ttfc_n": len(ttfc),
        "answer_p50_s": pctl(ans, 0.50), "answer_p95_s": pctl(ans, 0.95),
        "answer_n": len(ans),
        "by_pod": {},
        "by_client": {},
    }
    pods = sorted({r.get("pod") or "?" for r in rows})
    for pod in pods:
        prows = [r for r in rows if (r.get("pod") or "?") == pod]
        pok = [r for r in prows if r["outcome"] == "ok"]
        pttfc = [r["ttfc_seconds"] for r in pok if r.get("ttfc_seconds") is not None]
        pcache = [r.get("cache_result") for r in pok]
        summary["by_pod"][pod] = {
            "requests": len(prows), "ok": len(pok),
            "ttfc_p50_s": pctl(pttfc, 0.5), "ttfc_p95_s": pctl(pttfc, 0.95),
            "cache_hit_rate": round(pcache.count("hit") / max(1, len(pcache)), 4),
        }
    for client in sorted({r["client"] for r in rows}):
        summary["by_client"][str(client)] = sum(
            1 for r in rows if r["client"] == client)
    out = out_dir / f"summary_{run_label}.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.getenv("TARGET_URL", "http://hw11-svc"))
    parser.add_argument("--client", type=int,
                        default=int(os.getenv("CLIENT_ID", "0")))
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--profile", choices=["uniform", "skew"], default="uniform")
    parser.add_argument("--rate", type=float, default=4.0, help="запросов/с суммарно")
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument("--tokens", type=int, default=32)
    parser.add_argument("--sessions", type=int, default=8)
    parser.add_argument("--stream", action="store_true", default=True)
    parser.add_argument("--no-stream", dest="stream", action="store_false")
    parser.add_argument("--keepalive", action="store_true", default=True)
    parser.add_argument("--no-keepalive", dest="keepalive", action="store_false")
    parser.add_argument("--timeout", type=float, default=45.0)
    parser.add_argument("--retries", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1,
                        help="параллельных воркеров у клиента (конкурентная нагрузка)")
    parser.add_argument("--out", default=os.getenv("OUT_DIR", "/results"))
    parser.add_argument("--label", default="run")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"requests_c{args.client}_{args.label}.jsonl"
    if out_file.exists():
        out_file.unlink()

    stop_event = threading.Event()
    ready_event = threading.Event()
    workers = max(1, int(os.getenv("WORKERS", str(args.workers))))
    threads = []
    for wid in range(workers):
        out_file_w = out_dir / f"requests_c{args.client}_w{wid}_{args.label}.jsonl"
        if out_file_w.exists():
            out_file_w.unlink()
        t = threading.Thread(target=run_worker,
                             args=(args.client, wid, workers, args, out_file_w,
                                   stop_event, ready_event), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=args.duration + 90)
    line = {"client": args.client, "workers": workers}
    print(json.dumps(line, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
