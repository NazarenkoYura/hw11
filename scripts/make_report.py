"""Генератор report.html ДЗ 11: собирает сводки опытов из evidence/results
и строит отчёт в стиле предыдущих работ. Запуск: python scripts/make_report.py
"""
import json
import statistics
from collections import defaultdict
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVID = ROOT / "evidence"
RES = ROOT / "results"


def pctl(values, q):
    if not values:
        return None
    data = sorted(values)
    idx = min(len(data) - 1, max(0, round(q * (len(data) - 1))))
    return round(data[idx], 3)


def read_run_summaries():
    out = defaultdict(list)
    log = EVID / "exp_b_runs.log"
    if log.exists():
        for line in log.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith('{"run"'):
                d = json.loads(line)
                combo = d["run"].rsplit("-", 1)[0]
                out[combo].append(d)
    return out


def combo_table():
    runs = read_run_summaries()
    order = [
        ("plain-uniform-ka", "plain Service", "uniform (25%×4)", "keep-alive ON"),
        ("plain-skew-ka", "plain Service", "80/20 (клиент 0 — 80%)", "keep-alive ON"),
        ("affin-uniform-noka", "sessionAffinity: ClientIP", "uniform (25%×4)", "keep-alive OFF"),
        ("affin-skew-noka", "sessionAffinity: ClientIP", "80/20 (клиент 0 — 80%)", "keep-alive OFF"),
        ("plain-skew-noka", "plain Service", "80/20 (клиент 0 — 80%)", "keep-alive OFF"),
    ]
    rows = []
    for key, policy, traffic, ka in order:
        rs = runs.get(key, [])
        if not rs:
            continue
        p95s = [r["ttfc_p95"] for r in rs if r["ttfc_p95"] is not None]
        p50s = [r["ttfc_p50"] for r in rs if r["ttfc_p50"] is not None]
        err = statistics.mean(r["error_rate"] for r in rs)
        skew = []
        for r in rs:
            counts = [d["ok"] for d in r["by_pod"].values()]
            if len(counts) == 2 and min(counts) > 0:
                skew.append(max(counts) / min(counts))
        hit_hot, hit_cold = [], []
        for r in rs:
            hits = [d["cache_hit_rate"] for d in r["by_pod"].values()]
            if hits:
                hit_hot.append(max(hits)); hit_cold.append(min(hits))
        rows.append(f"""<tr>
<td>{escape(policy)}<br><span class="small">{escape(ka)}</span></td>
<td>{escape(traffic)}</td>
<td>{len(rs)}</td>
<td>{escape(str(round(statistics.mean(p50s),2)))} / {escape(str(round(statistics.mean(p95s),2)))}</td>
<td>{err*100:.1f}%</td>
<td>{round(statistics.mean(skew),2) if skew else '—'}</td>
<td>{round(statistics.mean(hit_hot)*100)}% / {round(statistics.mean(hit_cold)*100)}%</td>
</tr>""")
    return "\n".join(rows)


def hpa_stats():
    path = EVID / "exp_c_hpa_timeline.csv"
    stats = {"scale_up_s": None, "scale_down_s": None, "max_replicas": 0}
    up_start = down_start = None
    prev_desired = 2
    seen_3 = False
    load_stop = None
    log = EVID / "exp_c.log"
    if log.exists():
        for line in log.read_text(encoding="utf-8").splitlines():
            if line.startswith("load_stopped_utc="):
                load_stop = line.split("=", 1)[1].strip()
    import datetime as dt
    def parse(s):
        return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            parts = line.split(";")
            if not parts[0].startswith("20"):
                continue
            d = dict(p.split("=", 1) for p in parts[1:] if "=" in p)
            if "desired" not in d:
                continue
            desired = int(d["desired"])
            ts = parse(parts[0])
            stats["max_replicas"] = max(stats["max_replicas"], desired)
            if desired == 3 and prev_desired == 2 and not seen_3:
                seen_3 = True
                up_start = ts
            if desired == 2 and prev_desired == 3 and seen_3 and down_start is None:
                down_start = ts
                if load_stop:
                    stats["scale_down_s"] = round((ts - parse(load_stop)).total_seconds())
            prev_desired = desired
    return stats


def real_stats():
    """Сводка короткого опыта реальной модели."""
    rows = []
    for path in sorted(RES.glob("requests_real_*.jsonl")) + sorted(
            RES.glob("requests_c*_real.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
    ok = [r for r in rows if r["outcome"] == "ok"]
    ttft = [r["ttfc_seconds"] for r in ok if r.get("ttfc_seconds")]
    tpot = []
    for r in ok:
        if r.get("ttfc_seconds") is not None and r.get("answer_seconds") and (r.get("chunks") or 0) > 1:
            tpot.append((r["answer_seconds"] - r["ttfc_seconds"]) / (r["chunks"] - 1))
    tps = [r["chunks"] / r["answer_seconds"] for r in ok
           if r.get("answer_seconds") and r.get("chunks")]
    return {
        "n": len(ok), "errors": len(rows) - len(ok),
        "ttft_p50": pctl(ttft, 0.5), "ttft_p95": pctl(ttft, 0.95),
        "tpot_p50": pctl(tpot, 0.5), "tokens_per_s_p50": pctl(tps, 0.5),
    }


def main():
    hpa = hpa_stats()
    real = real_stats()
    css = """body{overflow-wrap:anywhere;margin:0;background:#f6f8fc;color:#18263d;
font:20px/1.6 system-ui,sans-serif}main{max-width:1080px;margin:auto;padding:36px 30px 80px}
h1{font-size:36px}h2{font-size:28px;margin-top:40px}h3{font-size:23px}
a{color:#6341ba}.lead{font-size:24px}
.note{border-left:4px solid #8866e8;padding:14px 22px;background:#eee9fb}
code,pre{font-size:.86em}pre{background:#e8edf6;padding:16px;white-space:pre-wrap}
table{width:100%;border-collapse:collapse;font-size:17px}
th,td{text-align:left;vertical-align:top;border-bottom:1px solid #cad4e2;padding:10px}
img{max-width:100%}.small{font-size:16px;color:#64748b}
@media print{body{background:white;font-size:12pt}main{padding:0}img{max-width:100%}}"""

    html = f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ДЗ 11 — отчёт: сервис в Kubernetes под нагрузкой</title>
<style>{css}</style></head><body><main>
<p class="small">МГТУ · MLOPS-26 · ДЗ 11 · Kubernetes под нагрузкой · репозиторий
github.com/NazarenkoYura/hw11 · коммит см. README (финальный)</p>
<h1>Отчёт: сервис в Kubernetes под нагрузкой</h1>
<p class="lead">Сервис ДЗ 8+9+10 развёрнут в kind (2 реплики, Helm, Prometheus/Grafana),
воспроизведены и исправлены три эксплуатационные проблемы: прогрев/выпуск,
привязка к Pod и «горячая реплика», HPA и алерты. backend_mode=simulated.</p>

<div class="note"><strong>Состав стенда.</strong> kind v0.33.0, Kubernetes v1.34.0,
helm v4.3.2, kubectl v1.34.1, Docker Desktop 29.5.2 (WSL2). Образ python:3.12-slim,
fastapi==0.150.0, uvicorn==0.54.0, prometheus-client==0.23.1. Прометей v3.8.1
(retention 2h, scrape 5s), Grafana 12.3.0, metrics-server v0.8.0. Лимиты сервиса:
requests cpu 300m / mem 256Mi, limits cpu 1 / mem 512Mi. backend_mode=simulated
возвращается в каждом ответе. Инструкции запуска/удаления и размеры — в README.md.</div>

<h2>1. Стенд и Helm</h2>
<p>Deployment из 2 реплик + два Service (обычный и sessionAffinity: ClientIP) +
ConfigMap + HPA (autoscaling/v2, min 2, max 3, CPU 70%, окно стабилизации
scale-down 60с) + metrics-server (resource metrics API) + Prometheus (сбор с
каждого Pod через kubernetes_sd по аннотациям, правила алертов) + Grafana с
provisioned дашбордом. Все параметры опыта — в chart/values.yaml: образ/версия
модели, прогрев, число реплик, лимит параллелизма, параметры кеша, тайминги
probes, режим дефекта, sessionAffinity, grace period.</p>
<p>Источник и проверка версии артефакта: файлы model.json версий A/B запечены в
образ, accepted.json (указатель = манифест) закрепляет SHA-256 файла и
runtime_sha256; backend пересчитывает хеш файла до «загрузки» и отказывается
работать при расхождении. Каждая реплика загружает артефакт при старте —
после замены Pod версия и runtime_sha256 восстанавливаются (проверка №1/№3).</p>
<p>Каждый ответ и строка журнала содержат request_id, pod, model_version
(фактически загруженный model_id), runtime_sha256, backend_mode. Диалог без
состояния: session_id/history/turn передаются в запросе; продолжение диалога
после замены реплики — проверка №3. Прогрев связан с readiness: /readyz 200
только после load_ok + прогрева (в дефектном режиме — до него).</p>

<h2>2. Опыт A: прогрев, выпуск и откат</h2>
<h3>2.1 Исходный дефект и исправление</h3>
<p><strong>Гипотеза.</strong> Если readiness считает запуск HTTP-процесса достаточным
(READY_EAGER=1), новый Pod попадает в EndpointSlice до конца прогрева и отвечает
непрогретым backend под нагрузкой.</p>
<p><strong>Воспроизведение.</strong> rolling update под нагрузкой 2 rps без keep-alive:
клиент записал <strong>7 ответов непрогретого backend</strong> (unwarmed=true) — все на
новых Pod'ах (results/requests_a1_defect.jsonl); серверный журнал —
evidence/exp_a1_unwarmed_serverlog.jsonl. Собственная проверка №1 на дефектном
стенде: <strong>FAIL «200 != 503: новый Pod отчитался готовностью до конца
прогрева»</strong> (evidence/checks-red-defect.log).</p>
<p><strong>Причина.</strong> /readyz отвечал 200 сразу после старта uvicorn;
startup-прооба проходила мгновенно, Pod попадал в Endpoints, рабочий трафик
шёл в процесс, у которого ещё шёл прогрев (20с).</p>
<p><strong>Изменение.</strong> /readyz возвращает 200 только после фактической
загрузки артефакта (hash-проверка) и завершения прогрева; startup-прооба
запасом покрывает прогрев (3с × 30 = 90с).</p>
<p><strong>Повтор.</strong> та же нагрузка при исправлении: 0 непрогретых ответов,
0 ошибок (results/requests_a1b_fixed.jsonl); проверка №1 — 5/5 OK
(evidence/checks-green-fixed.log). График 1 — Ready и фазы выпуска по времени.</p>
<img src="charts/chart1_rollout_ready.png" alt="Ready по времени">

<h3>2.2 Rolling update A→B под потоковыми ответами</h3>
<p>Перед обновлением запущен длительный стрим (16с, 32 части) и общий поток.
maxUnavailable=0/maxSurge=1, preStop 5с, grace period 45с. Результат: <strong>0
HTTP 5xx, 0 незавершённых потоков</strong> (results/requests_a2_rolloutAB.jsonl);
версия по фактическим ответам сменилась sim-A → sim-B; Ready не опускался ниже
2 (график 1). Неготовая версия B (WARMUP_NEVER=1) не попала в EndpointSlice и
не получила ни одного запроса; `helm rollback` вернул обслуживание без ошибок,
версия подтверждена по ответам клиентов (results/requests_a3_rollback.jsonl).</p>

<h3>2.3 Принудительное завершение процесса</h3>
<p>SIGKILL для PID1 изнутри namespace невозможен (защита ядра), поэтому
принудительная остановка контейнера снаружи (crictl stop -t 0 на kind-ноде —
аналог OOM/отказа ноды). Стрим напрямую в Pod-жертву получил <strong>15 из 32
частей, затем EOF</strong> (evidence/exp_a4_kill_stream2.txt); общий поток — 0
ошибок (второй Pod обслуживал всё). Отказ обнаружен kubelet'ом: контейнер
перезапущен, Pod 0/1 на время прогрева (startup 503), затем Ready; повторные
запросы — успешны.</p>

<h2>3. Опыт B: привязка к Pod и горячая реплика</h2>
<p>Один и тот же backend, одни и те же две реплики; два Service над ними:
обычный и sessionAffinity: ClientIP (timeout 10800с). 4 клиентских Pod'а с
разными IP; профили трафика uniform (по 25%) и skew (клиент 0 — 80%).
Расписание, длины входов (≤ 256 символов + история), лимит ответа (32 токена,
stream) и отправка — идентичны для сравниваемых вариантов; различаются только
Service и keep-alive. Фаза измерения 120с, каждая комбинация — 3 прогона;
между прогонами Pod'ы перезапускаются (явное начальное состояние кеша и
прогрева). Замедление генератора видно по разности «запланировано/отправлено»
(поле send_delay_s в записях; p95 &lt; 0.1с во всех прогонах).</p>
<p><strong>Имитация локального префиксного кеша:</strong> LRU по session_id,
ёмкость 64 сессий на реплику; стоимость hit 0.05с, miss 0.4с (prefill);
первый ход сессии — всегда miss; ход t попадает в кеш реплики, если она
обслуживала ход t−1 того же сеанса. Числа ниже относятся к этой модели
нагрузки.</p>
<table><thead><tr><th>Политика</th><th>Трафик</th><th>Прогоны</th>
<th>TTFC p50/p95, с</th><th>Ошибки</th><th>Перекос (max/min по Pod)</th>
<th>Hit-rate горячая/холодная</th></tr></thead><tbody>
{combo_table()}
</tbody></table>
<p class="small">TTFC — монотонные часы клиента до первой части SSE-ответа
(включает очередь); запросы по подам — успешные; полные сырые записи в
results/eB/requests_*.jsonl.</p>
<img src="charts/chart3_policies.png" alt="Сравнение политик">
<img src="charts/chart2_pod_ttfc_cache.png" alt="Задержка и распределение по репликам">
<h3>Что показал опыт</h3>
<ul>
<li><strong>Перекос возник</strong> при 80/20 с привязкой: горячая реплика
обслуживает ~80% запросов, её очередь растёт (TTFC p95 в разы выше), вторая
реплика простаивает (график 2). Дополнительно фиксируется, что при keep-alive
на обычном Service возникает <em>та же</em> привязка — на уровне соединения.</li>
<li><strong>Перекос не возник</strong> там, где привязки нет: 80/20 без
sessionAffinity и без keep-alive — каждый запрос новое соединение, kube-proxy
распределяет по-соединению; трасса запросов (pod в каждой записи) показывает
~1:1 при 4.8/1.2 rps от клиента 0. То же — uniform: соединения 4 клиентов
распределены по обеим репликам.</li>
<li><strong>Локальность кеша</strong> — цена привязки: при affinity горячая
реплика набирает высокий hit-rate (быстрый prefill), холодная — miss;
без привязки hit-rate падает у обеих.</li>
<li><strong>Исправление</strong> для этой модели нагрузки: отключить
sessionAffinity (диалоги без состояния — история передаётся в запросе),
трафик распределяется равномерно, очередь на горячей реплике исчезает; плата —
потеря hit-rate (замерена в таблице). Что потребует отдельной проверки на
настоящем inference engine: общий префиксный кеш реплик (llm-d / SGLang
router), учёт веса prefill против decode и transfer-затрат — перенос вывода
«отключить привязку» на движок с реальным KV-кешем требует замеров TTFT при
переносе сессии между репликами.</li>
</ul>

<h2>4. Опыт C: HPA и мониторинг</h2>
<p>HPA autoscaling/v2: min 2, max 3, CPU 70%; metrics-server (v1beta1.metrics.k8s.io
Available=True) и CPU requests (300m) у контейнеров проверены. Нагрузка
вычислительная: CPU_BURN_MS_PER_TOKEN=40 (sha256 busy-loop) — причина
масштабирования наблюдаема в метриках CPU.</p>
<ul>
<li>Переход 2→3: момент появления Pod'а и время готовности зафиксированы лентой
evidence/exp_c_hpa_timeline.csv (шаг 5с); прогрев новой реплики 20с.</li>
<li>Возврат 3→2: окно стабилизации scale-down 60с; фактическая длительность от
снятия нагрузки до возврата к 2 репликам — <strong>{hpa_stats()['scale_down_s']}с</strong>.</li>
<li>Горячая реплика повторно: при сохранённой привязке 80% запросов получает
одна реплика, вторая недогружена; средняя CPU по Deployment определяется
средним по репликам — усреднение маскирует перегрев одной реплики, поэтому
масштабирование по средней CPU при выраженном перекосе запаздывает; при
привязке новая реплика может получить мало запросов (ClientIP ведёт сессии на
старые Pod'ы до истечения timeout).</li>
</ul>
<img src="charts/chart4_hpa.png" alt="HPA 2→3→2">
<h3>Алерты</h3>
<p>Два алерта в правилах Prometheus (chart/templates/prometheus.yaml):
<strong>Hw11TtfcHighP95</strong> — histogram_quantile(0.95, sum by (le)
(rate(hw11_ttfc_seconds_bucket[2m]))) &gt; 2с, for 2m;
<strong>Hw11ErrorRateHigh</strong> — доля отказов (queue_full|not_ready|server_error
к общему числу) &gt; 5%, for 1m. Минимальный объём наблюдений: rate-окна 1–2
минуты — при scrape 5с это ≥12–24 наблюдения на серию. Управляемая нагрузка:
фаза 1 (2.5 rps, вычислительная) — p95 TTFC &gt; 2с → pending → firing через 2м;
фаза 2 (12 rps) — доля отказов &gt; 5% → Hw11ErrorRateHigh pending → firing через
1м; снятие нагрузки — оба алерта возвращаются в ok. Переходы зафиксированы
запросами /api/v1/alerts (evidence/exp_c_alerts.jsonl).</p>

<h2>5. Что измерялось</h2>
<p>Все времена — монотонные часы на клиенте (time.perf_counter). Для имитатора
TTFC — время до первой части SSE-ответа; интервалы между частями — inter-chunk
latency (p50/p95 в дашборде). Для реальной модели TTFT считается до первого
содержательного токена (TextIteratorStreamer, skip_special_tokens=true), TPOT —
по интервалам после первого токена: TPOT = (полное время − TTFT)/(N−1), где N —
число выданных кусков стримера (≈ один кусок на токен); ответ из одного токена
сохраняется с отсутствующим TPOT. tokens/s = N / полное время.</p>
<p>По каждому запросу сохранены: запланированное и фактическое время отправки
(send_delay_s — отставание расписания), первая часть, завершение, статус, Pod,
версия, число частей, cache_result, queue_seconds (серверная), исход (ok /
http_4xx / http_5xx / timeout / interrupted / retry_of_*). Отдельно считаются
запланированные, отправленные, завершённые, отклонённые (4xx), оборванные
(interrupted) и просроченные (timeout) — сводки в results/summary_*.json и
evidence/exp_b_runs.log; для успешных ответов p50/p95 с числом наблюдений
(ttfc_n) и долями ошибок.</p>
<p><strong>Histograms.</strong> hw11_ttfc_seconds, hw11_answer_seconds,
hw11_queue_seconds (buckets до 60с), hw11_inter_chunk_seconds (до 5с). Перцентиль
из classic histogram: Le-бакеты суммируются по всем Pod:</p>
<pre>histogram_quantile(0.95,
  sum by (le) (rate(hw11_ttfc_seconds_bucket[2m])))</pre>
<p>Сравнение с клиентским журналом за один и тот же период (опыт C, фаза 1):
клиентский p95 TTFC и серверный histogram_quantile совпадают в пределах шага
бакетов (сравнение в evidence/exp_c_promql_compare.json). Дашборд: TTFC
p50/p95, полная длительность, running/waiting по Pod, завершённые/ошибки в
секунду, CPU/RAM (cAdvisor), Ready-реплики, hit-rate кеша, unwarmed
(экспорт: monitoring/hw11-dashboard.json; используется ограниченный набор
меток — outcome/result/pod; request_id и session_id только в журнале).</p>

<h2>6. Собственные проверки</h2>
<p>Пять проверок (checks/checks.py, Job в кластере, python -m unittest):
(1) гейт прогрева — новый Pod вне EndpointSlice и /readyz=503 во время прогрева,
версия/хеш восстанавливаются; (2) версия после отката через Service —
model_id и runtime_sha256 ожидаемого артефакта; (3) замена Pod — диалог
продолжается на новой реплике, версия сохраняется; (4) учёт отказов — 400 и
оборванный поток фиксируются отдельными исходами client_error/interrupted в
/metrics; (5) изоляция двух диалогов с контрольными строками АЛЬФА/БЕТА.
Red: evidence/checks-red-defect.log (FAIL на дефекте READY_EAGER), Green:
evidence/checks-green-fixed.log (5/5 OK после исправления).</p>

<h2>7. Ревью: закреплять ли диалог за Pod</h2>
<div class="note">
<strong>Ситуация.</strong> Коллега предлагает навсегда закрепить диалог за Pod:
у A есть нужный префикс, но очередь 3с; B свободна, пересчёт 0.8с.
<strong>Ревью.</strong> При этих числах закрепление вредно: очередь 3с — это
p50+ задержки для всех запросов сеанса, а выигрыш от префикса (в нашем
имитаторе 0.4с→0.05с prefill, т.е. сотни мс) несопоставим с 3с ожидания.
Решение: не закреплять навсегда; направлять запросы по наименьшей очереди
(stateless-маршрутизация), пересчитывать префикс на свободной реплике
(0.8с один раз лучше, чем 3с на каждый ход). Если кеш сеанса всё же важнее —
использовать мягкую привязку с TTL и переездом при перегрузке, а не
«навсегда». Недостающие измерения: распределение длины очередей A/B во
времени (p95 ожидания на каждой реплике), доля запросов сеанса с hit/miss
после переезда, стоимость пересчёта на реальном движке (prefill 0.8с
имитатора ≠ стоимость на реальном KV-кеше), эффект на p95/p99 хвосты, а не
только на среднее. Допуск к новому запросу: запрос сеанса направляем на
реплику с минимальной очередью, если разница с «своей» репликой больше
стоимости пересчёта (0.8с); иначе — на свою. Восстановление диалога после
потери A: диалог обязан переживать потерю реплики — история передаётся в
запросе (как в нашем сервисе), «навсегда закреплённый» диалог умирает вместе
с Pod'ом (проверка №3 стенда показывает корректное поведение). Уже начатый
потоковый ответ не переключается между репликами: он завершается на A в
пределах grace period, следующий ход — по правилу допуска выше.
<strong>Кейс из лекции.</strong> Это тот же класс проблем, что и у Discord
(закрепление «гильдий» за процессами/нодами: горячие гильдии перегревают
отдельные узлы, а отказ узла делает недоступными закреплённые за ним
состояния) — первичный источник: Jesse Howarth, «How Discord Scaled Elixir to
5,000,000 Concurrent Users», blog.discord.com (2017). Вывод совпадает с их
практикой: состояние должно переживать потерю узла, а «горячие» ключи —
перераспределяться.
</div>

<h2>8. Ограничения и оставшиеся риски</h2>
<ul>
<li>Имитатор: числа TTFC/кеша относятся к модели нагрузки имитатора; перенос
на реальный движок требует замеров (реальный опыт — раздел 9).</li>
<li>kind — одна нода: сетевые эффекты multi-node не воспроизводятся; ClientIP
привязка проверена в пределах одного kube-proxy.</li>
<li>metrics-server использует --kubelet-insecure-tls (допустимо для стенда).</li>
<li>Прометей хранит 2ч (ограничение стенда), scrape 5с.</li>
<li>Инцидент стенда: kube-proxy (WSL2) падал на iptables-restore
(RULE_APPEND) — диагностика и обход в evidence/incident_kubeproxy.md.</li>
</ul>

<h2>9. Дополнительный опыт: реальная модель (Qwen3-0.6B, CPU)</h2>
<p>Тот же сервис, тот же Helm-чарт (профиль real.enabled=true), тот же loadgen и
дашборд; веса Qwen3-0.6B @ c1899de (кэш HuggingFace хоста) доступны всем
репликам через hostPath PV /mnt/hf (RWX на одной ноде kind); файлы артефакта
проверяются по SHA-256 из манифеста перед загрузкой (манифест и хеши
унаследованы из ДЗ 8+9+10, artifacts/real). backend_mode=transformers.
Короткая серия: те же 8 MCQ-промптов, 32 токена, stream.</p>
<table><thead><tr><th>Метрика</th><th>Значение (p50)</th><th>n</th></tr></thead><tbody>
<tr><td>TTFT до первого содержательного токена, с</td>
<td>{real['ttft_p50']} (p95 {real['ttft_p95']})</td><td>{real['n']}</td></tr>
<tr><td>TPOT, с/токен = (полное время − TTFT)/(N−1)</td>
<td>{real['tpot_p50']}</td><td>{real['n']}</td></tr>
<tr><td>tokens/s = N / полное время</td>
<td>{real['tokens_per_s_p50']}</td><td>{real['n']}</td></tr>
</tbody></table>
<p class="small">Ошибок: {real['errors']}. Сырые записи: results/requests_real_*.jsonl.
Измерения имитатора и модели в отчёте разделены (разделы 3 и 9); названия
TTFT/TPOT/tokens/s применены только к реальным токенам модели.</p>

<h2>10. Удаление стенда</h2>
<pre>make clean          # helm uninstall hw11 + kind delete cluster --name hw11
docker rmi hw11-service:1.0.0 hw11-service:real-1.0.0
# освобождается ~5 ГБ (kind node ~1.5 ГБ, образы 0.4+2.5 ГБ, prometheus+grafana ~0.6 ГБ)</pre>
<p>Фактический размер стенда зафиксирован командой `make size` (вывод в README).</p>
</main></body></html>
"""
    (ROOT / "report.html").write_text(html, encoding="utf-8")
    print("report.html written")


if __name__ == "__main__":
    main()
