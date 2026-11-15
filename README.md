# README — ДЗ 11: сервис в Kubernetes под нагрузкой

МГТУ · MLOPS-26 · работа к лекции 11. Продолжение сервиса ДЗ 8+9+10 (`hw8910`):
simulated backend на CPU, прогрев, очередь, стриминг, имитация префиксного кеша,
Prometheus/Grafana, HPA, Helm-чарт, kind.

## Быстрый старт

```bash
# 0. Инструменты: docker (Docker Desktop), kind, helm, kubectl — см. Версии ниже
# 1. Кластер
make cluster                 # kind create cluster --config kind-config.yaml
# 2. Образ
make build && make load      # сборка + загрузка в kind
# 3. Деплой стенда (рабочая конфигурация: readiness проверяет прогрев)
make deploy                  # helm upgrade --install hw11 chart --set readyEager=false
# 4. Проверить
kubectl get pods             # 2 реплики + prometheus + grafana + metrics-server
kubectl port-forward deploy/hw11-grafana 3000:3000   # дашборд http://localhost:3000
# 5. Собственные проверки (Job в кластере)
make checks
# 6. Удаление стенда
make clean                   # helm uninstall + kind delete cluster
```

## Опыты

| Опыт | Команда | Что показывает |
|---|---|---|
| A.1 дефект прогрева | `helm upgrade hw11 chart --reuse-values --set readyEager=true` + `scripts/exp_a_defect.sh` | непрогретый backend получает запросы |
| A.2 rollout A→B | `scripts/exp_a_rollout.sh` | стрим переживает rolling update, 0 5xx |
| A.3 неготовая B + откат | `scripts/exp_a_badb.sh` | B не готова — вне трафика; откат к A |
| B балансировка | `scripts/exp_b.sh` | plain vs ClientIP, uniform vs 80/20, 3×120с |
| C HPA + алерты | `scripts/exp_c.sh` | 2→3→2, окно стабилизации, pending/firing/resolved |
| Реальная модель | `scripts/exp_real.sh` | Qwen3-0.6B: TTFT/TPOT/tokens per s |

Результаты сырые: `results/requests_*.jsonl`, сводки `results/summary_*.json`,
журналы опытов `evidence/`, графики `charts/*.png`, отчёт `report.html`.

## Версии стенда (фактические)

| Компонент | Версия |
|---|---|
| Kubernetes (kind node) | v1.34.0 |
| kind | v0.33.0 |
| helm | v4.3.2 |
| kubectl | v1.34.1 |
| Docker Desktop | 29.5.2 (WSL2) |
| Образ сервиса | python:3.12-slim + fastapi==0.150.0, uvicorn==0.54.0, prometheus-client==0.23.1 |
| Prometheus | v3.8.1 (retention 2h, scrape 5s) |
| Grafana | 12.3.0 (provisioned dashboard) |
| metrics-server | v0.8.0 (--kubelet-insecure-tls) |
| Реальная модель | Qwen3-0.6B @ c1899de, torch==2.15.0, transformers==5.22.1 (CPU) |
| Backend mode | simulated (explicit) |

## Ресурсы и размер стенда

- Лимиты контейнеров сервиса: requests cpu 300m / mem 256Mi; limits cpu 1 / mem 512Mi.
- Prometheus retention 2h; Grafana 256Mi limit; метрики в Histogram (ограниченный набор меток).
- Фактический размер: `make size` (образы hw11-service ~0.4 ГБ sim / ~2.5 ГБ real,
  kind node ~1.5 ГБ, prometheus+grafana ~0.6 ГБ). Итого < 5 ГБ.

## Параметры backend (все в chart/values.yaml → ConfigMap)

- `WARMUP_SECONDS` 20 — прогрев (опыт A);
- `MAX_PARALLEL` 2, `QUEUE_TIMEOUT` 30с — очередь и лимит одновременной обработки;
- `PREFILL_HIT_SECONDS` 0.05 / `PREFILL_MISS_SECONDS` 0.4, `CACHE_CAPACITY` 64 —
  имитация локального префиксного кеша (LRU по session_id);
- `CHUNK_INTERVAL_SECONDS` 0.05, `CHUNKS_PER_REQUEST` 8 — выдача ответа частями;
- `CPU_BURN_MS_PER_TOKEN` 0/40 — вычислительная нагрузка (опыт C: наблюдаемый CPU для HPA);
- `READY_EAGER` 0/1 — исходный дефект readiness (опыт A);
- `WARMUP_NEVER` 0/1 — заведомо неготовая версия (опыт A.3).

Каждый ответ и строка журнала содержат: `request_id`, `pod`, `model_version`
(фактически загруженный `model_id`), `runtime_sha256`, `backend_mode=simulated`.
Диалог без состояния: `session_id`, `history`, `turn` передаются в запросе.

## Результаты опытов (фактические)

- **Опыт A** — исходный дефект (readiness до прогрева): 7 ответов непрогретого
  backend под нагрузкой; после исправления — 0. Rollout A→B со стримами: 0 5xx,
  0 обрывов. Неготовая B — вне EndpointSlice, 0 запросов; откат без ошибок.
  Kill контейнера: стрим оборван на 15/32 частей (EOF), Pod восстановлен.
  → evidence/exp_a_summary.md
- **Опыт B** — 5 комбинаций × 3 прогона × 120с, 6 rps, конкурентные клиенты
  (4 воркера): affinity+skew даёт перекос 5.8:1 и очередь (TTFC p50 0.56с против
  0.10с на холодной); plain+skew+keep-alive — привязка по соединению
  (равномерно при 4 соединениях); plain+skew+no-keep-alive — равномерно 1:1,
  hit-rate кеша падает с 0.97 до 0.49. → evidence/exp_b_runs.log
- **Опыт C** — HPA 2→3 через 25с после нагрузки (CPU 141–298% vs цель 70%),
  3→2 через 66с после снятия (окно стабилизации 60с); алерты:
  Hw11TtfcP95High pending(2.75с)→firing(34с), Hw11ErrorRateHigh
  pending(22%)→firing(93%), оба resolve после снятия нагрузки.
  → evidence/exp_c_hpa_timeline.csv, exp_c_alerts*.jsonl
- **Реальная модель** — Qwen3-0.6B (transformers, CPU, hostPath PV из кэша HF):
  22/22 ok, TTFT p50 0.16с, TPOT p50 0.175с/токен, 5.7 tokens/s.
  → results/requests_*_real.jsonl
- **report.html** — полный отчёт с графиками charts/*.png.
