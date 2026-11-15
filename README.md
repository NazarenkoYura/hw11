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
