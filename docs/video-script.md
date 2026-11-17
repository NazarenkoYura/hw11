# Сценарий видео к ДЗ 11 (~7 минут)

Формат — как в ДЗ 8+9+10: показываем команды, их вывод и коротко говорим, что
видим. Один дубль целиком, оговорки и паузы не страшны. Хронометраж 6–8 минут.

## Подготовка (до нажатия Rec, ~5 минут)

1. Docker Desktop запущен, кластер поднят:
   `kubectl get nodes` → hw11-control-plane Ready.
   Если кластер удалён: `make cluster && make build && make load && make deploy`.
2. `kubectl get pods` → 2 реплики hw11 1/1, prometheus/grafana/metrics-server Running.
3. Терминал (Git Bash) в каталоге `C:\Users\user\Documents\GitHub\hw11`,
   шрифт покрупнее: правой кнопкой по заголовку → Properties → Font 16–18.
4. Вкладки браузера (открыть заранее):
   - Grafana: `kubectl port-forward deploy/hw11-grafana 3000:3000` → http://localhost:3000
   - отчёт: открыть файл report.html из репозитория.
5. Windows: «Не беспокоить» (чтобы уведомления не лезли в кадр), закрыть лишние вкладки.
6. Репетиция двух live-фрагментов (сцены 3 и 7) — каждый ~40 секунд.

## Сцена 1. Интро — 0:00–0:20

Команда: `git log --oneline -5`

Сказать: «ДЗ 11 — сервис из ДЗ 8+9+10 в Kubernetes под нагрузкой. Три
проблемы: прогрев при выпуске, привязка клиента к Pod и горячая реплика,
автоскейлинг с алертами. Основная часть — CPU-имитатор, backend_mode
simulated; в конце — короткий опыт с реальной Qwen3-0.6B».

## Сцена 2. Стенд — 0:20–1:15

```
kubectl get pods -o wide                      # 2 реплики + мониторинг
helm list                                     # релиз hw11
kubectl get svc                               # два Service: plain + affinity
kubectl describe deploy hw11 | grep -A20 Readiness   # probes и resources
kubectl get --raw /apis/metrics.k8s.io/v1beta1/nodes | head -c 200   # resource metrics API живой
```

Переключиться в Grafana: дашборд «HW11 — сервис под нагрузкой», провести по
панелям (TTFC p50/p95, running/waiting по Pod, ошибки, CPU/RAM, Ready-реплики,
hit-rate кеша, unwarmed).

## Сцена 3. Опыт A: гейт прогрева LIVE — 1:15–2:10

```
kubectl delete pod <имя-пода> --wait=false
kubectl get pods                              # новый Pod 0/1 — startup probe 503 (прогрев 20с)
kubectl get endpoints hw11-svc                # нового пода НЕТ в Endpoints
# подождать Ready (10–20с):
kubectl get endpoints hw11-svc                # теперь 2 адреса
```

Пока ждём — красный лог (дефект до исправления):
`grep -E "FAIL|AssertionError" evidence/checks-red-defect.log | head -4`
→ «новый Pod отчитался готовностью до конца прогрева, 200 != 503».
Зелёный после исправления: `tail -6 evidence/checks-green-final.log` (5/5 OK).

Кратко по остальному опыту A (показать файл/график, не выполнять):
- rollout A→B под стримами: `charts/chart1_rollout_ready.png` — 0 5xx, 0 обрывов;
- kill контейнера посреди стрима: `cat evidence/exp_a4_kill_stream2.txt`
  (15 из 32 частей, затем EOF).

## Сцена 4. Опыт B: привязка и горячая реплика — 2:10–3:40

Открыть report.html, раздел 3 (таблица) — проговорить столбцы.
Ключевые числа (три прогона на комбинацию, 120с, 6 rps, конкурентные клиенты):
- sessionAffinity + 80/20: перекос 5.8:1, TTFC горячей 0.56с против 0.10с;
- keep-alive на обычном Service = та же привязка на уровне СОЕДИНЕНИЯ
  (4 соединения клиента → распределение равномерное);
- без привязки и keep-alive: 1:1, hit-rate кеша падает 0.97 → 0.49.

Сырая трасса (в каждой записи виден pod — по ней построены сводки):
`head -1 results/eB/requests_c0_w0_affin-skew-noka-1.jsonl | ./.venv/Scripts/python.exe -m json.tool`

Показать `charts/chart2_pod_ttfc_cache.png` и `charts/chart3_policies.png`.

## Сцена 5. Опыт C: HPA и алерты — 3:40–5:00

Как включали (показать команду, не выполнять):
`helm upgrade hw11 chart --reuse-values --set hpa.enabled=true --set backend.cpuBurnMsPerToken=40`

Переходы реплик из ленты (шаг 5с):
```
awk -F';' '{split($3,d,"="); if (d[2]!=p) {print $1, p"->"d[2]; p=d[2]}}' evidence/exp_c_hpa_timeline.csv
```
→ 19:46:26 2→3 (через 25с от начала нагрузки, CPU 141%); 19:56:58 3→2
(через 66с после снятия — окно стабилизации 60с).

Алерты (полный цикл pending→firing→resolved):
```
grep -m1 '"state": "pending"' evidence/exp_c_alerts_firing2.jsonl
grep -m1 '"state": "firing"' evidence/exp_c_alerts_firing2.jsonl
grep -c '"alerts": \[\]' evidence/exp_c_alerts_firing2.jsonl   # записей с обоими алертами в normal
```
Показать `charts/chart4_hpa.png`.

## Сцена 6. Реальная модель — 5:00–5:40

`head -1 results/requests_c0_w0_real.jsonl | ./.venv/Scripts/python.exe -m json.tool`
→ model_version Qwen/Qwen3-0.6B, backend_mode transformers.

Сказать: веса берутся из кэша HuggingFace через hostPath PV, SHA-256 файлов
артефакта сверяется до загрузки (`artifacts/real/accepted.json`). Результаты:
22/22 ok, TTFT p50 0.16с, TPOT 0.175с/токен, 5.7 tokens/s. Измерения
имитатора и модели в отчёте разделены.

## Сцена 7. Собственные проверки — 5:40–6:30

Эконом (10 секунд): `grep -E "\[check|Ran |OK" evidence/checks-green-final.log`
Полный live (~2 минуты, добавить к хронометражу): `make checks`

Юнит-тесты сервиса (16с, можно live): `make check`

## Сцена 8. Размер стенда и удаление — 6:30–7:00

`cat evidence/stand_size.txt` — образы 1.78+1.78 ГБ, нода ~25 ГБ с кэшем HF.
Показать команды удаления (README, раздел «Удаление»):
`make clean && docker rmi hw11-service:1.0.0 hw11-service:real-1.0.0`
Сказать: «полное удаление освобождает ~30 ГБ; пересоздание — по README,
make cluster → make build → make load → make deploy».

Финал: «Отчёт — report.html, сырые данные — results/, журналы опытов —
evidence/. Спасибо за внимание».
