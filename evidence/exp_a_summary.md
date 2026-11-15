# Опыт A: прогрев, выпуск и откат — сводка результатов

Стенд: kind hw11, модель sim-A → sim-B, прогрев 20с, max_parallel 2, readiness
/readyz, startup-прооба с запасом 90с, terminationGracePeriodSeconds 45,
preStop 5с. Монитор Ready/EndpointSlice: evidence/exp_a1_timeline.csv (1с).

## A.1 Исходный дефект: readiness до прогрева (READY_EAGER=1)

Гипотеза: если /readyz отвечает 200 сразу после старта HTTP-процесса, новый Pod
получает рабочий трафик во время прогрева и отвечает непрогретым backend.

Воспроизведение: rolling update под нагрузкой (loadgen 2 rps, 4 клиента,
без keep-alive; helm upgrade → READY_EAGER=1). Результаты клиента
(results/requests_a1_defect.jsonl):
- 40/40 успешных HTTP, из них **7 ответов от непрогретого backend**
  (unwarmed=true, ответ начинается с «UNWARMED sim-A:», счётчик
  hw11_unwarmed_total вырос);
- все 7 — на новых Pod'ах нового RS (hw11-dc448f9bb-*), т.е. именно
  замещающие Pod'ы отдавали трафик до завершения прогрева;
- серверный журнал: evidence/exp_a1_unwarmed_serverlog.jsonl
  (outcome=unwarmed, stream-запросы).

Исправление: /readyz=200 только после load_ok + прогрева (warmup_finished).
Повтор той же нагрузки при включённом исправлении
(results/requests_a1b_fixed.jsonl):
- 40/40 успешных, **0 непрогретых ответов**, 0 ошибок;
- новые Pod'ы (dc448f9bb-jpdq5/g7bvb) начали получать трафик только после
  прогрева (Ready → EndpointSlice), до этого весь трафик на старых Pod'ах.

Red/green через собственные проверки: evidence/checks-red-defect.log —
`FAIL: test_10 ... 200 != 503 : новый Pod отчитался готовностью до конца
прогрева`; evidence/checks-green-fixed.log — после исправления 5/5 OK
(проверка №1: `/readyz=503 во время прогрева, новый Pod вне EndpointSlice,
версия и runtime_sha256 восстановлены`).

## A.2 Rolling update A→B при активных стримах

helm upgrade modelVersion=B (прогрев новой версии 20с), нагрузка — потоки по
16с (32 части × 0.5с), rate 0.25 rps + общий поток. Результаты
(results/requests_a2_rolloutAB.jsonl):
- **0 ошибок / 0 обрывов**, все стримы завершены (данные ok=6: 2 на старом
  RS, 4 на новом);
- версии по фактическим ответам: sim-A → sim-B (клиент видит model_version
  в каждом ответе);
- старые Pod'ы дозавершали активные стримы в grace period (45с),
  preStop 5с даёт EndpointSlice время убрать Pod до SIGTERM;
- лента Ready: ready-реплик не опускалось ниже 2 (maxUnavailable=0,
  maxSurge=1): evidence/exp_a1_timeline.csv.

## A.3 Заведомо неготовая B и управляемый откат

helm upgrade WARMUP_NEVER=1: новый Pod hw11-7747886446-m2xbt загрузил артефакт
sim-B (load_ok) и удерживал прогрев бесконечно (журнал: evidence/exp_a3_badb_log.jsonl,
`warmup_withheld`):
- Pod 0/1 Running, /readyz=503, **в EndpointSlice не попал**, 0 запросов от
  клиентов за всё время (в записях loadgen его Pod отсутствует);
- рабочий трафик продолжался на старых Pod'ах без ошибок;
- `helm rollback 26 --wait` — откат завершился, неготовый Pod Terminating,
  нагрузка (results/requests_a3_rollback.jsonl): 7/7 ok, 0 ошибок,
  версия по фактическим ответам **sim-B** (ожидаемая версия отката — фикс B),
  runtime_sha256 = a625e391… соответствует запечённому артефакту.

## A.4 Принудительное завершение процесса

Убийство PID1 изнутри namespace невозможно (ядро игнорирует SIGKILL для PID1
из своего namespace) — принудительная остановка контейнера снаружи через
crictl stop -t 0 (kind-нода), 2 сценария:

1. Общий поток (results/requests_a4c_general.jsonl): 6/6 ok — во время
   недоступности одного Pod'а весь трафик обслуживал второй.
2. Детерминированный стрим напрямую в Pod-жертву (минуя балансировщик):
   evidence/exp_a4_kill_stream2.txt — стрим начался в 17:37:24, контейнер
   принудительно остановлен в 17:37:31, **клиент получил 15 из 32 частей,
   затем EOF** (соединение оборвано принудительно).
   Повтор: у стороннего клиента обрывов нет.

Обнаружение отказа и восстановление: контейнер перезапущен kubelet'ом
(Restart Count +1, события Killing/Started в evidence/exp_a_kill.log),
Pod 0/1 на время прогрева (startup probe 503 → evidence/exp_a_kill_restart.txt),
затем Ready (kubectl wait условие выполнено); EndpointSlice убрал Pod до
полной остановки процесса — новые соединения сразу шли на живую реплику.

## Ограничения опыта A

- Неготовые ответы имитатора детерминированы и помечены «UNWARMED» — у
  реальной модели признак непрогретости может отсутствовать, поэтому
  единственная защита — готовность по факту прогрева.
- В одном эпизоде A.1 старые Pod'ы перечитали обновлённый ConfigMap после
  рестарта контейнера (env фиксирован на момент старта процесса) —
  флаги READY_EAGER/WARMUP_NEVER продублированы напрямую в шаблоне Pod.
- crictl stop используется как замена «kill процесса снаружи» — в
  продакшене аналогом является отказ ноды/OOM-killer.
