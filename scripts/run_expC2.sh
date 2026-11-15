#!/usr/bin/env bash
# Опыт C (повтор с конкурентной нагрузкой): полный цикл HPA 2→3→2 и алерты.
# Клиенты конкурентные (--workers): одиночный клиент ограничен ~1/время_ответа
# и не создаёт ни CPU-нагрузки, ни очереди. TTFC сервера включает очередь.
set -uo pipefail
cd "$(dirname "$0")/.."
LOG=evidence/exp_c.log

echo "=== $(date -u +%H:%M:%SZ) C2: исходное состояние (HPA min 2) ===" >> "$LOG"
kubectl get hpa hw11 >> "$LOG" 2>&1

# Фаза 1: 3 rps, вычислительная нагрузка — CPU выше цели → 2→3; p95 TTFC>2с → алерт 1
echo "=== $(date -u +%H:%M:%SZ) C2 фаза 1: 3 rps × workers=8 (масштаб 2→3, алерт TTFC) ===" >> "$LOG"
kubectl delete pod loadgen-c1 --ignore-not-found --force --grace-period=0 >/dev/null 2>&1 || true
kubectl run loadgen-c1 --image=hw11-service:1.0.0 --restart=Never --labels=role=loadgen \
  --command -- sh -c "python scripts/loadgen.py --url http://hw11-svc:8000 \
  --rate 3 --duration 300 --tokens 32 --workers 8 --out /tmp/results --label c1 \
  --no-keepalive && sleep 1800" >/dev/null 2>&1
date -u +"c2_phase1_utc=%Y-%m-%dT%H:%M:%SZ" >> "$LOG"
sleep 330
kubectl get hpa hw11 >> "$LOG" 2>&1

# Фаза 2: 12 rps × workers=40 — очередь, 503 → алерт 2 (доля отказов)
echo "=== $(date -u +%H:%M:%SZ) C2 фаза 2: 12 rps × workers=40 (алерт ошибок) ===" >> "$LOG"
kubectl run loadgen-c2 --image=hw11-service:1.0.0 --restart=Never --labels=role=loadgen \
  --command -- sh -c "python scripts/loadgen.py --url http://hw11-svc:8000 \
  --rate 12 --duration 240 --tokens 32 --workers 40 --out /tmp/results --label c2 \
  --no-keepalive && sleep 1800" >/dev/null 2>&1
date -u +"c2_phase2_utc=%Y-%m-%dT%H:%M:%SZ" >> "$LOG"
sleep 260
kubectl get hpa hw11 >> "$LOG" 2>&1

# Фаза 3: снятие нагрузки → resolve алертов, 3→2 (окно стабилизации 60с)
echo "=== $(date -u +%H:%M:%SZ) C2 фаза 3: снятие нагрузки ===" >> "$LOG"
kubectl delete pod loadgen-c1 loadgen-c2 --force --grace-period=0 >/dev/null 2>&1 || true
date -u +"load_stopped_utc=%Y-%m-%dT%H:%M:%SZ" >> "$LOG"
sleep 420
kubectl get hpa hw11 >> "$LOG" 2>&1
kubectl get pods -l 'app=hw11,role!=checks' --no-headers >> "$LOG" 2>&1
echo "EXP C2 DONE $(date -u +%H:%M:%SZ)" >> "$LOG"
