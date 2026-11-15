#!/usr/bin/env bash
# Опыт C: HPA 2→3→2 по CPU + два алерта Prometheus (pending/firing/resolved).
# Запускается ПОСЛЕ опыта B (стенд на modelVersion=B, дефолтные части).
set -uo pipefail
cd "$(dirname "$0")/.."
LOG=evidence/exp_c.log
: > "$LOG"

echo "=== $(date -u +%H:%M:%SZ) Включаем HPA + вычислительную нагрузку (40мс/токен) ===" >> "$LOG"
kubectl delete job hw11-checks --ignore-not-found >/dev/null 2>&1 || true
helm.exe upgrade hw11 chart --reuse-values --set checks.enabled=false \
  --set hpa.enabled=true --set backend.cpuBurnMsPerToken=40 >> "$LOG" 2>&1
kubectl rollout status deployment/hw11 --timeout=180s >> "$LOG" 2>&1
kubectl get hpa hw11 >> "$LOG" 2>&1

# Фаза 1: умеренная нагрузка 2.5 rps — CPU растёт, p95 TTFC > 2с (алерт 1)
echo "=== $(date -u +%H:%M:%SZ) Фаза 1: нагрузка 2.5 rps (TTFC-алерт) ===" >> "$LOG"
kubectl delete pod loadgen-c1 --ignore-not-found >/dev/null 2>&1 || true
kubectl run loadgen-c1 --image=hw11-service:1.0.0 --restart=Never --labels=role=loadgen \
  --command -- sh -c "python scripts/loadgen.py --url http://hw11-svc:8000 \
  --rate 2.5 --duration 300 --tokens 32 --out /tmp/results --label c1 --no-keepalive && sleep 1200" >/dev/null 2>&1
sleep 210
kubectl get hpa hw11 >> "$LOG" 2>&1
kubectl get pods -l app=hw11 --no-headers >> "$LOG" 2>&1

# Фаза 2: перегрузка 12 rps — очередь и 503 (алерт 2)
echo "=== $(date -u +%H:%M:%SZ) Фаза 2: перегрузка 12 rps (ErrorRate-алерт) ===" >> "$LOG"
kubectl run loadgen-c2 --image=hw11-service:1.0.0 --restart=Never --labels=role=loadgen \
  --command -- sh -c "python scripts/loadgen.py --url http://hw11-svc:8000 \
  --rate 12 --duration 180 --tokens 32 --out /tmp/results --label c2 --no-keepalive && sleep 1200" >/dev/null 2>&1
sleep 200
kubectl get hpa hw11 >> "$LOG" 2>&1

# Фаза 3: остановка нагрузки — алерты resolve, масштабирование 3→2
echo "=== $(date -u +%H:%M:%SZ) Фаза 3: снятие нагрузки (resolve + 3→2) ===" >> "$LOG"
kubectl delete pod loadgen-c1 loadgen-c2 --force --grace-period=0 >/dev/null 2>&1 || true
date -u +"load_stopped_utc=%Y-%m-%dT%H:%M:%SZ" >> "$LOG"
sleep 360
echo "=== $(date -u +%H:%M:%SZ) итог:" >> "$LOG"
kubectl get hpa hw11 >> "$LOG" 2>&1
kubectl get pods -l app=hw11 --no-headers >> "$LOG" 2>&1
echo "EXP C LOAD PHASES DONE $(date -u +%H:%M:%SZ)" >> "$LOG"
