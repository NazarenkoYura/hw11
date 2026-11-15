#!/usr/bin/env bash
# Опыт B (продолжение): комбинации с перекосом и affinity — конкурентные клиенты
# (--workers 4: у каждого клиента 4 параллельных соединения, иначе одиночный
# последовательный клиент не способен нагрузить реплику выше ~1/время_ответа).
# plain-uniform-ka выполнен ранее (workers=1 достаточно для 1.5 rps/клиент).
set -uo pipefail
cd "$(dirname "$0")/.."
LOG=evidence/exp_b_runs.log

wait_quiescence() {
  # Ждём: ровно 2 готовых Pod'а сервиса, ни одного Terminating, один активный RS.
  for i in $(seq 1 40); do
    n_term=$(kubectl get pods -l 'app=hw11,role!=checks' --no-headers 2>/dev/null | grep -c Terminating)
    n_ready=$(kubectl get pods -l 'app=hw11,role!=checks' --no-headers 2>/dev/null | grep -c " 1/1 ")
    n_all=$(kubectl get pods -l 'app=hw11,role!=checks' --no-headers 2>/dev/null | wc -l)
    n_rs=$(kubectl get rs -l app=hw11 --no-headers 2>/dev/null | awk '$2>0' | wc -l)
    if [ "$n_term" -eq 0 ] && [ "$n_ready" -eq 2 ] && [ "$n_all" -eq 2 ] && [ "$n_rs" -eq 1 ]; then
      return 0
    fi
    sleep 3
  done
  echo "WARNING: quiescence timeout (term=$n_term ready=$n_ready all=$n_all rs=$n_rs)" >> "$LOG"
  return 1
}

run_combo() {
  local label="$1" svc="$2" profile="$3" ka="$4" workers="$5"
  for run in 1 2 3; do
    local full="${label}-${run}"
    echo "=== $(date -u +%H:%M:%SZ) RUN $full svc=$svc profile=$profile keepalive=$ka workers=$workers ===" >> "$LOG"
    kubectl rollout restart deployment/hw11 >/dev/null 2>&1
    kubectl rollout status deployment/hw11 --timeout=180s >/dev/null 2>&1
    wait_quiescence || true
    sleep 3
    local clients
    clients=$(kubectl get endpoints "$svc" -o jsonpath='{.subsets[0].addresses[*].ip}' 2>/dev/null | wc -w)
    echo "endpoints_ready=$clients" >> "$LOG"
    for c in 0 1 2 3; do
      kubectl delete pod "loadgen-$full-c$c" --ignore-not-found >/dev/null 2>&1
      kubectl run "loadgen-$full-c$c" --image=hw11-service:1.0.0 --restart=Never \
        --labels=role=loadgen \
        --command -- sh -c "python scripts/loadgen.py --url http://$svc:8000 \
          --client $c --clients 4 --profile $profile --rate 6 --duration 120 \
          --tokens 32 --sessions 8 $ka --workers $workers --out /tmp/results \
          --label $full && sleep 600" >/dev/null 2>&1
    done
    sleep 130
    for c in 0 1 2 3; do
      MSYS_NO_PATHCONV=1 kubectl cp "loadgen-$full-c$c:/tmp/results" \
        "results/eB" >/dev/null 2>&1
      kubectl delete pod "loadgen-$full-c$c" --force --grace-period=0 >/dev/null 2>&1
    done
    .venv/Scripts/python.exe scripts/summarize_run.py "results/eB" "$full" >> "$LOG" 2>&1
    tail -1 "$LOG"
  done
}

run_combo "plain-skew-ka"      hw11-svc          skew    --keepalive    4
run_combo "affin-uniform-noka" hw11-svc-affinity uniform --no-keepalive 4
run_combo "affin-skew-noka"    hw11-svc-affinity skew    --no-keepalive 4
run_combo "plain-skew-noka"    hw11-svc          skew    --no-keepalive 4

echo "EXP B PART2 DONE $(date -u +%H:%M:%SZ)" >> "$LOG"
