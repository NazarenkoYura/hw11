#!/usr/bin/env bash
# Исправление kube-proxy на Docker Desktop/WSL2: RULE_APPEND в iptables-restore.
# Подробности: evidence/incident_kubeproxy.md
set -uo pipefail
wsl -d docker-desktop -u root modprobe xt_statistic 2>/dev/null || true
wsl -d docker-desktop -u root modprobe xt_recent 2>/dev/null || true
for i in 1 2 3 4 5; do
  kubectl -n kube-system delete pod -l k8s-app=kube-proxy --ignore-not-found >/dev/null 2>&1
  sleep 45
  if ! kubectl -n kube-system logs -l k8s-app=kube-proxy --since=1m 2>/dev/null \
       | grep -qE "RULE_APPEND|Sync failed"; then
    echo "kube-proxy: синхронизация успешна (попытка $i)"
    exit 0
  fi
  echo "kube-proxy: Sync failed (попытка $i), перезапуск"
done
echo "kube-proxy: не удалось восстановить за 5 попыток" >&2
exit 1
