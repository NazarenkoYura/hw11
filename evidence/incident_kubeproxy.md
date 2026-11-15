# Инцидент стенда 2026-11-15: kube-proxy RULE_APPEND (WSL2/Docker Desktop)

Симптом: kube-proxy v1.34 (iptables mode) в kind-ноде на Docker Desktop
WSL2-ядро 5.15.167.4 периодически падал на iptables-restore:
  RULE_APPEND failed (No such file or directory): rule in chain KUBE-SVC-*
  Warning: Extension recent is not supported, missing kernel module?
Следствие: ClusterIP-маршрутизация не работала (под-под — работал).

Диагностика: kubectl -n kube-system logs -l k8s-app=kube-proxy
Проверка модулей ядра: cat /proc/net/ip_tables_matches (WIP: statistic,
recent отсутствовал до перезапуска kube-proxy).

Исправление (воспроизводится скриптом scripts/fix_kubeproxy.sh):
  wsl -d docker-desktop -u root modprobe xt_statistic xt_recent
  kubectl -n kube-system delete pod -l k8s-app=kube-proxy
  повторять, пока в логах kube-proxy не исчезнет Sync failed.
  После успешной синхронизации правила Service сохраняются.

Итог: kube-proxy Running, 0 ошибок синхронизации, все Service работают.
