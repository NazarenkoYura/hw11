"""Собственные проверки hw11 (п.6 задания, минимум пять).

Исполняются Job'ом ВНУТРИ кластера (образ hw11-service, доступ к Service
hw11-svc) либо с хоста (TARGET_URL, управление Pod через kubectl).

  1. warmup_gate           — новый Pod недоступен рабочему трафику во время
                             прогрева (red на дефекте READY_EAGER=1);
  2. version_after_rollback— после отката ответы отдают ожидаемую версию и
                             runtime_sha256 запечённого артефакта;
  3. recovery_after_replacement — замена Pod: тот же артефакт/версия, диалог
                             продолжается на новой реплике (история в запросе);
  4. failures_accounting   — отказы/оборванные потоки учитываются отдельными
                             исходами в /metrics (client_error, interrupted);
  5. dialog_isolation      — два диалога с различимыми контрольными строками
                             не смешиваются (история передаётся в запросе).

Запуск:
  python checks/checks.py            (mode=job внутри кластера)
  python -m unittest -v checks.checks
"""
import json
import os
import ssl
import sys
import time
import unittest
import urllib.parse
import urllib.request
from pathlib import Path

import requests

TARGET_URL = os.getenv("TARGET_URL", "http://hw11-svc:8000")
EXPECTED_VERSION = os.getenv("EXPECTED_VERSION", "sim-A")
WARMUP_SECONDS = float(os.getenv("WARMUP_SECONDS", "20"))
DEPLOYMENT = os.getenv("DEPLOYMENT", "hw11")
LABEL = os.getenv("POD_LABEL", "app=hw11")

IN_CLUSTER = Path("/var/run/secrets/kubernetes.io/serviceaccount/token").exists()
K8S_API = "https://kubernetes.default.svc"


def k8s(method: str, path: str, body: dict | None = None) -> dict | None:
    """Клиент API кластера: token ServiceAccount (job) или kubectl (host)."""
    if IN_CLUSTER:
        token = Path("/var/run/secrets/kubernetes.io/serviceaccount/token").read_text()
        ca = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        ctx = ssl.create_default_context(cafile=ca)
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(K8S_API + path, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, context=ctx, timeout=30) as response:
            code = response.status
            payload = response.read()
        if code >= 300:
            raise RuntimeError(f"k8s API {method} {path}: HTTP {code}")
        return json.loads(payload) if payload else None
    import subprocess
    if method.upper() == "DELETE":
        name = path.split("/")[-1]
        result = subprocess.run(["kubectl", "delete", "pod", name, "-n", "default"],
                                capture_output=True, text=True, timeout=90)
        if result.returncode != 0:
            raise RuntimeError(f"kubectl delete pod {name}: {result.stderr[:200]}")
        return None
    result = subprocess.run(["kubectl", "get", "pods", "-n", "default", "-l", LABEL,
                             "-o", "json"], capture_output=True, text=True, timeout=60)
    return json.loads(result.stdout)


def hw11_pods() -> list[dict]:
    if IN_CLUSTER:
        data = k8s("GET", "/api/v1/namespaces/default/pods?labelSelector=app%3Dhw11")
    else:
        data = k8s("GET", "")
    # Pod'ы самих проверок (role=checks) — не часть сервиса.
    return [item for item in data.get("items", [])
            if item.get("metadata", {}).get("labels", {}).get("role") != "checks"]


def pods_ready() -> list[str]:
    out = []
    for item in hw11_pods():
        cond = {c["type"]: c["status"] for c in item.get("status", {}).get("conditions", [])}
        if cond.get("Ready") == "True":
            out.append(item["metadata"]["name"])
    return out


def pod_ip(name: str) -> str | None:
    for item in hw11_pods():
        if item["metadata"]["name"] == name:
            return item.get("status", {}).get("podIP")
    return None


def delete_pod(name: str) -> None:
    if IN_CLUSTER:
        k8s("DELETE", f"/api/v1/namespaces/default/pods/{name}",
            body={"apiVersion": "v1", "kind": "DeleteOptions"})
    else:
        delete_pod_host(name)
        return


def delete_pod_host(name: str) -> None:
    import subprocess
    subprocess.run(["kubectl", "delete", "pod", name, "-n", "default", "--wait=false"],
                   capture_output=True, timeout=60)


def wait_ready_pods(min_count: int, timeout_s: float = 120) -> list[str]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        ready = pods_ready()
        if len(ready) >= min_count:
            return ready
        time.sleep(2)
    return pods_ready()


def endpoints_ready_ips(service_name: str = "hw11-svc") -> list[str]:
    """IP адресов с ready=True в EndpointSlice сервиса (рабочий трафик)."""
    sel = "kubernetes.io/service-name=" + service_name
    if IN_CLUSTER:
        path = ("/apis/discovery.k8s.io/v1/namespaces/default/endpointslices"
                "?labelSelector=" + urllib.parse.quote(sel, safe=""))
        data = k8s("GET", path)
    else:
        import subprocess
        result = subprocess.run(["kubectl", "get", "endpointslices", "-n", "default",
                                 "-l", sel, "-o", "json"],
                                capture_output=True, text=True, timeout=60)
        data = json.loads(result.stdout)
    ips = []
    for ep_item in data.get("items", []):
        for e in ep_item.get("endpoints", []):
            if e.get("conditions", {}).get("ready"):
                ips.extend(e.get("addresses", []))
    return ips


def expected_hash() -> str:
    """runtime_sha256 ожидаемой версии — из запечённого артефакта этого же образа."""
    letter = EXPECTED_VERSION[-1]
    manifest = json.loads(
        Path(f"/app/artifacts/sim/{letter}/accepted.json").read_text(encoding="utf-8"))
    return manifest["runtime_sha256"]


def metrics_counters(_base: str | None = None) -> dict:
    """Сумма hw11_requests_total по ВСЕМ готовым Pod (запрос через Service
    попадает в одну реплику — исход мог засчитаться на другой)."""
    counters: dict = {}
    for item in hw11_pods():
        cond = {c["type"]: c["status"] for c in item.get("status", {}).get("conditions", [])}
        ip = item.get("status", {}).get("podIP")
        if cond.get("Ready") != "True" or not ip:
            continue
        try:
            text = requests.get(f"http://{ip}:8000/metrics", timeout=5).text
        except requests.RequestException:
            continue
        for line in text.splitlines():
            if line.startswith("hw11_requests_total{"):
                outcome = line.split('outcome="')[1].split('"')[0]
                counters[outcome] = counters.get(outcome, 0) + float(
                    line.rsplit(" ", 1)[1])
    return counters


def first_readyz_response(ip: str, wait_s: float = 30) -> int:
    """Первый успешный HTTP-ответ /readyz у Pod (пока uvicorn не слушает —
    connection refused; ждём первый ответ, не дольше wait_s)."""
    deadline = time.time() + wait_s
    while time.time() < deadline:
        try:
            return requests.get(f"http://{ip}:8000/readyz", timeout=4).status_code
        except requests.RequestException:
            time.sleep(0.5)
    raise RuntimeError(f"{ip}: нет HTTP-ответа за {wait_s}с")


class Hw11Checks(unittest.TestCase):
    """Пять проверок против сервиса, развёрнутого в Kubernetes."""

    def test_10_warmup_gate_and_version_restore(self):
        """Проверки 1 и часть 3: во время прогрева новый Pod вне трафика."""
        ready = wait_ready_pods(2)
        self.assertGreaterEqual(len(ready), 2, "стенд не в рабочем состоянии")
        victim = ready[0]
        before_ip = pod_ip(victim)
        delete_pod(victim)
        # Новый Pod появится вместо удалённого (не из набора старых).
        new_pod, new_ip = None, None
        deadline = time.time() + 60
        while time.time() < deadline and new_pod is None:
            known = set(ready) | {victim}
            for item in hw11_pods():
                name = item["metadata"]["name"]
                if name not in known and item.get("status", {}).get("podIP"):
                    new_pod = name
                    new_ip = item["status"]["podIP"]
                    break
            time.sleep(1)
        self.assertIsNotNone(new_pod, "заменяющий Pod не появился")
        print(f"[check1] удалён {victim} ({before_ip}); заменяющий {new_pod} ({new_ip})")
        # Первый HTTP-ответ нового Pod: пока uvicorn не слушает — connection
        # refused, первый успешный ответ приходит ДО конца прогрева (сервис
        # начинает слушать раньше, чем завершается прогрев).
        first_status = first_readyz_response(new_ip, wait_s=30)
        if WARMUP_SECONDS > 5:
            self.assertEqual(first_status, 503,
                             "новый Pod отчитался готовностью до конца прогрева "
                             "(дефект READY_EAGER?)")
            ips = endpoints_ready_ips()
            self.assertNotIn(new_ip, ips,
                             "непрогретый Pod уже в рабочих EndpointSlice")
            print(f"[check1] прогрев {WARMUP_SECONDS}с: новый Pod вне Endpoints, /readyz=503")
        # После прогрева: готов, в Endpoints, та же версия артефакта.
        deadline = time.time() + 120
        ready_ok = False
        while time.time() < deadline:
            try:
                if requests.get(f"http://{new_ip}:8000/readyz", timeout=5).status_code == 200:
                    ready_ok = True
                    break
            except requests.ConnectionError:
                pass
            time.sleep(2)
        self.assertTrue(ready_ok, "новый Pod так и не стал готов")
        body = requests.post(f"http://{new_ip}:8000/generate", timeout=60, json={
            "prompt": "version-restore probe", "max_new_tokens": 2}).json()
        self.assertEqual(body["model_id"], EXPECTED_VERSION,
                         "версия артефакта не восстановилась после замены Pod")
        self.assertEqual(body["runtime_sha256"], expected_hash(),
                         "runtime_sha256 не совпадает с запечённым артефактом")
        print(f"[check1/3] новый Pod готов: {body['model_version']} "
              f"sha={body['runtime_sha256'][:12]}… восстановлен")

    def test_20_version_after_rollback_via_service(self):
        """Проверка 2: через Service — ожидаемая версия и хеш после отката."""
        body = requests.post(TARGET_URL + "/generate", timeout=60, json={
            "prompt": "rollback version probe", "max_new_tokens": 2}).json()
        self.assertEqual(body["model_id"], EXPECTED_VERSION)
        self.assertEqual(body["runtime_sha256"], expected_hash())
        self.assertEqual(body["backend_mode"], "simulated")
        print(f"[check2] Service отвечает {body['model_version']} "
              f"sha={body['runtime_sha256'][:12]}… (после отката)")

    def test_30_dialog_continues_after_pod_replacement(self):
        """Проверка 3 (продолжение): диалог продолжается после замены реплики."""
        session = f"recovery-{int(time.time())}"
        first = requests.post(TARGET_URL + "/generate", timeout=60, json={
            "prompt": "RECOVERY-DIALOG первый ход", "max_new_tokens": 4,
            "session_id": session, "turn": 0}).json()
        pod_before = first["pod"]
        delete_pod(pod_before)
        wait_ready_pods(2, timeout_s=120)
        # Диалог продолжается ПОСЛЕ замены реплики: следующий ход может
        # обслужить только новая/выжившая реплика — под_before больше не
        # принимает рабочий трафик, если его Pod был заменён.
        second = requests.post(TARGET_URL + "/generate", timeout=60, json={
            "prompt": "RECOVERY-DIALOG первый ход", "max_new_tokens": 4,
            "session_id": session, "turn": 1,
            "history": [{"role": "user", "content": "RECOVERY-DIALOG первый ход"}]}).json()
        self.assertEqual(second["model_id"], first["model_id"],
                         "версия изменилась после замены реплики")
        self.assertIn("RECOVERY-DIALOG", second["answer"])
        self.assertTrue(second["ready"])
        if second["pod"] == pod_before:
            # Pod удалён — имя не могло сохраниться; повторить, пока не попадём
            # на заменяющую реплику (kube-proxy может послать и на выжившую).
            for _ in range(6):
                pod_before2 = second["pod"]
                delete_pod(pod_before2)
                wait_ready_pods(2, timeout_s=120)
                second = requests.post(TARGET_URL + "/generate", timeout=60, json={
                    "prompt": "RECOVERY-DIALOG первый ход", "max_new_tokens": 4,
                    "session_id": session, "turn": 2,
                    "history": [{"role": "user",
                                 "content": "RECOVERY-DIALOG первый ход"}]}).json()
                if second["pod"] != pod_before2:
                    break
        self.assertNotEqual(second["pod"], pod_before,
                            "диалог не переехал на новую реплику после замены Pod")
        print(f"[check3] диалог продолжился: pod {pod_before} -> {second['pod']} "
              f"(версия {second['model_version']} сохранена)")

    def test_40_failures_accounting(self):
        """Проверка 4: 4xx и оборванный поток учитываются отдельными исходами."""
        base_counters = metrics_counters(TARGET_URL)
        # 400 до backend — отдельный исход client_error
        bad = requests.post(TARGET_URL + "/generate", timeout=30,
                            json={"prompt": "", "max_new_tokens": 1})
        self.assertEqual(bad.status_code, 400)
        # Обрыв потокового ответа после первой части: raw socket с резким FIN —
        # requests.close() может вернуть соединение в пул (keep-alive) без FIN.
        import socket as _socket
        host = urllib.parse.urlparse(TARGET_URL).hostname
        port = urllib.parse.urlparse(TARGET_URL).port or 8000
        sock = _socket.create_connection((host, port), timeout=15)
        body = json.dumps({"prompt": "interrupt-me stream", "max_new_tokens": 32,
                           "stream": True}).encode()
        req = (b"POST /generate HTTP/1.1\r\nHost: " + host.encode()
               + b"\r\nContent-Type: application/json\r\nContent-Length: "
               + str(len(body)).encode() + b"\r\n\r\n" + body)
        sock.sendall(req)
        data = b""
        while b'"chunk_index": 0' not in data:
            data += sock.recv(4096)
        self.assertTrue(b'"chunk_index": 0' in data, "не дождались первой части потока")
        time.sleep(0.2)
        sock.close()  # клиент разрывает соединение посреди потока
        # Счётчики исходов увеличиваются раздельно (poll до 15с)
        deadline = time.time() + 15
        ok = False
        while time.time() < deadline:
            counters = metrics_counters()
            got_interrupted = (counters.get("interrupted", 0)
                               > base_counters.get("interrupted", 0))
            got_client_error = (counters.get("client_error", 0)
                                > base_counters.get("client_error", 0))
            if got_interrupted and got_client_error:
                ok = True
                break
            time.sleep(1)
        self.assertTrue(ok, "исходы client_error/interrupted не учтены в /metrics")
        print(f"[check4] учёты: client_error и interrupted инкрементированы "
              f"(retry клиента фиксируется отдельной попыткой в loadgen)")

    def test_50_dialog_isolation(self):
        """Проверка 5: два диалога с контрольными строками не смешиваются."""
        alpha = f"iso-alpha-{int(time.time())}"
        beta = f"iso-beta-{int(time.time())}"
        a0 = requests.post(TARGET_URL + "/generate", timeout=60, json={
            "prompt": "АЛЬФА-КОНТРОЛЬ уникальная строка альфа", "max_new_tokens": 6,
            "session_id": alpha, "turn": 0}).json()
        b0 = requests.post(TARGET_URL + "/generate", timeout=60, json={
            "prompt": "БЕТА-КОНТРОЛЬ уникальная строка бета", "max_new_tokens": 6,
            "session_id": beta, "turn": 0}).json()
        a1 = requests.post(TARGET_URL + "/generate", timeout=60, json={
            "prompt": "АЛЬФА-КОНТРОЛЬ уникальная строка альфа", "max_new_tokens": 6,
            "session_id": alpha, "turn": 1,
            "history": [{"role": "user",
                         "content": "АЛЬФА-КОНТРОЛЬ уникальная строка альфа"}]}).json()
        b1 = requests.post(TARGET_URL + "/generate", timeout=60, json={
            "prompt": "БЕТА-КОНТРОЛЬ уникальная строка бета", "max_new_tokens": 6,
            "session_id": beta, "turn": 1,
            "history": [{"role": "user",
                         "content": "БЕТА-КОНТРОЛЬ уникальная строка бета"}]}).json()
        self.assertIn("АЛЬФА-КОНТРОЛЬ", a1["answer"])
        self.assertIn("БЕТА-КОНТРОЛЬ", b1["answer"])
        self.assertNotIn("БЕТА-КОНТРОЛЬ", a1["answer"])
        self.assertNotIn("АЛЬФА-КОНТРОЛЬ", b1["answer"])
        print(f"[check5] диалоги изолированы: alpha pod={a1['pod']} (cache {a1['cache_result']}), "
              f"beta pod={b1['pod']} (cache {b1['cache_result']})")


if __name__ == "__main__":
    unittest.main(verbosity=2)
