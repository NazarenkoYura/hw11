"""Юнит-проверки сервиса hw11 (in-process, без кластера).

Запуск: python -m unittest tests.test_service -v
Проверяются контракты, унаследованные из ДЗ 8+9+10, и новое hw11:
валидация до backend, версия артефакта по хешу, eager-readiness дефект,
стриминг частями, префиксный кеш (hit/miss), учёт исходов в /metrics.
"""
import json
import os
import sys
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import service  # noqa: E402

ARTIFACTS = ROOT / "artifacts" / "sim"


def make_client(workspace: Path, warmup: float = 0.2, **kwargs) -> TestClient:
    app = service.create_app(workspace, warmup_seconds=warmup, **kwargs)
    return TestClient(app)


class ServiceContract(unittest.TestCase):
    """Контракты ДЗ 8+9+10, продолженные в hw11."""

    def test_01_not_ready_gate_and_health(self):
        with make_client(ARTIFACTS / "A", warmup=3.0) as client:
            health = client.get("/health").json()
            self.assertTrue(health["alive"])
            self.assertFalse(health["ready"])
            r = client.post("/generate", json={"prompt": "x", "max_new_tokens": 1})
            self.assertEqual(r.status_code, 503)
            ready = client.get("/ready")
            self.assertEqual(ready.status_code, 503)
            self.assertFalse(ready.json()["ready"])

    def test_02_validation_before_backend(self):
        with make_client(ARTIFACTS / "A", warmup=0.2) as client:
            # ждём готовность
            for _ in range(100):
                if client.get("/health").json()["ready"]:
                    break
                import time
                time.sleep(0.1)
            calls_before = client.get("/debug").json()["calls"]
            for bad in ({}, {"prompt": ""}, {"prompt": "x", "max_new_tokens": 0},
                        {"prompt": "x", "max_new_tokens": True},
                        {"prompt": "y" * 3000}, {"prompt": 5},
                        {"prompt": "x", "session_id": ""},
                        {"prompt": "x", "turn": -1}):
                r = client.post("/generate", json=bad)
                self.assertEqual(r.status_code, 400, f"payload={bad}")
            self.assertEqual(client.get("/debug").json()["calls"], calls_before)

    def test_03_response_contract_version_and_hash(self):
        manifest = json.loads((ARTIFACTS / "A" / "accepted.json").read_text(encoding="utf-8"))
        with make_client(ARTIFACTS / "A", warmup=0.2) as client:
            for _ in range(100):
                if client.get("/health").json()["ready"]:
                    break
                import time
                time.sleep(0.1)
            r = client.post("/generate", json={
                "prompt": "CONTROL-1 line", "max_new_tokens": 4,
                "session_id": "sessA", "turn": 0})
            body = r.json()
            self.assertEqual(r.status_code, 200)
            self.assertEqual(body["model_id"], "sim-A")
            self.assertEqual(body["model_version"], manifest["model_id"])
            self.assertEqual(body["runtime_sha256"], manifest["runtime_sha256"])
            self.assertEqual(body["backend_mode"], "simulated")
            self.assertIn("CONTROL-1 line", body["answer"])
            self.assertIn("request_id", body)
            self.assertIn("pod", body)
            self.assertGreaterEqual(body["chunks"], 1)
            self.assertEqual(body["cache_result"], "miss")  # ход 0 — всегда miss

    def test_04_stream_chunks_and_ttfc(self):
        with make_client(ARTIFACTS / "A", warmup=0.2) as client:
            for _ in range(100):
                if client.get("/health").json()["ready"]:
                    break
                import time
                time.sleep(0.1)
            events = []
            with client.stream("POST", "/generate", json={
                    "prompt": "CONTROL-STREAM", "max_new_tokens": 8,
                    "session_id": "s", "turn": 0, "stream": True}) as response:
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.headers["content-type"].startswith("text/event-stream"))
                for line in response.iter_lines():
                    if line.startswith("data: "):
                        events.append(json.loads(line[6:]))
            deltas = [e for e in events if "delta" in e]
            done = [e for e in events if e.get("done")]
            self.assertGreaterEqual(len(deltas), 4)
            self.assertTrue(deltas[0].get("first") or deltas[0]["chunk_index"] == 0)
            self.assertEqual(len(done), 1)
            self.assertEqual(done[0]["backend_mode"], "simulated")
            self.assertIn("CONTROL-STREAM", done[0]["answer"])
            self.assertGreater(done[0]["ttfc_seconds"], 0)


class WarmupDefect(unittest.TestCase):
    """Исходный дефект опыта A: READY_EAGER отвечает готовностью до прогрева."""

    def test_05_eager_readiness_serves_unwarmed(self):
        with make_client(ARTIFACTS / "A", warmup=3.0, ready_eager=True) as client:
            r = client.get("/readyz")
            self.assertEqual(r.status_code, 200)  # дефект: готов до прогрева
            body = client.post("/generate", json={"prompt": "x", "max_new_tokens": 1}).json()
            self.assertTrue(body["unwarmed"])
            self.assertIn("UNWARMED", body["answer"])
            # /debug различает «отчитанную» и фактическую готовность
            debug = client.get("/debug").json()
            self.assertTrue(debug["ready"])
            self.assertFalse(debug["ready_truthful"])

    def test_06_fixed_readiness_blocks_until_warmup(self):
        with make_client(ARTIFACTS / "A", warmup=1.5) as client:
            self.assertEqual(client.get("/readyz").status_code, 503)
            self.assertEqual(client.post("/generate",
                                         json={"prompt": "x", "max_new_tokens": 1}).status_code,
                             503)
            for _ in range(200):
                if client.get("/readyz").status_code == 200:
                    break
                import time
                time.sleep(0.05)
            self.assertEqual(client.get("/readyz").status_code, 200)
            self.assertEqual(client.post("/generate",
                                         json={"prompt": "x", "max_new_tokens": 1}).status_code,
                             200)


class CacheSimulation(unittest.TestCase):
    """Имитация локального префиксного кеша: ход 1+ попадает в кеш той же реплики."""

    def test_07_turn_1_is_hit_after_turn_0(self):
        with make_client(ARTIFACTS / "A", warmup=0.2) as client:
            for _ in range(100):
                if client.get("/health").json()["ready"]:
                    break
                import time
                time.sleep(0.1)
            first = client.post("/generate", json={
                "prompt": "p0", "max_new_tokens": 2, "session_id": "k", "turn": 0}).json()
            self.assertEqual(first["cache_result"], "miss")
            second = client.post("/generate", json={
                "prompt": "p1", "max_new_tokens": 2, "session_id": "k", "turn": 1}).json()
            self.assertEqual(second["cache_result"], "hit")
            third = client.post("/generate", json={
                "prompt": "p2", "max_new_tokens": 2, "session_id": "k", "turn": 2}).json()
            self.assertEqual(third["cache_result"], "hit")
            # другой сеанс на той же реплике — miss
            other = client.post("/generate", json={
                "prompt": "p0", "max_new_tokens": 2, "session_id": "k2", "turn": 0}).json()
            self.assertEqual(other["cache_result"], "miss")
            stats = client.get("/debug").json()["cache"]
            self.assertEqual(stats["hits"], 2)
            self.assertEqual(stats["misses"], 2)

    def test_08_cache_capacity_lru(self):
        with make_client(ARTIFACTS / "A", warmup=0.2,
                         backend_kwargs={"cache_capacity": 2}) as client:
            for _ in range(100):
                if client.get("/health").json()["ready"]:
                    break
                import time
                time.sleep(0.1)
            for sid in ("c1", "c2"):
                client.post("/generate", json={"prompt": "p", "max_new_tokens": 1,
                                               "session_id": sid, "turn": 0})
            client.post("/generate", json={"prompt": "p", "max_new_tokens": 1,
                                           "session_id": "c3", "turn": 0})
            # c1 вытеснен (LRU, ёмкость 2) — ход 1 для c1 больше не попадает в кеш
            evicted = client.post("/generate", json={"prompt": "p", "max_new_tokens": 1,
                                                     "session_id": "c1", "turn": 1})
            self.assertEqual(evicted.json()["cache_result"], "miss")


class MetricsEndpoint(unittest.TestCase):
    """Prometheus: histograms с границами, счётчики исходов."""

    def test_09_metrics_histograms_and_counters(self):
        with make_client(ARTIFACTS / "A", warmup=0.2) as client:
            for _ in range(100):
                if client.get("/health").json()["ready"]:
                    break
                import time
                time.sleep(0.1)
            client.post("/generate", json={"prompt": "m", "max_new_tokens": 4,
                                           "session_id": "m1", "turn": 0})
            text = client.get("/metrics").text
            self.assertIn("hw11_ttfc_seconds_bucket", text)
            self.assertIn('le="0.5"', text)
            self.assertIn("hw11_answer_seconds_bucket", text)
            self.assertIn("hw11_queue_seconds_bucket", text)
            self.assertIn('hw11_requests_total{outcome="ok"}', text)
            self.assertIn("hw11_running", text)
            self.assertIn("hw11_waiting", text)
            self.assertIn('hw11_cache_total{result="miss"}', text)

    def test_10_queue_full_rejected_as_503(self):
        """Реальный subprocess: длинный запрос занимает единственный слот,
        второй получает 503 «очередь переполнена»; исход учтён в /metrics."""
        import subprocess
        import tempfile
        import time as _time

        import requests as _rq

        with tempfile.TemporaryDirectory() as tmp:
            tmp_root = Path(tmp)
            import shutil
            shutil.copytree(ARTIFACTS / "A", tmp_root / "A")
            port = 8123
            env = dict(os.environ, WARMUP_SECONDS="0.2", PORT=str(port),
                       QUEUE_TIMEOUT="0.5", MAX_PARALLEL="1",
                       CHUNKS_PER_REQUEST="32",
                       PYTHONIOENCODING="utf-8")
            proc = subprocess.Popen(
                [sys.executable, str(ROOT / "service.py"),
                 "--workspace", str(tmp_root / "A"), "--port", str(port)],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            try:
                base = f"http://127.0.0.1:{port}"
                for _ in range(100):
                    try:
                        if _rq.get(base + "/health", timeout=2).json()["ready"]:
                            break
                    except _rq.ConnectionError:
                        pass
                    _time.sleep(0.1)
                else:
                    self.fail("сервис не стал готов")
                import threading
                result = {}

                def blocker():
                    result["r"] = _rq.post(base + "/generate", timeout=30, json={
                        "prompt": "long", "max_new_tokens": 32})

                t = threading.Thread(target=blocker)
                t.start()
                _time.sleep(0.5)  # blocker держит единственный слот
                r = _rq.post(base + "/generate", timeout=30, json={
                    "prompt": "second", "max_new_tokens": 1})
                t.join(timeout=60)
                self.assertEqual(r.status_code, 503)
                self.assertIn("очередь", r.json()["error"])
                self.assertEqual(result["r"].status_code, 200)
                text = _rq.get(base + "/metrics", timeout=5).text
                self.assertIn('hw11_requests_total{outcome="queue_full"}', text)
            finally:
                proc.kill()
                proc.wait(timeout=10)


if __name__ == "__main__":
    unittest.main(verbosity=2)
