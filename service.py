"""HTTP-сервис модели ДЗ 11: FastAPI, simulated backend, стриминг, метрики, probes.

Продолжение сервиса ДЗ 8+9+10 (контракты /health /ready /generate сохранены):
- /readyz — readiness для probes: в дефектном режиме READY_EAGER=1 отвечает 200
  сразу после старта HTTP-процесса (исходный дефект опыта A), в исправленном —
  только после загрузки артефакта и завершения прогрева;
- /healthz — liveness: процесс жив;
- /generate принимает session_id/history/turn (диалог без состояния — история
  передаётся в запросе), режим stream=true отдаёт ответ частями (SSE);
- каждый ответ содержит request_id, pod, backend_mode=simulated, модель и
  runtime_sha256 фактически загруженного артефакта;
- /metrics — Prometheus (histograms TTFC/answer/inter-chunk/queue, counters);
- журнал — JSON-строки в stdout с request_id, pod, версией.

CLI (env-переменные по умолчанию — значения из ConfigMap Helm):
  python service.py --workspace /app/artifacts/A --port 8000 --host 0.0.0.0
"""
import argparse
import json
import logging
import os
import socket
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app

import backends

# Зафиксированные лимиты стенда (наследованы из ДЗ 8+9+10)
MAX_NEW_TOKENS_LIMIT = 32
MAX_PROMPT_CHARS = 2048
MAX_BODY_BYTES = 65536
MAX_HISTORY_ITEMS = 20
MAX_SESSION_CHARS = 128

log = logging.getLogger("hw11.service")

# ---------------- Prometheus-метрики (ограниченный набор меток) ----------------

TTFC_BUCKETS = (0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6, 8, 10, 15, 20, 30, 45, 60)
QUEUE_BUCKETS = (0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 15, 20, 30, 45, 60)
ITL_BUCKETS = (0.01, 0.02, 0.03, 0.05, 0.08, 0.1, 0.15, 0.2, 0.3, 0.5, 1, 2, 5)

REQUESTS = Counter("hw11_requests_total", "Запросы по исходам",
                   ["outcome"])  # ok|unwarmed|client_error|not_ready|queue_full|server_error
TTFC_H = Histogram("hw11_ttfc_seconds", "Серверное время до первой части ответа",
                   buckets=TTFC_BUCKETS)
ANSWER_H = Histogram("hw11_answer_seconds", "Длительность полного ответа",
                     buckets=TTFC_BUCKETS)
INTERCHUNK_H = Histogram("hw11_inter_chunk_seconds", "Интервалы между частями",
                         buckets=ITL_BUCKETS)
QUEUE_H = Histogram("hw11_queue_seconds", "Ожидание в очереди перед генерацией",
                    buckets=QUEUE_BUCKETS)
RUNNING_G = Gauge("hw11_running", "Генераций в обработке прямо сейчас")
WAITING_G = Gauge("hw11_waiting", "Запросов, ожидающих семафора")
READY_G = Gauge("hw11_ready", "Readiness по текущей конфигурации")
WARMED_G = Gauge("hw11_warmup_finished", "Прогрев завершён")
CACHE_C = Counter("hw11_cache_total", "Просмотры имитации префиксного кеша", ["result"])
CHUNKS_C = Counter("hw11_chunks_total", "Выданные части ответов")
UNWARMED_C = Counter("hw11_unwarmed_total", "Ответы непрогретого backend")


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


class JsonFormatter(logging.Formatter):
    """JSON-строки в stdout: request_id, pod, версия — в каждом событии."""

    def format(self, record: logging.LogRecord) -> str:
        base = {"utc": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
                "level": record.levelname, "pod": os.getenv("POD_NAME", socket.gethostname())}
        extra = getattr(record, "kv", None)
        if isinstance(extra, dict):
            base.update(extra)
        base["message"] = record.getMessage()
        return json.dumps(base, ensure_ascii=False)


def setup_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.root.handlers = [handler]
    logging.root.setLevel(logging.INFO)


class ServiceState:
    """Состояние: backend, готовность, лимиты, конфигурация опыта."""

    def __init__(self, root: Path, warmup_seconds: float, backend_name: str = "simulated",
                 device: str = "cpu", max_parallel: int | None = None,
                 queue_timeout: float = 30.0, ready_eager: bool = False,
                 warmup_never: bool = False, backend_kwargs: dict | None = None):
        self.root = Path(root)
        self.backend_name = backend_name
        self.device = device
        self.ready = False
        self.warmup_finished = threading.Event()
        self.ready_eager = ready_eager
        self.warmup_never = warmup_never
        self.started_at = time.time()
        self.load_error = None
        self.max_new_tokens_limit = MAX_NEW_TOKENS_LIMIT
        self.queue_timeout = float(queue_timeout)
        self.max_parallel = max_parallel if max_parallel is not None else 2
        self.semaphore = threading.Semaphore(self.max_parallel)
        self.backend_kwargs = backend_kwargs or {}
        self.backend = None
        self.warmup_seconds = warmup_seconds
        self.pod = os.getenv("POD_NAME", socket.gethostname())
        self._load_thread = threading.Thread(target=self._load_and_warm, daemon=True)
        self._load_thread.start()

    # ---------- готовность ----------

    def is_ready(self) -> bool:
        """Дефект: READY_EAGER считает запущенный HTTP-процесс достаточным."""
        if self.ready_eager:
            return True
        return self.ready

    def _load_and_warm(self):
        try:
            manifest = json.loads((self.root / "accepted.json").read_text(encoding="utf-8"))
            self.manifest = manifest
            runtime_limit = manifest.get("runtime", {}).get("generation", {}).get("max_new_tokens")
            if isinstance(runtime_limit, int) and 0 < runtime_limit <= MAX_NEW_TOKENS_LIMIT:
                self.max_new_tokens_limit = runtime_limit
            model_dir = self.root  # артефакт-имитатор лежит рядом с accepted.json
            if self.backend_name == "transformers":
                # Реальные веса: локальный snapshot из манифеста (hostPath PV с
                # кэшем HuggingFace) — доступен всем репликам на ноде.
                local = manifest.get("local_path") or manifest.get("artifact_uri", "")
                candidate = Path(str(local).replace("file://", ""))
                model_dir = candidate if candidate.is_dir() else None
            self.backend = backends.create_backend(
                self.backend_name, manifest, model_dir=model_dir, device=self.device,
                **self.backend_kwargs)
            log.info("load_ok", extra={"kv": {
                "model_id": self.backend.loaded_id,
                "runtime_sha256": self.backend.runtime_sha256,
                "backend": self.backend.kind}})
            # Прогрев: один реальный вызов генерации + имитация длительного прогрева.
            self.backend.generate("warmup", 1, _internal=True)
            time.sleep(self.warmup_seconds)
            if self.warmup_never:
                # Заведомо неготовая версия: прогрев не завершается никогда.
                log.error("warmup_withheld", extra={"kv": {
                    "reason": "WARMUP_NEVER=1 (заведомо неготовая версия)"}})
                threading.Event().wait()
            self.backend.warmed = True
            self.warmup_finished.set()
            self.ready = True
            log.info("warmup_done", extra={"kv": {
                "warmup_seconds": self.warmup_seconds}})
        except BaseException as error:  # загрузка не удалась: сервис жив, но не готов
            self.load_error = f"{type(error).__name__}: {error}"
            self.warmup_finished.set()
            log.error("load_failed", extra={"kv": {"error": self.load_error}})

    # ---------- очередь и генерация ----------

    def acquire_slot(self) -> float:
        """Ожидание семафора (очередь). Возвращает queue_seconds или бросает TimeoutError."""
        WAITING_G.inc()
        queue_started = time.perf_counter()
        try:
            acquired = self.semaphore.acquire(timeout=self.queue_timeout)
        finally:
            WAITING_G.dec()
        queue_seconds = time.perf_counter() - queue_started
        QUEUE_H.observe(queue_seconds)
        if not acquired:
            raise TimeoutError("очередь генерации переполнена")
        return queue_seconds

    def release_slot(self):
        self.semaphore.release()

    def generate_full(self, payload: dict, request_started: float | None = None) -> dict:
        """Блокирующая генерация полного ответа (в threadpool).

        Слот семафора уже захвачен endpoint'ом (до отправки заголовков);
        generate_full освобождает его в finally. request_started — момент
        приёма запроса endpoint'ом: TTFC/answer считаются от него, т.е.
        включают очередь (как их видит клиент)."""
        prompt, limit, session_id, turn = self.validate(payload)
        try:
            RUNNING_G.inc()
            started = request_started if request_started is not None else time.perf_counter()
            backend = self.backend
            answer = ""
            chunks = 0
            cache_result = None
            ttfc_backend = None
            prev_chunk_time = started
            unwarmed = not getattr(backend, "warmed", True)
            for part in backend.generate_stream(prompt, limit, session_id=session_id,
                                                turn=turn):
                now = time.perf_counter()
                if part.get("first"):
                    ttfc_backend = now - started
                    TTFC_H.observe(ttfc_backend)
                    cache_result = part.get("cache_result")
                else:
                    INTERCHUNK_H.observe(now - prev_chunk_time)
                prev_chunk_time = now
                answer += part["delta"]
                chunks += 1
            answer_seconds = time.perf_counter() - started
            ANSWER_H.observe(answer_seconds)
            CHUNKS_C.inc(chunks)
            if cache_result:
                CACHE_C.labels(cache_result).inc()
            if unwarmed:
                UNWARMED_C.inc()
            result = {
                "answer": answer,
                "output_tokens": int(limit),
                "chunks": chunks,
                "model_id": backend.loaded_id,
                "model_version": backend.loaded_id,
                "runtime_sha256": backend.runtime_sha256,
                "backend_mode": backend.kind,
                "pod": self.pod,
                "request_id": payload.get("_request_id") or str(uuid.uuid4()),
                "session_id": session_id,
                "turn": turn,
                "cache_result": cache_result,
                "unwarmed": unwarmed,
                "ready": self.is_ready(),
                "ttfc_seconds": ttfc_backend,
                "generation_seconds": answer_seconds,
            }
            REQUESTS.labels("unwarmed" if unwarmed else "ok").inc()
            return result
        finally:
            RUNNING_G.dec()
            self.release_slot()

    # ---------- валидация (до backend — как в ДЗ 8+9+10) ----------

    def validate(self, payload: dict) -> tuple[str, int, str | None, int]:
        prompt = payload.get("prompt")
        limit = payload.get("max_new_tokens", self.max_new_tokens_limit)
        if not isinstance(prompt, str) or not prompt:
            raise ValueError("prompt: нужна непустая строка")
        if len(prompt) > MAX_PROMPT_CHARS:
            raise ValueError(f"prompt: превышен лимит {MAX_PROMPT_CHARS} символов")
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("max_new_tokens: нужно целое число")
        if not 1 <= limit <= self.max_new_tokens_limit:
            raise ValueError(
                f"max_new_tokens: поддерживается диапазон 1..{self.max_new_tokens_limit}")
        session_id = payload.get("session_id")
        if session_id is not None:
            if not isinstance(session_id, str) or not (1 <= len(session_id) <= MAX_SESSION_CHARS):
                raise ValueError("session_id: непустая строка до 128 символов")
        history = payload.get("history", [])
        if not isinstance(history, list) or len(history) > MAX_HISTORY_ITEMS:
            raise ValueError(f"history: список до {MAX_HISTORY_ITEMS} элементов")
        for item in history:
            if not isinstance(item, dict) or not isinstance(item.get("content"), str):
                raise ValueError("history: элементы с полями role/content")
        turn = payload.get("turn", len(history))
        if isinstance(turn, bool) or not isinstance(turn, int) or turn < 0:
            raise ValueError("turn: неотрицательное целое")
        stream = payload.get("stream", False)
        if not isinstance(stream, bool):
            raise ValueError("stream: нужно булево значение")
        return prompt, limit, session_id, turn


def advertised(state: ServiceState):
    """Модель по принятому указателю (может отличаться от загруженной до перезапуска)."""
    try:
        return json.loads((state.root / "accepted.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def sse_line(obj: dict) -> str:
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"


class AccountingStreamingResponse(StreamingResponse):
    """StreamingResponse с учётом обрыва клиента.

    Starlette не сообщает sync/async-генератору об обрыве соединения:
    при spec_version>=2.4 uvicorn бросает исключение из send(), при более
    старом поведении response завершается через http.disconnect в receive().
    Оба пути перехватываются здесь и вызывают on_disconnect() ровно один раз,
    пока ответ не завершён штатно (учёт исхода interrupted + освобождение
    семафора — иначе слот терялся бы до перезапуска Pod).
    """

    def __init__(self, *args, on_disconnect=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_disconnect = on_disconnect
        self._finished = False
        self._fired = False

    def _fire(self):
        if self._on_disconnect and not self._fired:
            self._fired = True
            try:
                self._on_disconnect()
            except Exception:
                log.exception("on_disconnect handler failed")

    async def stream_response(self, send: "Send") -> None:
        try:
            await super().stream_response(send)
        except Exception:
            if not self._finished:
                self._fire()
            raise
        self._finished = True

    async def listen_for_disconnect(self, receive: "Receive") -> None:
        message = None
        while message is None or message["type"] != "http.disconnect":
            message = await receive()
        if not self._finished:
            self._fire()


def create_app(root: Path, warmup_seconds: float = 20.0, backend_name: str = "simulated",
               device: str = "cpu", max_parallel: int | None = None,
               queue_timeout: float = 30.0, ready_eager: bool = False,
               warmup_never: bool = False, backend_kwargs: dict | None = None) -> FastAPI:
    state = ServiceState(root, warmup_seconds, backend_name, device, max_parallel,
                         queue_timeout, ready_eager, warmup_never, backend_kwargs)

    @asynccontextmanager
    async def lifespan(_app):
        yield

    app = FastAPI(title="mlops26 hw11 model service", lifespan=lifespan)

    @app.get("/health")
    def health():
        return {"alive": True, "backend": getattr(state.backend, "kind", state.backend_name),
                "ready": state.is_ready()}

    @app.get("/healthz")
    def healthz():
        """Liveness: процесс жив. Всегда 200."""
        READY_G.set(1 if state.is_ready() else 0)
        WARMED_G.set(1 if state.warmup_finished.is_set() else 0)
        return {"alive": True}

    @app.get("/readyz")
    def readyz():
        """Readiness для probes. Дефект (READY_EAGER=1): 200 сразу после старта
        HTTP-процесса, до конца прогрева. Исправление: 200 только после
        фактической загрузки артефакта и завершённого прогрева."""
        READY_G.set(1 if state.is_ready() else 0)
        WARMED_G.set(1 if state.warmup_finished.is_set() else 0)
        if state.is_ready():
            return {"ready": True, "pod": state.pod}
        body = {"ready": False, "warmup_finished": state.warmup_finished.is_set(),
                "pod": state.pod}
        if state.load_error:
            body["load_error"] = state.load_error
        return JSONResponse(status_code=503, content=body)

    @app.get("/ready")
    def ready():
        """Детальный readiness (контракт ДЗ 8+9+10)."""
        if state.is_ready():
            return JSONResponse(status_code=200, content={
                "ready": True, "model_id": state.backend.loaded_id,
                "runtime_sha256": state.backend.runtime_sha256,
                "backend_mode": state.backend.kind, "pod": state.pod})
        body = {"ready": False, "warmup_finished": state.warmup_finished.is_set()}
        if state.load_error:
            body["load_error"] = state.load_error
        return JSONResponse(status_code=503, content=body)

    @app.get("/debug")
    def debug():
        """Тестовый маршрут: загрузка, вызовы backend, кеш, счётчики непрогретых ответов."""
        adv = advertised(state)
        return {
            "loaded_id": getattr(state.backend, "loaded_id", None),
            "runtime_sha256": getattr(state.backend, "runtime_sha256", None),
            "advertised_id": adv.get("model_id") if adv else None,
            "pointer_matches_loaded": bool(
                adv and state.backend and adv.get("model_id") == state.backend.loaded_id),
            "calls": getattr(state.backend, "calls", 0),
            "calls_unwarmed": getattr(state.backend, "calls_unwarmed", 0),
            "cache": getattr(state.backend, "cache_stats", lambda: None)(),
            "warmup_finished": state.warmup_finished.is_set(),
            "ready": state.is_ready(),
            "ready_truthful": state.ready,
            "backend": getattr(state.backend, "kind", state.backend_name),
            "backend_mode": state.backend.kind if state.backend else state.backend_name,
            "pod": state.pod,
        }

    @app.post("/generate")
    async def generate(request: Request):
        request_id = str(uuid.uuid4())
        request_started = time.perf_counter()
        if not state.is_ready():
            REQUESTS.labels("not_ready").inc()
            log.info("request_rejected", extra={"kv": {
                "request_id": request_id, "outcome": "not_ready", "status": 503}})
            return JSONResponse(status_code=503, content={
                "error": "сервис еще не готов: загрузка/прогрев не завершены",
                "request_id": request_id, "pod": state.pod})
        size = int(request.headers.get("Content-Length", "0") or 0)
        if size < 0 or size > MAX_BODY_BYTES:
            REQUESTS.labels("client_error").inc()
            return JSONResponse(status_code=413, content={"error": "body too large",
                                                          "request_id": request_id})
        raw = await request.body()
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("тело запроса должно быть JSON-объектом")
            prompt, limit, session_id, turn = state.validate(payload)
        except (ValueError, TypeError, UnicodeDecodeError) as error:
            REQUESTS.labels("client_error").inc()
            log.info("request_rejected", extra={"kv": {
                "request_id": request_id, "outcome": "client_error", "status": 400}})
            return JSONResponse(status_code=400, content={"error": str(error),
                                                          "request_id": request_id})
        payload["_request_id"] = request_id
        stream = payload.get("stream", False)
        # Очередь: блокирующее ожидание семафора в threadpool.
        try:
            queue_seconds = await run_in_threadpool(state.acquire_slot)
        except TimeoutError as error:
            REQUESTS.labels("queue_full").inc()
            log.info("request_rejected", extra={"kv": {
                "request_id": request_id, "outcome": "queue_full", "status": 503,
                "session_id": session_id}})
            return JSONResponse(status_code=503, content={"error": str(error),
                                                          "request_id": request_id,
                                                          "pod": state.pod})
        except BaseException:
            state.release_slot()
            raise

        if not stream:
            try:
                result = await run_in_threadpool(state.generate_full, payload,
                                                 request_started)
                result["queue_seconds"] = queue_seconds
                log.info("request_done", extra={"kv": {
                    "request_id": request_id, "session_id": session_id,
                    "pod": result["pod"], "model_version": result["model_version"],
                    "outcome": "unwarmed" if result["unwarmed"] else "ok",
                    "ttfc_seconds": result["ttfc_seconds"],
                    "answer_seconds": result["generation_seconds"],
                    "queue_seconds": queue_seconds, "cache": result["cache_result"]}})
                return JSONResponse(status_code=200, content=result)
            except BaseException:
                # release_slot/RUNNING_G.dec() уже выполнены в finally generate_full
                REQUESTS.labels("server_error").inc()
                log.exception("request_failed", extra={"kv": {"request_id": request_id}})
                return JSONResponse(status_code=500, content={"error": "internal error",
                                                              "request_id": request_id})

        # Потоковый режим: semaphore уже захвачен. Генератор синхронный — его
        # итерирует starlette в threadpool. Starlette НЕ закрывает sync-
        # генератор при обрыве клиента, поэтому обрыв отслеживается явно:
        # watcher читает receive(), на http.disconnect выставляет stop_flag,
        # потребитель прерывает итерацию, закрывает генератор и учитывает
        # исход interrupted (иначе семафор утёк бы до конца стрима).
        backend = state.backend
        started = request_started  # TTFC/answer от приёма запроса (включая очередь)
        unwarmed = not getattr(backend, "warmed", True)
        stop_flag = threading.Event()
        sync_gen = backend.generate_stream(prompt, limit, session_id=session_id,
                                           turn=turn)
        released = threading.Event()
        stream_chunks = 0

        def finish_release():
            # Семафор освобождается ровно один раз: штатный конец, обрыв
            # (on_disconnect) или GC-закрытие заброшенного генератора.
            if not released.is_set():
                released.set()
                state.release_slot()

        def note_disconnect():
            # Вызывается AccountingStreamingResponse при обрыве клиента.
            if not stop_flag.is_set():
                stop_flag.set()
            try:
                sync_gen.close()
            except Exception:
                pass
            REQUESTS.labels("interrupted").inc()
            log.info("request_interrupted", extra={"kv": {
                "request_id": request_id, "session_id": session_id,
                "pod": state.pod, "chunks": stream_chunks}})
            finish_release()

        async def disconnect_watcher():
            try:
                while True:
                    message = await request.receive()
                    if message["type"] == "http.disconnect":
                        stop_flag.set()
                        return
            except asyncio.CancelledError:
                raise

        async def sse_stream():
            nonlocal stream_chunks
            watcher = asyncio.create_task(disconnect_watcher())
            interrupted = False
            answer = ""
            chunks = 0
            ttfc_backend = None
            cache_result = None
            prev_chunk_time = started
            try:
                async for part in iterate_in_threadpool(sync_gen):
                    if stop_flag.is_set():
                        interrupted = True
                        break
                    now = time.perf_counter()
                    if part.get("first"):
                        ttfc_backend = now - started
                        TTFC_H.observe(ttfc_backend)
                        cache_result = part.get("cache_result")
                    else:
                        INTERCHUNK_H.observe(now - prev_chunk_time)
                    prev_chunk_time = now
                    answer += part["delta"]
                    chunks += 1
                    stream_chunks = chunks
                    CHUNKS_C.inc()
                    yield sse_line({"delta": part["delta"],
                                    "chunk_index": part["chunk_index"]})
                if not interrupted:
                    answer_seconds = time.perf_counter() - started
                    ANSWER_H.observe(answer_seconds)
                    if cache_result:
                        CACHE_C.labels(cache_result).inc()
                    if unwarmed:
                        UNWARMED_C.inc()
                    REQUESTS.labels("unwarmed" if unwarmed else "ok").inc()
                    log.info("request_done", extra={"kv": {
                        "request_id": request_id, "session_id": session_id,
                        "pod": state.pod, "model_version": backend.loaded_id,
                        "outcome": "unwarmed" if unwarmed else "ok", "stream": True,
                        "ttfc_seconds": ttfc_backend,
                        "answer_seconds": answer_seconds,
                        "queue_seconds": queue_seconds, "cache": cache_result}})
                    yield sse_line({"done": True, "answer": answer,
                                    "output_tokens": int(limit), "chunks": chunks,
                                    "model_id": backend.loaded_id,
                                    "model_version": backend.loaded_id,
                                    "runtime_sha256": backend.runtime_sha256,
                                    "backend_mode": backend.kind, "pod": state.pod,
                                    "request_id": request_id,
                                    "session_id": session_id, "turn": turn,
                                    "cache_result": cache_result,
                                    "unwarmed": unwarmed,
                                    "queue_seconds": queue_seconds,
                                    "ttfc_seconds": ttfc_backend,
                                    "generation_seconds": answer_seconds})
            finally:
                watcher.cancel()
                finish_release()

        return AccountingStreamingResponse(sse_stream(), on_disconnect=note_disconnect,
                                           media_type="text/event-stream",
                                           headers={"X-Request-Id": request_id,
                                                    "X-Pod": state.pod})

    app.mount("/metrics", make_asgi_app())
    app.state.service_state = state
    return app


import asyncio  # noqa: E402

from starlette.concurrency import iterate_in_threadpool, run_in_threadpool  # noqa: E402


def main():
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path,
                        default=Path(_env("WORKSPACE", ".lab")))
    parser.add_argument("--port", type=int, default=int(_env("PORT", "8000")))
    parser.add_argument("--host", default=_env("HOST", "0.0.0.0"))
    parser.add_argument("--warmup-seconds", type=float,
                        default=_env_float("WARMUP_SECONDS", 20.0))
    parser.add_argument("--backend", choices=["simulated", "stub", "transformers"],
                        default=_env("BACKEND", "simulated"))
    parser.add_argument("--device", default=_env("DEVICE", "cpu"))
    parser.add_argument("--max-parallel", type=int, default=int(_env("MAX_PARALLEL", "2")))
    parser.add_argument("--queue-timeout", type=float,
                        default=_env_float("QUEUE_TIMEOUT", 30.0))
    parser.add_argument("--ready-eager", action="store_true",
                        default=_env("READY_EAGER", "0") == "1")
    parser.add_argument("--warmup-never", action="store_true",
                        default=_env("WARMUP_NEVER", "0") == "1")
    parser.add_argument("--prefill-hit", type=float,
                        default=_env_float("PREFILL_HIT_SECONDS", 0.05))
    parser.add_argument("--prefill-miss", type=float,
                        default=_env_float("PREFILL_MISS_SECONDS", 0.4))
    parser.add_argument("--cache-capacity", type=int,
                        default=int(_env("CACHE_CAPACITY", "64")))
    parser.add_argument("--chunk-interval", type=float,
                        default=_env_float("CHUNK_INTERVAL_SECONDS", 0.05))
    parser.add_argument("--chunks-per-request", type=int,
                        default=int(_env("CHUNKS_PER_REQUEST", "8")))
    parser.add_argument("--cpu-burn-ms-per-token", type=float,
                        default=_env_float("CPU_BURN_MS_PER_TOKEN", 0.0))
    args = parser.parse_args()

    import uvicorn

    backend_kwargs = {
        "prefill_hit_seconds": args.prefill_hit,
        "prefill_miss_seconds": args.prefill_miss,
        "chunk_interval_seconds": args.chunk_interval,
        "chunks_per_request": args.chunks_per_request,
        "cache_capacity": args.cache_capacity,
        "cpu_burn_ms_per_token": args.cpu_burn_ms_per_token,
    }
    app = create_app(args.workspace, args.warmup_seconds, args.backend, args.device,
                     args.max_parallel, args.queue_timeout, args.ready_eager,
                     args.warmup_never, backend_kwargs)
    log.info("serve", extra={"kv": {"host": args.host, "port": args.port,
                                    "workspace": str(args.workspace),
                                    "backend": args.backend,
                                    "warmup_seconds": args.warmup_seconds,
                                    "ready_eager": args.ready_eager,
                                    "max_parallel": args.max_parallel}})
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning",
                timeout_graceful_shutdown=None)


if __name__ == "__main__":
    main()
