"""Бэкенды сервиса hw11 через общий интерфейс (продолжение ДЗ 8+9+10).

Общий интерфейс (инвариант сохранён из hw8910):
  backend.loaded_id        — фактически загруженная версия (model_id);
  backend.runtime_sha256   — runtime загруженного артефакта;
  backend.calls            — число фактических вызовов генерации;
  backend.generate(prompt, limit) -> {'answer': str, 'output_tokens': int, ...}

Новое в hw11 (backend_mode=simulated):
  - настраиваемые прогрев/очередь/лимит параллельной обработки — на уровне
    service.ServiceState (семафор + queue timeout), как и раньше;
  - выдача ответа частями: generate_stream() возвращает генератор частей
    (chunk) с управляемым интервалом;
  - имитация ЛОКАЛЬНОГО префиксного кеша с ограниченной ёмкостью (LRU по
    session_id): hit/miss имеют разную стоимость prefill;
  - вычислительная нагрузка (busy-hashing) для наблюдаемого CPU в опыте C/HPA;
  - признак прогрева: до backend.warmed=True ответы помечены UNWARMED и
    считаются отдельно (исходный дефект опыта A).

TransformersBackend сохранён из ДЗ 8+9+10 без изменений логики: та же проверка
SHA-256 файлов артефакта до загрузки, тот же dtype/генерационные параметры из
манифеста. torch импортируется лениво — в sim-образе его нет.
"""
import hashlib
import json
import math
import time
from collections import OrderedDict
from pathlib import Path


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_artifact_files(manifest: dict, model_dir: Path) -> None:
    """Пересчёт хешей файлов артефакта до загрузки (как в ДЗ 8+9+10)."""
    file_hashes = manifest.get("file_hashes")
    if not isinstance(file_hashes, dict) or not file_hashes:
        raise ValueError("В манифесте нет закрепленного списка file_hashes")
    for name, expected in file_hashes.items():
        path = model_dir / name
        if not path.is_file():
            raise ValueError(f"Файл артефакта отсутствует: {name}")
        actual = _sha256_file(path)
        if actual != expected:
            raise ValueError(f"Хеш файла {name}: {actual} != {expected}")


def _burn_cpu(millis: float) -> None:
    """Заметная для cAdvisor вычислительная нагрузка (sha256 busy-loop)."""
    if millis <= 0:
        return
    deadline = time.perf_counter() + millis / 1000.0
    buf = b"hw11-cpu-burn" * 512
    while time.perf_counter() < deadline:
        hashlib.sha256(buf).hexdigest()


_FILLER = ("рокировочный ход ладьёй защищает", "пешечная структура требует",
           "слон возвращает давление по диагонали", "центр раскрывается после",
           "переход в эндшпиль выгоден", "атака по открытой линии целесообразна",
           "ферзь меняет позицию на", "контроль поля ослаблен у")


class SimulatedBackend:
    """Лёгкий управляемый CPU-имитатор: backend_mode=simulated.

    Параметры (env/CLI, фиксируются в отчёте):
      prefill_hit_seconds / prefill_miss_seconds — стоимость cache hit/miss;
      cache_capacity — ограничение локального префиксного кеша (LRU, ключ
        session_id, значение — старший обслуженный номер хода);
      chunk_interval_seconds — интервал между частями ответа;
      chunks_per_request — на сколько частей делится ответ;
      cpu_burn_ms_per_token — вычислительная нагрузка на токен (для HPA).
    """

    kind = "simulated"

    def __init__(self, manifest: dict, model_dir: Path | None = None, *,
                 prefill_hit_seconds: float = 0.05,
                 prefill_miss_seconds: float = 0.4,
                 chunk_interval_seconds: float = 0.05,
                 chunks_per_request: int = 8,
                 cache_capacity: int = 64,
                 cpu_burn_ms_per_token: float = 0.0, **_):
        if model_dir is not None:
            verify_artifact_files(manifest, Path(model_dir))
            # "Загрузка весов" имитатора: чтение файла артефакта после сверки хеша.
            (Path(model_dir) / "model.json").read_text(encoding="utf-8")
        self.loaded_id = manifest["model_id"]
        self.runtime_sha256 = manifest["runtime_sha256"]
        self.calls = 0
        self.calls_unwarmed = 0
        self.warmed = False  # переводится service после прогрева
        self.cache_hits = 0
        self.cache_misses = 0
        self.prefill_hit_seconds = float(prefill_hit_seconds)
        self.prefill_miss_seconds = float(prefill_miss_seconds)
        self.chunk_interval_seconds = float(chunk_interval_seconds)
        self.chunks_per_request = max(1, int(chunks_per_request))
        self.cache_capacity = max(1, int(cache_capacity))
        self.cpu_burn_ms_per_token = float(cpu_burn_ms_per_token)
        self._cache: OrderedDict[str, int] = OrderedDict()

    # ---------- cache ----------

    def _cache_lookup(self, session_id: str | None, turn: int,
                      count: bool = True) -> tuple[float, str]:
        """Стоимость prefill по имитации локального префиксного кеша.

        Первый ход сессии всегда miss (префикса нет); повторный ход на ТОЙ ЖЕ
        реплике — hit. После обработки хода T реплика хранит префикс 0..T.
        count=False — внутренние вызовы (прогрев) не учитываются в статистике.
        """
        hit = False
        if session_id and turn > 0:
            cached_depth = self._cache.get(session_id)
            hit = cached_depth is not None and turn <= cached_depth
            if hit:
                self._cache.move_to_end(session_id)
        if count:
            if hit:
                self.cache_hits += 1
            else:
                self.cache_misses += 1
        return (self.prefill_hit_seconds if hit else self.prefill_miss_seconds,
                "hit" if hit else "miss")

    def _cache_commit(self, session_id: str | None, turn: int) -> None:
        """После хода T реплика хранит префикс глубиной T+1 (ходы 0..T)."""
        if not session_id:
            return
        depth = max(turn + 1, self._cache.get(session_id, 0))
        self._cache[session_id] = depth
        self._cache.move_to_end(session_id)
        while len(self._cache) > self.cache_capacity:
            self._cache.popitem(last=False)

    def cache_stats(self) -> dict:
        return {"hits": self.cache_hits, "misses": self.cache_misses,
                "entries": len(self._cache), "capacity": self.cache_capacity}

    # ---------- generation ----------

    def _echo_answer(self, prompt: str, turn: int) -> str:
        """Детерминированный ответ: эхо контрольной строки + стабильный filler."""
        user_part = prompt.strip()
        if turn:
            user_part = user_part.splitlines()[-1] if prompt else user_part
        seed = hashlib.sha256(f"{self.loaded_id}|{turn}|{user_part}".encode()).hexdigest()
        filler = _FILLER[int(seed[:2], 16) % len(_FILLER)]
        marker = f"UNWARMED {self.loaded_id}: " if not self.warmed else f"[{self.loaded_id}] "
        return f"{marker}{user_part[:120]} — {filler} ({seed[:8]})"

    def _plan(self, limit: int) -> int:
        return max(1, min(self.chunks_per_request, limit))

    def generate_stream(self, prompt: str, limit: int, session_id: str | None = None,
                        turn: int = 0, _internal: bool = False):
        """Генератор частей. Первая часть выходит после prefill+интервала,
        далее по одной каждые chunk_interval_seconds."""
        self.calls += 1
        if not self.warmed and not _internal:
            self.calls_unwarmed += 1
        prefill, cache_result = self._cache_lookup(session_id, turn,
                                                   count=not _internal)
        burn_ms = self.cpu_burn_ms_per_token * limit
        _burn_cpu(burn_ms * 0.2)  # префиксная часть вычислений
        time.sleep(prefill)
        chunks = self._plan(limit)
        answer = self._echo_answer(prompt, turn)
        step = max(1, math.ceil(len(answer) / chunks))
        ttfc_backend = None
        for i in range(chunks):
            _burn_cpu(burn_ms * 0.8 / chunks)
            time.sleep(self.chunk_interval_seconds)
            delta = answer[i * step:(i + 1) * step]
            if delta:
                yield {"delta": delta, "chunk_index": i, "first": i == 0,
                       "cache_result": cache_result}
        if session_id:
            self._cache_commit(session_id, turn)

    def generate(self, prompt: str, limit: int, session_id: str | None = None,
                 turn: int = 0, _internal: bool = False) -> dict:
        started = time.perf_counter()
        answer = ""
        chunks = 0
        for part in self.generate_stream(prompt, limit, session_id, turn,
                                         _internal=_internal):
            answer += part["delta"]
            chunks += 1
        return {"answer": answer, "output_tokens": int(limit),
                "chunks": chunks,
                "cache_result": None,  # точное значение известно в stream-цикле
                "generation_seconds_backend": time.perf_counter() - started}


def parse_artifact_uri(uri: str):
    """'huggingface:repo@revision' -> (repo, revision). Иначе (uri, None)."""
    if isinstance(uri, str) and uri.startswith("huggingface:"):
        rest = uri[len("huggingface:"):]
        repo, _, revision = rest.partition("@")
        return repo, revision or None
    return uri, None


class TransformersBackend:
    """Реальная модель ДЗ 8+9+10 (Transformers): та же проверка хешей и dtype
    из манифеста. Дополнительные поля — TTFT/TPOT считаются на клиенте по
    фактическим токенам; здесь стриминг по токенам через TextIterator."""

    def __init__(self, manifest: dict, model_dir=None, device: str = "cpu", **_):
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self.kind = "transformers"
        self.calls = 0
        self.device = device
        dir_path = Path(model_dir) if model_dir else None
        if dir_path is not None:
            verify_artifact_files(manifest, dir_path)
            source, revision = str(dir_path), None
        else:
            source, revision = parse_artifact_uri(manifest["artifact_uri"])
        runtime = manifest.get("runtime", {})
        generation = runtime.get("generation", {})
        dtype_name = runtime.get("dtype", "float32")
        dtype = getattr(torch, dtype_name, None) if isinstance(dtype_name, str) else None
        if dtype is None:
            raise ValueError(f"runtime.dtype {dtype_name!r} не распознан torch")
        self.tokenizer = AutoTokenizer.from_pretrained(source, revision=revision)
        self.model = AutoModelForCausalLM.from_pretrained(source, revision=revision,
                                                          torch_dtype=dtype)
        self.model.to(device)
        self.model.eval()
        self.loaded_id = manifest["model_id"]
        self.runtime_sha256 = manifest["runtime_sha256"]
        self.generation_defaults = {
            "do_sample": bool(generation.get("do_sample", False)),
            "max_new_tokens": int(generation.get("max_new_tokens", 32)),
            "enable_thinking": bool(generation.get("enable_thinking", False)),
        }
        self.cache_hits = 0
        self.cache_misses = 0

    def generate_stream(self, prompt: str, limit: int, session_id=None, turn=0,
                        _internal: bool = False):
        """Потоковая отдача по фактическим токенам (TextIterator)."""
        import torch

        self.calls += 1
        started = time.perf_counter()
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
        first_done = False
        with torch.no_grad():
            streamer_out = []
            from transformers import TextIteratorStreamer
            from threading import Thread
            streamer = TextIteratorStreamer(self.tokenizer, skip_prompt=True,
                                            skip_special_tokens=True)
            kwargs = dict(**inputs, max_new_tokens=int(limit),
                          do_sample=self.generation_defaults["do_sample"],
                          pad_token_id=self.tokenizer.eos_token_id, streamer=streamer)
            worker = Thread(target=self.model.generate, kwargs=kwargs, daemon=True)
            worker.start()
            n = 0
            for text_piece in streamer:
                if text_piece:
                    yield {"delta": text_piece, "chunk_index": n, "first": not first_done,
                           "cache_result": "n/a"}
                    first_done = True
                    n += 1
            worker.join()
        yield {"done": True, "ttfc_backend_seconds": None}

    def generate(self, prompt: str, limit: int, session_id=None, turn=0,
                 _internal: bool = False) -> dict:
        import torch

        self.calls += 1
        started = time.perf_counter()
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
        with torch.no_grad():
            output = self.model.generate(
                **inputs,
                max_new_tokens=int(limit),
                do_sample=self.generation_defaults["do_sample"],
                pad_token_id=self.tokenizer.eos_token_id,
            )
        new_tokens = output[0][inputs["input_ids"].shape[1]:]
        answer = self.tokenizer.decode(new_tokens, skip_special_tokens=True)
        return {
            "answer": answer,
            "output_tokens": int(new_tokens.shape[0]),
            "generation_seconds_backend": time.perf_counter() - started,
        }


def create_backend(name: str, manifest: dict, **kwargs):
    if name in ("simulated", "stub"):  # 'stub' — совместимость с CLI ДЗ 8+9+10
        return SimulatedBackend(manifest, **kwargs)
    if name == "transformers":
        return TransformersBackend(manifest, **kwargs)
    raise ValueError(f"Неизвестный backend: {name}")
