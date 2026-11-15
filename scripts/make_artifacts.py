"""Генератор артефактов модели-имитатора версий A и B.

Источник и проверка версии артефакта (п.1 задания): небольшой файл модели с
хешем. Структура каталога версии (монтируется как workspace сервиса):
  accepted.json   — принятый указатель (полный манифест, как в ДЗ 8+9+10);
  manifest-A.json — тот же манифест отдельным файлом (для аудита);
  model.json      — «веса» имитатора (детерминированный JSON).

runtime_sha256 = SHA-256 канонического JSON блока runtime (как release.canonical
из ДЗ 8+9+10). file_hashes закрепляет SHA-256 model.json — backend пересчитывает
хеш файла до «загрузки» и отказывается работать при расхождении.
"""
import hashlib
import json
import sys
from pathlib import Path

ARTIFACTS = Path(__file__).resolve().parent.parent / "artifacts" / "sim"


def canonical(data) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_version(version: str, words: int) -> None:
    root = ARTIFACTS / version
    root.mkdir(parents=True, exist_ok=True)
    # Детерминированные «веса»: стабильная псевдослучайная последовательность.
    seed = hashlib.sha256(f"hw11-sim-{version}".encode()).digest()
    weights = []
    buf = seed
    for _ in range(words):
        buf = hashlib.sha256(buf).digest()
        weights.append(round(int.from_bytes(buf[:4], "big") / 2**32 - 0.5, 6))
    model = {"schema_version": 1, "backend_mode": "simulated", "version": version,
             "weights": weights}
    model_bytes = (json.dumps(model, ensure_ascii=False) + "\n").encode("utf-8")
    (root / "model.json").write_bytes(model_bytes)
    model_hash = sha256_bytes(model_bytes)

    runtime = {
        "dtype": "sim",
        "backend_mode": "simulated",
        "generation": {"max_new_tokens": 32, "do_sample": False},
    }
    manifest = {
        "schema_version": 1,
        "model_id": f"sim-{version}",
        "artifact_uri": f"file:///app/artifacts/{version}/model.json",
        "file_hashes": {"model.json": model_hash},
        "runtime": runtime,
        "runtime_sha256": sha256_bytes(canonical(runtime).encode("utf-8")),
    }
    manifest_text = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    (root / "model.json").write_bytes(model_bytes)
    (root / f"manifest-{version}.json").write_text(manifest_text, encoding="utf-8")
    (root / "accepted.json").write_text(manifest_text, encoding="utf-8")
    print(f"{version}: model.json sha256={model_hash[:16]}... "
          f"runtime_sha256={manifest['runtime_sha256'][:16]}...")


if __name__ == "__main__":
    words = int(sys.argv[1]) if len(sys.argv) > 1 else 4096
    make_version("A", words)
    make_version("B", words)
