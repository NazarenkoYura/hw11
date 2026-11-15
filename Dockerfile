# HW11: образ сервиса и генератора нагрузки (python:3.12-slim, без весов).
# Артефакты модели-имитатора A и B запечены; версия выбирается env WORKSPACE
# (задаётся Helm values), дефектные режимы — READY_EAGER / WARMUP_NEVER.
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY service.py backends.py ./
COPY artifacts/sim ./artifacts/sim
COPY scripts/loadgen.py ./scripts/loadgen.py
COPY checks ./checks
# Непривилегированный пользователь
RUN useradd --system --uid 10001 hw11 && chown -R hw11:hw11 /app
USER 10001:10001
ENV WORKSPACE=/app/artifacts/sim/A PORT=8000
EXPOSE 8000
CMD ["python", "service.py"]

# Цель real: + CPU-инференс Qwen3-0.6B (кэш HuggingFace монтируется с хоста
# через hostPath PV /mnt/hf — см. kind-config.yaml extraMounts).
FROM base AS real
USER root
RUN pip install --no-cache-dir torch==2.15.0 --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir transformers==5.22.1 peft==0.21.2
USER 10001:10001
