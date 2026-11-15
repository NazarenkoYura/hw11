# HW11 — основные команды стенда.
# Кластер kind, чарт hw11, мониторинг, проверки и удаление — всё воспроизводимо.
OS_WINDOWS := 1
PYTHON := .venv/Scripts/python.exe
KIND := C:/Users/user/bin/kind.exe
HELM := C:/Users/user/bin/helm.exe
KUBECTL := kubectl

.PHONY: help artifacts build load deploy fix defect check checks clean-up clean size

help:
	@echo "make artifacts  — пересобрать артефакты модели-имитатора A/B"
	@echo "make build      — собрать образ hw11-service:1.0.0 (и real при нужно)"
	@echo "make load       — загрузить образ в kind-кластер"
	@echo "make cluster    — создать kind-кластер (kind-config.yaml)"
	@echo "make defect     — деплой с ИСХОДНЫМ дефектом (READY_EAGER=1)"
	@echo "make fix        — деплой с ИСПРАВЛЕННОЙ готовностью (READY_EAGER=0)"
	@echo "make deploy     — то же, что fix (рабочая конфигурация)"
	@echo "make check      — юнит-тесты сервиса (без кластера)"
	@echo "make checks     — 5 собственных проверок Job'ом в кластере"
	@echo "make clean      — удалить стенд: helm uninstall + kind delete"
	@echo "make size       — фактический размер стенда"

artifacts:
	$(PYTHON) scripts/make_artifacts.py

build:
	docker build -t hw11-service:1.0.0 --target base .

build-real:
	docker build -t hw11-service:real-1.0.0 --target real .

load:
	$(KIND) load docker-image hw11-service:1.0.0 --name hw11
	$(KIND) load docker-image hw11-service:real-1.0.0 --name hw11 || true

cluster:
	$(KIND) create cluster --config kind-config.yaml

defect:
	$(HELM) upgrade --install hw11 chart --set readyEager=true

fix deploy:
	$(HELM) upgrade --install hw11 chart --set readyEager=false

check:
	$(PYTHON) -m compileall -q service.py backends.py scripts checks tests
	$(PYTHON) -m unittest discover -s tests -v

checks:
	$(HELM) upgrade hw11 chart --reuse-values --set checks.enabled=true
	$(KUBECTL) delete job hw11-checks --ignore-not-found
	$(HELM) upgrade hw11 chart --reuse-values --set checks.enabled=true
	$(KUBECTL) wait --for=condition=complete job/hw11-checks --timeout=300s
	$(KUBECTL) logs job/hw11-checks
	$(HELM) upgrade hw11 chart --reuse-values --set checks.enabled=false

clean-up: clean
clean:
	-$(HELM) uninstall hw11
	$(KIND) delete cluster --name hw11

size:
	@docker system df | head -8
	@echo "--- images hw11:"
	@docker images "hw11-service*" --format "{{.Repository}}:{{.Tag}} {{.Size}}"
