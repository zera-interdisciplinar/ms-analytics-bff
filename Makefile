.DEFAULT_GOAL := help

PYTHON ?= python
PIP ?= pip
PYTEST ?= ./.venv/bin/pytest -q
UVICORN ?= uvicorn
COMPOSE ?= docker compose

APP_MODULE ?= app.api.main:app
CRON_MODULE ?= app.cron.main
HOST ?= 0.0.0.0
PORT ?= 8000
COVERAGE_MIN ?= 80

.PHONY: help install test test-cov run cron dev up up-cron down logs restart clean

help:
	@echo "Available targets:"
	@echo "  install   Install project dependencies"
	@echo "  test      Run the test suite"
	@echo "  test-cov  Run tests with coverage"
	@echo "  run       Start the API server"
	@echo "  cron      Run the cronjob entrypoint once"
	@echo "  dev       Start the API server with reload"
	@echo "  up        Start docker-compose API"
	@echo "  up-cron   Run the cronjob container once"
	@echo "  down      Stop docker-compose services"
	@echo "  logs      Follow docker-compose logs"
	@echo "  restart   Restart docker-compose services"
	@echo "  clean     Remove local caches and coverage artifacts"

install:
	$(PIP) install -r requirements.txt

test:
	$(PYTEST)

test-cov:
	$(PYTEST) --cov=. --cov-report=xml --cov-fail-under=$(COVERAGE_MIN)

run:
	$(UVICORN) $(APP_MODULE) --host $(HOST) --port $(PORT)

cron:
	$(PYTHON) -m $(CRON_MODULE)

dev:
	$(UVICORN) $(APP_MODULE) --host $(HOST) --port $(PORT) --reload

up:
	$(COMPOSE) up -d --build

up-cron:
	$(COMPOSE) --profile cron run --rm cron

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f api

restart:
	$(COMPOSE) down
	$(COMPOSE) up -d --build

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf htmlcov coverage.xml .coverage .coverage.*
