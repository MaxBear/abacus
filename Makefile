.DEFAULT_GOAL := help
SHELL := /bin/bash

VENV := .venv
PY   := $(VENV)/bin/python
PIP  := $(VENV)/bin/pip

.PHONY: help venv install lint fmt test up down logs rebuild verify clean

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

venv: ## Create the local 3.12 venv (uv if present, else stdlib venv)
	@if command -v uv >/dev/null 2>&1; then \
		uv venv --python 3.12 $(VENV); \
	else \
		echo "uv not found; falling back to python3 -m venv (needs python3.12 on PATH)"; \
		python3.12 -m venv $(VENV) || python3 -m venv $(VENV); \
	fi

install: venv ## Install dev dependencies
	@if command -v uv >/dev/null 2>&1; then \
		VIRTUAL_ENV=$(VENV) uv pip install -r requirements-dev.txt; \
	else \
		$(PIP) install -r requirements-dev.txt; \
	fi

lint: ## Lint
	$(VENV)/bin/ruff check .

fmt: ## Format
	$(VENV)/bin/ruff format .
	$(VENV)/bin/ruff check --fix .

test: ## Run tests (no containers required)
	$(VENV)/bin/pytest

up: ## Start the stack
	docker compose up -d --build

down: ## Stop the stack and drop volumes
	docker compose down -v

logs: ## Tail api logs
	docker compose logs -f api

rebuild: ## Rebuild the api image from scratch
	docker compose build --no-cache api

verify: ## Phase 0 acceptance checks (see README)
	@./scripts/verify-phase0.sh

clean: ## Remove local caches and venv
	rm -rf $(VENV) .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
