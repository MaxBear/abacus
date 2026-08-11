.DEFAULT_GOAL := help
SHELL := /bin/bash

VENV := .venv

.PHONY: help install lock lint fmt test up down logs rebuild verify clean

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Sync .venv to uv.lock exactly (runtime + dev)
	@command -v uv >/dev/null 2>&1 || { echo "uv required: https://docs.astral.sh/uv/getting-started/installation/"; exit 1; }
	uv sync

lock: ## Re-resolve uv.lock after editing pyproject.toml dependencies
	uv lock

lint: ## Lint
	uv run ruff check .

fmt: ## Format
	uv run ruff format .
	uv run ruff check --fix .

test: ## Run tests (no containers required)
	uv run pytest

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
