.DEFAULT_GOAL := help
SHELL := /bin/bash

VENV := .venv

.PHONY: help install lock lint fmt test migrate migration up down logs rebuild verify chat clean

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Create .venv and install exactly what uv.lock pins
	@command -v uv >/dev/null 2>&1 || { \
		echo "uv not found. Install it: brew install uv  (or: curl -LsSf https://astral.sh/uv/install.sh | sh)"; \
		exit 1; \
	}
	uv sync --locked

lock: ## Re-resolve uv.lock after editing pyproject.toml
	uv lock

lint: ## Lint
	uv run ruff check .

fmt: ## Format
	uv run ruff format .
	uv run ruff check --fix .

test: ## Run tests (no containers required)
	uv run pytest

# Migrations are a deliberate step, never the api container's entrypoint: with
# more than one replica every pod would race to migrate the same database on
# every rollout. Run this once, then roll the pods.
#
# Reads DATABASE_URL like everything else. From the host that means .env — the
# built-in default points at `postgres:5432`, which only resolves inside the
# compose network. `cp .env.example .env` is the fix.
migrate: ## Apply migrations up to head
	uv run alembic upgrade head

migration: ## Autogenerate a revision: make migration m="add chat tables"
	@test -n "$(m)" || { echo 'usage: make migration m="what changed"'; exit 1; }
	uv run alembic revision --autogenerate -m "$(m)"

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

chat: ## Serve dev/chat.html — a browser client for the chat socket
	@echo "http://localhost:8080/chat.html  (needs the api running: make up)"
	@uv run python -m http.server 8080 --directory dev

clean: ## Remove local caches and venv
	rm -rf $(VENV) .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
