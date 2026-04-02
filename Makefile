.PHONY: install lint format type-check test test-cov run

install:
	uv sync --all-extras

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

type-check:
	uv run mypy src/

test:
	uv run pytest

test-cov:
	uv run pytest --cov=steeleye --cov-report=term-missing

run:
	uv run python -m steeleye