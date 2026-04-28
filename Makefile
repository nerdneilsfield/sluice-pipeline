.DEFAULT_GOAL := help

.PHONY: help install lint format format-check typecheck test check quality

help:
	@printf "Available targets:\n"
	@printf "  install        Install the package in editable mode\n"
	@printf "  lint           Run Ruff lint checks\n"
	@printf "  format         Format the repository with Ruff\n"
	@printf "  format-check   Check formatting without modifying files\n"
	@printf "  typecheck      Run ty type checking\n"
	@printf "  test           Run the full test suite with coverage\n"
	@printf "  check          Run lint, format-check, and typecheck\n"
	@printf "  quality        Run check plus tests\n"

install:
	pip install -e '.[dev]'

lint:
	ruff check . --output-format concise

format:
	ruff format .

format-check:
	ruff format --check .

typecheck:
	ty check

test:
	python -m pytest \
		tests \
		--cov=sluice \
		--cov-report=term-missing \
		-q

check: lint format-check typecheck

quality: check test
