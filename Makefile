.PHONY: test test-fast test-coverage lint typecheck help

# Default target
help:
	@echo "Available targets:"
	@echo "  make test          - Run unit tests with coverage"
	@echo "  make test-fast     - Run unit tests without coverage"
	@echo "  make test-coverage - Run all tests with full coverage report"
	@echo "  make lint          - Run ruff linter"
	@echo "  make typecheck     - Run mypy type checker"

# Unit tests with coverage
test:
	uv run pytest tests/unit/ \
		--cov=src/rabbitkit \
		--cov-report=term-missing

# Fast unit tests without coverage
test-fast:
	uv run pytest tests/unit/ -q --no-cov --tb=short

# Full coverage report
test-coverage:
	uv run pytest tests/unit/ \
		--cov=src/rabbitkit \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		--cov-report=xml:coverage.xml

# Run specific test file
test-file:
	uv run pytest $(FILE) -v

# Linting
lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

# Type checking
typecheck:
	uv run mypy src/rabbitkit/
