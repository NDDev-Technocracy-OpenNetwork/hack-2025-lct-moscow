# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **dlt (Data Load Tool)** repository - an open-source Python ELT/ETL library for data pipelines that handles schema management, normalization, and incremental loading automatically. The project supports 30+ SQL databases, REST APIs, filesystem sources, and streaming data from Kafka.

## Development Commands

### Setup and Environment
- `make install-uv` - Install the UV package manager (required for development)
- `make dev` - Setup development environment with all dependencies
- `uv sync --all-extras --group docs --group dev --group providers --group pipeline --group sources --group sentry-sdk --group ibis --group adbc --group dashboard-tests` - Full sync of all development dependencies

### Code Quality and Testing
- `make lint` - Run comprehensive linting including mypy, ruff, flake8, black, and security checks
- `make format` - Format code using black
- `make test` - Run all tests (requires destination credentials)
- `make test-common` - Run tests for common components (no external dependencies)
- `make test-load-local` - Test using only local destinations (duckdb, filesystem)
- `make test-load-local-postgres` - Test with local PostgreSQL

### Documentation and Examples
- `make lint-and-test-snippets` - Test and lint documentation code snippets
- `make lint-and-test-examples` - Test and lint example code
- `make preprocess-docs` - Run documentation preprocessing

### Build and Release
- `make build-library` - Build the dlt package for distribution
- `make publish-library` - Build and publish to PyPI (maintainers only)

## Architecture Overview

### Core Components
- **`dlt/`** - Main library code
  - **`pipeline/`** - Core pipeline orchestration and management
  - **`extract/`** - Data extraction decorators and source definitions
  - **`normalize/`** - Data normalization and schema management
  - **`load/`** - Loading data to destinations
  - **`destinations/`** - Destination-specific implementations (PostgreSQL, ClickHouse, DuckDB, etc.)
  - **`sources/`** - Pre-built sources (REST API, SQL databases, filesystem, Kafka)
  - **`common/`** - Shared utilities and configuration
  - **`cli/`** - Command-line interface

### Key Patterns
- **Sources** are Python generators decorated with `@dlt.resource` or `@dlt.source`
- **Pipelines** orchestrate extract-normalize-load workflows with automatic schema evolution
- **Destinations** handle data loading with format-specific optimizations
- **Configuration** uses TOML files in `.dlt/` directory for secrets and settings

### Testing Structure
- **`tests/common/`** - Tests for core functionality (no external dependencies)
- **`tests/load/`** - Destination-specific loading tests
- **`tests/sources/`** - Source implementation tests
- **`tests/pipeline/`** - Pipeline orchestration tests
- **`tests/e2e/`** - End-to-end integration tests

## Development Workflow

### Branch Strategy
- **devel** - Default branch for feature development and regular contributions
- **master** - Production releases and hotfixes only
- Branch naming: `{category}/{ticket-id}-description` (e.g., `feat/4922-add-avro-support`)
- Categories: `feat`, `fix`, `exp`, `test`, `docs`, `keep`

### Code Standards
- Python 3.9+ compatibility required
- Type hints mandatory (mypy enforced)
- Black code formatting with 200 character line limit
- Comprehensive docstrings for public APIs
- Security scanning with bandit

### Testing Requirements
- All new features require tests
- Use `pytest` for test execution
- Mock external dependencies in unit tests
- Integration tests require appropriate destination credentials

## Configuration

### Development Dependencies
The project uses `uv` for dependency management with multiple optional extras:
- Core: `dlt` package with basic functionality
- Destinations: `[postgres]`, `[clickhouse]`, `[filesystem]`, etc.
- Sources: Various source-specific dependencies
- Development: Testing, linting, documentation tools

### Environment Setup
- UV-based dependency management (pyproject.toml + uv.lock)
- Multiple test environments for different destination combinations
- Docker compositions for integration testing (`tests/load/*/docker-compose.yml`)

## Important Notes

- **New destinations are unlikely to be merged** due to maintenance overhead
- Significant changes require GitHub issues for discussion before implementation
- All contributions must target the **devel** branch unless it's a hotfix
- CI credentials available via labels for fork contributors when needed
- The project maintains backward compatibility with semantic versioning