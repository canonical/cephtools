# AGENTS.md

Notes for coding agents working in this repository.

## Environment
- This is a **Python** project using a **src/** layout.
- Minimum Python version: **3.12**.
- The CLI entry point is:
  - `cephtools = "cephtools:main.cli"`
- Dependencies and dev tools are managed with **uv**.
- Dev dependencies include:
  - `pytest`
  - `ruff`
  - `pex`

## Important command conventions
- Prefer **`uv run ...`** for project commands instead of assuming tools are globally on `PATH`.
  - Example: use `uv run pytest`, not bare `pytest`.
  - Example: use `uv run ruff check .`.
- This repo also provides a **justfile**. Prefer `just` recipes when they match the task.

## Common commands
- Sync dev environment:
  - `just sync`
  - equivalent: `UV_CACHE_DIR=.uv-cache uv sync --group dev`
- Run tests:
  - `just unittest`
  - `just unittest -q`
  - `uv run pytest`
- Run lint/format checks:
  - `just lint`
- Build the standalone PEX:
  - `just build-pex`

## Repo-specific tips
- `just` runs bash with `-euo pipefail`.
- The justfile sets `UV_CACHE_DIR=.uv-cache` for uv-based commands.
- If a command like `pytest` appears missing, check whether it should be run via **`uv run`** instead.
- The README may describe workflows, but command behavior should be verified against the current code when they differ.

## Files worth checking early
- `pyproject.toml` — dependencies, Python version, entry point
- `justfile` — preferred dev/test/lint commands
- `README.md` — user-facing workflows and CLI usage
