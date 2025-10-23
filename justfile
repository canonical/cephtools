# Safer shell
set shell := ["bash", "-euo", "pipefail", "-c"]

# Binaries (override if needed): UV=uv just unittest
UV := env_var_or_default('UV', 'uv')

default: help

help:
	@just --list

# Install base + dev deps into the project env
sync:
	UV_CACHE_DIR=.uv-cache {{UV}} sync --group dev

# Lint with ruff from the project env
lint:
	UV_CACHE_DIR=.uv-cache {{UV}} run ruff check .
	UV_CACHE_DIR=.uv-cache {{UV}} run ruff format --check .

# Extra args forwarded, e.g.: just unittest -k foo -q
unittest *ARGS:
	UV_CACHE_DIR=.uv-cache {{UV}} run pytest {{ARGS}}

# Build a standalone PEX installer
build-pex OUTPUT="dist/cephtools":
	scripts/build_pex.sh {{OUTPUT}}
