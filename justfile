# Safer shell
set shell := ["bash", "-euo", "pipefail", "-c"]

# Binaries (override if needed): UV=uv just unittest
UV := env_var_or_default('UV', 'uv')

default: help

help:
	@just --list

# Install base + dev deps into the project env
sync:
	{{UV}} sync --group dev

# Lint with ruff from the project env
lint:
	{{UV}} run ruff check .
	{{UV}} run ruff format --check .

# Extra args forwarded, e.g.: just unittest -k foo -q
unittest *ARGS:
	{{UV}} run pytest {{ARGS}}
