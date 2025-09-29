#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_PATH="${1:-"$ROOT_DIR/dist/cephtools"}"

cd "${ROOT_DIR}"

export UV_CACHE_DIR="${ROOT_DIR}/.uv-cache"
export PIP_CACHE_DIR="${ROOT_DIR}/.pip-cache"
export PEX_ROOT="${ROOT_DIR}/.pex-build"
STAGING_DIR="${ROOT_DIR}/build/pex-app"

mkdir -p "dist" "${PEX_ROOT}" "${PIP_CACHE_DIR}" "${STAGING_DIR}"

uv build >/dev/null

wheel="$(ls -t dist/cephtools-*.whl | head -n 1 || true)"
if [[ -z "${wheel:-}" ]]; then
	echo "Unable to locate built wheel in dist/. Did uv build succeed?" >&2
	exit 1
fi

wheel="$(realpath "${wheel}")"

VENV_PYTHON="${ROOT_DIR}/.venv/bin/python"
VENV_PEX="${ROOT_DIR}/.venv/bin/pex"

if [[ ! -x "${VENV_PYTHON}" ]]; then
	echo "Missing virtual environment at .venv/. Run 'just sync' first." >&2
	exit 1
fi

"${VENV_PYTHON}" -m ensurepip --upgrade >/dev/null

if [[ ! -x "${VENV_PEX}" ]]; then
	echo "pex is not installed in the project environment. Run 'just sync' first." >&2
	exit 1
fi

rm -rf "${STAGING_DIR:?}/"*
"${VENV_PYTHON}" -m pip install --target "${STAGING_DIR}" "${wheel}" >/dev/null
"${VENV_PEX}" -D "${STAGING_DIR}" \
	-m cephtools:main.cli \
	--python-shebang "/usr/bin/env python3" \
	-o "${OUTPUT_PATH}"

echo "Wrote ${OUTPUT_PATH}"
