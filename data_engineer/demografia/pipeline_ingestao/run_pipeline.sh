#!/usr/bin/env bash
set -Eeuo pipefail

PIPELINE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$PIPELINE_DIR/src"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
export RAW_DIR="${RAW_DIR:-$HOME/LoadFile/raw}"
export STAGING_DIR="${STAGING_DIR:-$HOME/LoadFile/staging}"
export SPARK_WAREHOUSE_DIR="${SPARK_WAREHOUSE_DIR:-$HOME/LoadFile/output}"

if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
    printf 'Erro: defina POSTGRES_PASSWORD antes de executar o pipeline.\n' >&2
    exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    printf 'Erro: Python nao encontrado: %s\n' "$PYTHON_BIN" >&2
    exit 1
fi

if ! command -v "$SPARK_SUBMIT" >/dev/null 2>&1; then
    printf 'Erro: spark-submit nao encontrado: %s\n' "$SPARK_SUBMIT" >&2
    exit 1
fi

run_step() {
    local name="$1"
    shift
    printf '\n==> %s\n' "$name"
    "$@"
}

trap 'printf "\nPipeline interrompido na linha %s.\n" "$LINENO" >&2' ERR

run_step "Extract" bash -c "cd '$SRC_DIR/extract' && '$PYTHON_BIN' config.py"
run_step "Staging" bash -c "'$PYTHON_BIN' '$SRC_DIR/staging/staging.py'"
run_step "Ingest Bronze" "$SPARK_SUBMIT" --master "$SPARK_MASTER" --name demografia-ingest "$SRC_DIR/ingest/config.py"
run_step "Process Silver" "$SPARK_SUBMIT" --master "$SPARK_MASTER" --name demografia-process "$SRC_DIR/process/config.py"

printf '\nPipeline concluido com sucesso.\n'
