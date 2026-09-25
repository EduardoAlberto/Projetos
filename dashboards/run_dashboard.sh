#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ ! -x ".venv/bin/streamlit" ]]; then
  echo "Ambiente virtual não encontrado. Execute primeiro:"
  echo "  python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

if [[ ! -f ".env" ]]; then
  echo "Arquivo .env não encontrado. Crie-o a partir de .env.example:"
  echo "  cp .env.example .env"
  exit 1
fi

exec .venv/bin/streamlit run app.py "$@"
