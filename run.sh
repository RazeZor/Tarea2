#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p resultados

echo "=== 1. Preparando entorno virtual Python (.venv) ==="
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
.venv/bin/pip install -q -r requirements.txt

echo "=== 2. Levantando infraestructura Docker (Redis 7 + HBase 2.5) ==="
docker compose up -d --wait

echo "=== 3. Ejecutando pipeline completo (Partes B, C y D) ==="
.venv/bin/python3 -u src/pipeline_completo.py 2>&1 | tee resultados/salida_completa.txt

echo "=== 4. Actualizando README.md e informe/informe.pdf ==="
.venv/bin/python3 src/generar_documentos.py
