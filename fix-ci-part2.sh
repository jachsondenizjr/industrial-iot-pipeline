#!/usr/bin/env bash
# Parte 2 da correcao do CI/CD do industrial-iot-pipeline.
# O fix-ci.sh anterior parou no meio (falhou no passo que usava "python3",
# que nao existe nesse Git Bash) — por isso venv/ e a pasta duplicada ja
# foram removidos e corrigidos, mas sensor_simulator.py, o arquivo de
# testes, o .flake8 e o ci.yml ainda nao. Este script termina o que faltou,
# sem depender de python3 (so bash/sed).
#
# Rode a partir da RAIZ do repositorio (onde fica a pasta .git).
set -e

if [ ! -d ".git" ]; then
  echo "ERRO: rode este script a partir da raiz do repositorio (onde esta a pasta .git)."
  exit 1
fi

echo "1) Removendo imports nao usados de data_generator/sensor_simulator.py..."
sed -i '/^import time$/d' data_generator/sensor_simulator.py
sed -i '/^from typing import Generator$/d' data_generator/sensor_simulator.py

echo "2) Reescrevendo tests/test_sensor_simulator.py..."
cat > tests/test_sensor_simulator.py << 'FILE_EOF'
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_generator.sensor_simulator import (  # noqa: E402
    generate_sensor_reading,
    save_batch_to_json,
    EQUIPMENT_FLEET,
)


def test_equipment_fleet_not_empty():
    assert len(EQUIPMENT_FLEET) > 0


def test_sensor_reading_has_required_fields():
    equipment = EQUIPMENT_FLEET[0]
    reading = generate_sensor_reading(equipment)
    required = ["event_id", "equipment_id", "timestamp", "rpm",
                "temp_coolant_c", "nox_ppm", "is_anomaly"]
    for field in required:
        assert field in reading


def test_sensor_values_within_range():
    equipment = EQUIPMENT_FLEET[0]
    for _ in range(50):
        reading = generate_sensor_reading(equipment)
        assert reading["rpm"] >= 0
        assert reading["temp_coolant_c"] >= 50
        assert reading["nox_ppm"] >= 0
        assert 0 <= reading["load_pct"] <= 100


def test_event_id_is_unique():
    equipment = EQUIPMENT_FLEET[0]
    ids = [generate_sensor_reading(equipment)["event_id"] for _ in range(100)]
    assert len(set(ids)) == 100


def test_save_batch_creates_file(tmp_path):
    import json
    output = str(tmp_path / "test.json")
    save_batch_to_json(output, num_events=10)
    assert os.path.exists(output)
    with open(output) as f:
        data = json.load(f)
    assert len(data) == 10
FILE_EOF

echo "3) Criando .flake8 (config unica de lint, exclui notebooks/)..."
cat > .flake8 << 'FILE_EOF'
[flake8]
max-line-length = 120
exclude =
    .git,
    __pycache__,
    venv,
    .venv,
    *.egg-info,
    dist,
    build,
    notebooks
FILE_EOF

echo "4) Simplificando .github/workflows/ci.yml (usa o .flake8 acima)..."
cat > .github/workflows/ci.yml << 'FILE_EOF'
name: CI/CD — Industrial IoT Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install pandas pytest python-dotenv

      - name: Run data generator tests
        run: |
          python -m pytest tests/ -v

      - name: Lint with flake8
        run: |
          pip install flake8
          flake8 .
FILE_EOF

echo ""
echo "Tudo aplicado. Se pytest/flake8 estiverem instalados neste terminal, conferindo agora:"
if command -v flake8 >/dev/null 2>&1 && command -v pytest >/dev/null 2>&1; then
  python -m pytest tests/ -v
  flake8 .
  echo "OK: pytest e flake8 passaram limpos."
else
  echo "(pytest/flake8 nao encontrados neste terminal local — sem problema, o GitHub Actions valida no push)"
fi

echo ""
echo "Arquivos prontos. Revise no Source Control do VS Code, escreva a mensagem de commit e faca o push."
