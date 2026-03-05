#!/usr/bin/env bash
set -euo pipefail
python3 scripts/cluster_analysis.py sample_input.json --output sample_output.json
python3 scripts/generate_report.py sample_output.json --report sample_report.txt
cat sample_report.txt
