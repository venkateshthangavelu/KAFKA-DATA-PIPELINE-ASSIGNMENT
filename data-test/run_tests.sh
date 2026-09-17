#!/bin/bash
set -e

cd "$(dirname "$0")"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q --html=reports/customer_account_report.html
