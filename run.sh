#!/bin/sh
set -e

python wait_for_db.py
uvicorn main:app --host 0.0.0.0 --port 8000
