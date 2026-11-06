#!/usr/bin/env sh
# Showcase step 01 in the prep container, which has prep.py, prep_day.sql and
# the pinned DuckDB. Needs Docker and a .env (copy .env.example); ClickHouse
# is not started. --build keeps the image in step with the checkout. Rewrites
# output/ from input/.
set -eu
cd "$(dirname "$0")/../.."
exec docker compose --profile prep run --rm --build --no-deps \
  --user "$(id -u):$(id -g)" --env PYTHONUNBUFFERED=1 \
  --volume "$PWD/showcase:/showcase" --entrypoint python \
  prep /showcase/01-raw-to-prep/prep_snippet.py
