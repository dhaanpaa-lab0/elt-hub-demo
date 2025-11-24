#!/usr/bin/env bash
source .env
PGPASSWORD="$PG_PASS" psql-18 --host=localhost --port=5532 --user="$PG_USER" "$PG_DB"