#!/usr/bin/env bash
source .env
PGPASSWORD="$PG_PASS" psql-18 --host=localhost --port="$PG_PORT" --user="$PG_USER" --file="$1" "$PG_DB"