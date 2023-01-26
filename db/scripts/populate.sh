#!/bin/bash

PGPASSWORD="$POSTGRES_PASSWORD" \
  psql -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -v BARS_1_DATA_PATH="${SAMPLE_DATA_PATH}/bars_1.csv" \
  -v BARS_2_DATA_PATH="${SAMPLE_DATA_PATH}/bars_2.csv" \
  -f /scripts/populate.sql