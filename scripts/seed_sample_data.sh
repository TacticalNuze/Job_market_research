#!/bin/bash
# Seed the `traitement` MinIO bucket from the sample files committed in traitement/,
# then run the loader so the star schema is populated and the Superset dashboards
# have something to read.
#
# This short-circuits scraping -> NER -> Spark: traitement/*.json is already in the
# exact shape Postgres/_init_postgres.py:load_offers() expects (job_url,
# date_publication, source, contrat, titre, compagnie, secteur, niveau_etudes,
# niveau_experience, description, skills[{nom, type_skill}]).
#
# Usage:  bash scripts/seed_sample_data.sh
# Requires: the dev stack already up (docker compose -f dockercompose.dev.yaml up -d).

set -euo pipefail

cd "$(dirname "$0")/.."

NETWORK="${NETWORK:-job_analytics_app_default}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"

echo "==> Uploading traitement/*.json to the MinIO 'traitement' bucket"
# Uses the mc client from the minio image the stack already pulls, so no host-side
# Python dependency is needed. //./ prefixes keep Git Bash on Windows from mangling
# the container-side absolute paths.
MSYS_NO_PATHCONV=1 docker run --rm \
  --network "$NETWORK" \
  -v "$(pwd)/traitement:/seed:ro" \
  --entrypoint sh \
  minio/mc -c "
    mc alias set local http://minio:9000 '$MINIO_ROOT_USER' '$MINIO_ROOT_PASSWORD' &&
    mc mb -p local/traitement &&
    mc cp /seed/*.json local/traitement/ &&
    mc ls local/traitement/
  "

echo
echo "==> Loading the bucket into PostgreSQL (fact_offre + dims + offre_skill)"
# The loader dedupes on job_url, so re-running is safe and the three sample files
# (which hold the same 145 offers from different enrichment runs) collapse to 145 rows.
docker compose -f dockercompose.dev.yaml run --rm pipeline_loader python load_offers.py

echo
echo "==> Row counts"
docker exec postgres psql -U "${POSTGRES_USER:-root}" -d "${POSTGRES_DB:-offers}" -c "
  SELECT 'fact_offre' AS table, COUNT(*) FROM fact_offre
  UNION ALL SELECT 'dim_skill',    COUNT(*) FROM dim_skill
  UNION ALL SELECT 'offre_skill',  COUNT(*) FROM offre_skill
  UNION ALL SELECT 'dim_compagnie',COUNT(*) FROM dim_compagnie
  UNION ALL SELECT 'dim_titre',    COUNT(*) FROM dim_titre;
"

echo
echo "Done. Now import superset/dashboard_export_20250807T121001.zip at http://localhost:8088"
