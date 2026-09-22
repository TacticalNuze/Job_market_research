#!/bin/bash

# Identifiants lus depuis l'environnement (.docker.env), avec les valeurs
# actuelles conservées en repli pour ne rien casser en local.
POSTGRES_USER="${POSTGRES_USER:-root}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-123456}"
POSTGRES_DB="${POSTGRES_DB:-offers}"
DB_HOST="${DB_HOST:-postgres}"
DB_PORT="${DB_PORT:-5432}"
SUPERSET_ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-admin}"

superset db upgrade
superset re-encrypt-secrets
# Création de l'utilisateur admin (ignorer l'erreur si déjà créé)
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password "${SUPERSET_ADMIN_PASSWORD}" || true

superset init

# Ajout de la connexion PostgreSQL
superset dbs add \
    --database-name "${POSTGRES_DB}" \
    --sqlalchemy-uri "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${DB_HOST}:${DB_PORT}/${POSTGRES_DB}" \
    --extra '{"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {}, "schemas_allowed_for_csv_upload": []}' \
    --expose-in-sql-lab || true



# Démarrer Superset
superset run -h 0.0.0.0 -p 8088
