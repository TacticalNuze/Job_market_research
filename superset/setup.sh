#!/bin/bash

superset db upgrade

# Création de l'utilisateur admin (ignorer l'erreur si déjà créé)
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin || true

superset init

# Ajout de la connexion PostgreSQL
superset dbs add \
    --database-name "offers" \
    --sqlalchemy-uri "postgresql://root:123456@postgres:5432/offers" \
    --extra '{"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {}, "schemas_allowed_for_csv_upload": []}' \
    --expose-in-sql-lab || true

# Démarrer Superset
superset run -h 0.0.0.0 -p 8088
