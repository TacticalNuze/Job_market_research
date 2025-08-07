#!/bin/bash

superset db upgrade
superset re-encrypt-secrets
# Création de l'utilisateur admin (ignorer l'erreur si déjà créé)
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin || true

superset init



cat <<EOF | flask shell
from superset import db
from superset.models.core import Database

db.session.add(Database(
    database_name="offers",
    sqlalchemy_uri="postgresql://root:123456@postgres:5432/offers",
    extra='{"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {}, "schemas_allowed_for_csv_upload": []}'
))
db.session.commit()
EOF


# Démarrer Superset
superset run -h 0.0.0.0 -p 8088
