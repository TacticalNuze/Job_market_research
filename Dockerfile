# Stage 1: Installing uv and the project dependencies
FROM python:3.10-slim-bookworm AS builder

WORKDIR /app

# 1. Mettre à jour et installer curl/git, puis patcher tous les paquets
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends curl git python3-venv python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Installer uv via le script officiel
RUN curl -Ls https://astral.sh/uv/install.sh | bash \
    && mv /root/.local/bin/uv /usr/local/bin/uv

# 3. Créer et activer le virtualenv, préparer le PATH
ENV PATH="/app/.venv/bin:$PATH"
RUN python -m venv .venv && . .venv/bin/activate && python -m ensurepip --upgrade

# 4. Copier uniquement les fichiers de config Python (pour maximiser le cache Docker)
COPY pyproject.toml uv.lock ./
RUN uv sync

#RUN python -m spacy download en_core_web_lg

# -----------------------------------------------------------
# Stage 2: Image finale pour Celery (avec Chrome + Chromedriver)
# -----------------------------------------------------------
FROM python:3.10.10-slim-bullseye AS celery_setup

WORKDIR /app

# 1. Copier le virtualenv depuis l’étape builder
COPY --from=builder /app/.venv /app/.venv

# 2. Créer l’utilisateur non-root pour Celery
RUN groupadd --gid 1000 celery_group \
    && useradd --uid 1000 --gid 1000 -m celery_user

# 3. Mettre à jour, installer dépendances système + Chrome + Chromedriver
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
         wget curl unzip jq gnupg ca-certificates fonts-liberation \
         libappindicator3-1 libasound2 libatk-bridge2.0-0 libatk1.0-0 \
         libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 libnss3 \
         libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 \
         xdg-utils libgbm1 libu2f-udev libvulkan1 \
    && rm -rf /var/lib/apt/lists/* \
    \
    && CHROME_VERSION=$(curl -s https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json \
        | jq -r '.channels.Stable.version') \
    && wget -q https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chrome-linux64.zip \
    && unzip chrome-linux64.zip -d /opt/ \
    && mv /opt/chrome-linux64 /opt/chrome \
    && chmod +x /opt/chrome/chrome \
    && rm chrome-linux64.zip
# Creation du dossier pour le chromedriver


RUN mkdir -p /home/celery_user/.local/share/undetected_chromedriver \
 && chown -R celery_user:celery_group /home/celery_user/.local \
 && chmod -R u+rw /home/celery_user/.local

# Téléchargement automatique de la dernière version stable de Chromedriver
RUN CHROME_VERSION=$(curl -s https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json \
    | jq -r '.channels.Stable.version') \
    && wget  https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chromedriver-linux64.zip \
    && unzip chromedriver-linux64.zip -d /tmp \
    && mkdir -p /home/celery_user/.local/share/undetected_chromedriver \
    && mv /tmp/chromedriver-linux64/chromedriver /home/celery_user/.local/share/undetected_chromedriver/ \
    && chmod +x /home/celery_user/.local/share/undetected_chromedriver/chromedriver \
    && rm -rf /tmp/chromedriver-linux64 chromedriver-linux64.zip

RUN chown celery_user:celery_group /home/celery_user/.local/share/undetected_chromedriver/chromedriver \
&& chmod u+rw /home/celery_user/.local/share/undetected_chromedriver/chromedriver

# 5. Copier tout le code de l’application (celery_app et data_extraction seront écrasés par les volumes Compose)
COPY --chown=celery_user:celery_group . /app
RUN chown -R celery_user:celery_group /app/data_extraction/scraping_output

# 6. Définir l’environnement Python final (virtualenv 3.10)
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app:/app/.venv/lib/python3.10/site-packages"

# 7. Passer à l’utilisateur non-root
USER celery_user

# 8. Point d’entrée par défaut pour Celery (sera remplacé par 'command:' dans docker-compose)
#CMD ["bash", "-c", "source /app/.venv/bin/activate && which pip && which celery && celery --version"]

#CMD ["celery", "-A", "celery_app.tasks", "worker", "--loglevel=info", "-E"]
