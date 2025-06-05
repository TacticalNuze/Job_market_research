# Stage 1: Installing uv and the project dependencies
FROM python:3.10-slim-bookworm AS builder
WORKDIR /app
RUN python -m venv .venv
ENV PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync

# Stage 2: Final image with Chrome only (no ChromeDriver)
FROM python:3.10-slim-bookworm AS celery_setup

COPY --from=builder /app/.venv /app/.venv
# Create user for Celery
RUN groupadd --gid 1000 celery_group \
    && useradd --uid 1000 --gid 1000 -m celery_user
# Install dependencies
RUN apt-get update && apt-get install -y \
    wget curl unzip jq gnupg ca-certificates fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 libnss3 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 \
    xdg-utils libgbm1 libu2f-udev libvulkan1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


# Create logs folder and set permissions
RUN mkdir -p /app/data_extraction/Websites/log

# Copy project files
COPY --chown=celery_user:celery_group . /app
WORKDIR /app
# Set environment
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app:/app/.venv/lib/python3.10/site-packages"

# Téléchargement automatique de la dernière version stable de Chrome
RUN CHROME_VERSION=$(curl -s https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json \
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
    && mv /tmp/chromedriver-linux64/chromedriver /home/celery_user/.local/share/undetected_chromedriver \
    && chmod +x /home/celery_user/.local/share/undetected_chromedriver \
    && rm -rf /tmp/chromedriver-linux64 chromedriver-linux64.zip
RUN chown celery_user:celery_group /home/celery_user/.local/share/undetected_chromedriver/chromedriver \
&& chmod u+rw /home/celery_user/.local/share/undetected_chromedriver/chromedriver


WORKDIR /app
