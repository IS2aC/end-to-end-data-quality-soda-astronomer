# Utiliser l'image de base Astronomer
FROM quay.io/astronomer/astro-runtime:12.6.0

# Passer à l'utilisateur root pour installer les dépendances
USER root

# Mettre à jour les paquets et installer les dépendances système nécessaires
RUN chmod -R 755 /var/lib/apt/lists/ && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update && \
    apt-get install -y \
    python3-venv \
    python3-dev \
    build-essential \
    gcc \
    libpq-dev \
    libssl-dev \
    libffi-dev \
    && apt-get clean

# Créer un environnement virtuel pour Soda
RUN python3.11 -m venv soda_venv && source soda_venv/bin/activate &&\
    pip install -i https://pypi.cloud.soda.io soda-core &&\
    pip install -i https://pypi.cloud.soda.io soda-scientific &&\
    pip install --no-cache-dir -i https://pypi.cloud.soda.io "soda-snowflake"

