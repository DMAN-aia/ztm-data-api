FROM python:3.11-bullseye

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    xdg-utils \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download TLS library during build using the library's own download mechanism
# This runs while Docker build has network access; at Render runtime GitHub is blocked
RUN python -c "from tls_requests import TLSLibrary; TLSLibrary.download(version='1.13.1')" \
    && find /usr/local/lib -name "*.so" -path "*/tls_requests/*" -o -name "*.so" -path "*/tls_client/*" | head -20

COPY . .

ENV PYTHONUNBUFFERED=1
ENV SOCCERDATA_DIR=/tmp/soccerdata_cache
ENV DISPLAY=:99

EXPOSE 10000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "10000"]
