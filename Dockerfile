FROM python:3.11-bullseye

# Install Chrome via direct .deb download (works in Docker without apt repo setup)
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

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-bake tls-client .so during Docker build (Render blocks GitHub downloads at runtime)
# Correct filename from bogdanfinn/tls-client v1.13.1 release assets
RUN TLS_LIB_DIR=$(python -c "import tls_client, os; print(os.path.join(os.path.dirname(tls_client.__file__), 'dependencies'))") \
    && mkdir -p "$TLS_LIB_DIR" \
    && wget -q "https://github.com/bogdanfinn/tls-client/releases/download/v1.13.1/tls-client-linux-ubuntu-amd64-1.13.1.so" \
         -O "$TLS_LIB_DIR/tls-client-linux-ubuntu-amd64-1.13.1.so" \
    && echo "TLS lib installed:" \
    && ls -lh "$TLS_LIB_DIR"

# Copy app
COPY . .

# Environment
ENV PYTHONUNBUFFERED=1
ENV SOCCERDATA_DIR=/tmp/soccerdata_cache
ENV DISPLAY=:99

EXPOSE 10000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "10000"]
