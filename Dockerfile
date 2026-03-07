FROM python:3.11-bullseye

# Install Chrome + Chromedriver
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install matching Chromedriver via new Chrome for Testing endpoint
RUN CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+\.\d+') \
    && curl -s "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json" \
    | python3 -c "
import sys, json
data = json.load(sys.stdin)
version = '$CHROME_VERSION'
major = version.split('.')[0]
for v in reversed(data['versions']):
    if v['version'].split('.')[0] == major:
        for d in v.get('downloads', {}).get('chromedriver', []):
            if d['platform'] == 'linux64':
                print(d['url'])
                break
        break
" | xargs wget -q -O /tmp/chromedriver.zip \
    && unzip /tmp/chromedriver.zip -d /tmp/chromedriver_dir \
    && mv /tmp/chromedriver_dir/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /tmp/chromedriver.zip /tmp/chromedriver_dir

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Environment
ENV PYTHONUNBUFFERED=1
ENV SOCCERDATA_DIR=/tmp/soccerdata_cache
ENV DISPLAY=:99

EXPOSE 10000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "10000"]
