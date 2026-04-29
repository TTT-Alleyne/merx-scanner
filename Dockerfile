FROM python:3.11-slim

# Install system deps for Selenium + Playwright
RUN apt-get update && apt-get install -y \
    wget curl gnupg unzip \
    chromium chromium-driver \
    fonts-liberation libasound2t64 libatk-bridge2.0-0 \
    libatk1.0-0 libcups2 libdbus-1-3 libgdk-pixbuf-xlib-2.0-0 libnspr4 \
    libnss3 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 \
    xdg-utils libgbm1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --timeout=120 --retries=5 -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium
RUN playwright install-deps chromium || true

COPY . .

RUN mkdir -p /results

CMD ["python", "runner.py"]
