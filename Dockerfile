FROM python:3.11-slim

WORKDIR /app

# System deps (optional, but nice to have)
RUN apt-get update && apt-get install -y \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Install the hubspot_scanner package
RUN pip install -e .

# Default command: run pipeline once (Render cron will trigger this)
CMD ["python", "pipeline_worker.py"]
