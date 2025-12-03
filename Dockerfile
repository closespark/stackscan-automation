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

# Default command: run all workers sequentially via daily_worker.py
# The daily_worker.py runs: pipeline_worker → outreach_worker → calendly_worker
# For Render: Use "python daily_worker.py" in cron job or worker command
# Or use: bash -c "python pipeline_worker.py && python outreach_worker.py"
CMD ["python", "daily_worker.py"]
