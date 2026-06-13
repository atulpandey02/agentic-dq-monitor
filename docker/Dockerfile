FROM python:3.10-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy agent source
COPY agent/ ./agent/
COPY llmops/ ./llmops/
COPY evaluation/ ./evaluation/
COPY .env.example .env.example

# Expose FastAPI port
EXPOSE 8090

# Start the agent API
CMD ["python", "-m", "agent.api"]