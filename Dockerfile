# Use the official Python image with a specific version
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Create logs directory with proper permissions
RUN mkdir -p /app/logs && chmod -R 777 /app/logs

# Health check configuration (for Koyeb monitoring)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s \
    CMD python healthcheck.py || exit 1

# Main command to run the bot with log streaming
CMD (python bot.py &) && tail -f /app/logs/bot.log
