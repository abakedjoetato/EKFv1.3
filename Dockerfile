
# Use Python 3.11 slim image for Railway deployment
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV MODE=production

# Install system dependencies required for the bot
RUN apt-get update && apt-get install -y \
    openssh-client \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories for the bot
RUN mkdir -p assets bot/cogs bot/models bot/parsers bot/utils

# Set permissions for log files
RUN touch bot.log && chmod 666 bot.log

# Expose port 5000 for Railway health checks
EXPOSE 5000

# Health check for Railway
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://0.0.0.0:5000/health || exit 1

# Run the Discord bot
CMD ["python3", "main.py"]
