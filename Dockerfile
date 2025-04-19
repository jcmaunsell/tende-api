# Build stage
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies with explicit upgrade of pip and explicit installation of python-multipart
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir python-multipart==0.0.9 && \
    pip install --no-cache-dir "ddtrace[fastapi]>=1.15.0" && \
    pip install --no-cache-dir -r requirements.txt

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m -u 1000 appuser && \
    mkdir -p /app /var/log && \
    chown -R appuser:appuser /app /var/log

# Copy only necessary files from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY . .

# Create log directories and files with proper permissions
RUN mkdir -p /var/log/tende-api && \
    touch /var/log/tende-api/api.log && \
    touch /app/api.log && \
    chown -R appuser:appuser /var/log/tende-api /app/api.log && \
    chmod 755 /var/log/tende-api && \
    chmod 644 /var/log/tende-api/api.log /app/api.log

# Switch to non-root user
USER appuser

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DD_AGENT_HOST=dd-agent \
    DD_ENV=development \
    DD_SERVICE=tende-api \
    DD_VERSION=0.1

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

# Run the application with ddtrace
CMD ["ddtrace-run", "uvicorn", "tende.main:app", "--host", "0.0.0.0", "--port", "8000"]
