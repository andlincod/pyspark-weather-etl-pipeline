# Multi-stage Dockerfile for Weather ETL Pipeline
# Stage 1: Base Python environment with Java
FROM openjdk:8-jdk-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic links for python
RUN ln -s /usr/bin/python3 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

# Stage 2: Build dependencies
FROM base as builder

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Stage 3: Production image
FROM base as production

# Create app user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy Python dependencies from builder stage
COPY --from=builder /usr/local/lib/python3.*/site-packages /usr/local/lib/python3.*/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/data/raw /app/data/processed /app/data/temp /app/data/backup /app/logs

# Set permissions
RUN chown -R appuser:appuser /app

# Switch to app user
USER appuser

# Expose port for Jupyter notebook (if needed)
EXPOSE 8888

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import pyspark; print('Health check passed')" || exit 1

# Default command
CMD ["python", "dags/etl_pipeline.py", "--mode", "single"]

# Stage 4: Development image
FROM production as development

# Switch back to root for development tools
USER root

# Install development dependencies
RUN pip install --no-cache-dir \
    jupyter \
    jupyterlab \
    ipykernel \
    pytest-cov \
    black \
    flake8

# Install Jupyter kernel
RUN python -m ipykernel install --user --name weather-etl --display-name "Weather ETL"

# Expose Jupyter port
EXPOSE 8888

# Switch back to app user
USER appuser

# Development command
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
