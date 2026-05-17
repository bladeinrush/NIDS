FROM python:3.9

WORKDIR /app

# Install system dependencies for compiling packages
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --no-cache-dir -U pip

# Uninstall conflicting packages
RUN pip uninstall -y numpy pandas scikit-learn joblib || true

# Copy and install requirements with verbose logging
COPY requirements.txt .
RUN pip install --no-cache-dir --verbose -r requirements.txt

# Copy application files
COPY . .

# Environment variables for Kafka
ENV PYTHONUNBUFFERED=1
ENV KAFKA_BROKER=kafka:9093

# Health check and run application
CMD ["python", "file_processor.py"]