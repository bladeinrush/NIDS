FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y \
    libpcap0.8-dev \
    && rm -rf /var/lib/apt/lists/*

COPY app.py .
COPY templates/ ./templates/
COPY static/ ./static/
COPY uploads/ ./uploads/

EXPOSE 5000

CMD ["python", "app.py"]