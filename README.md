# NIDS — Real-Time Network Intrusion Detection System

A real-time Network Intrusion Detection System (NIDS) that uses machine learning to detect network attacks. Built with Apache Kafka for stream processing and trained on the NSL-KDD dataset.

---

## Overview

This system captures network traffic, processes packets in real time, and classifies them as **normal** or **attack** using an ensemble of machine learning models (Random Forest + Decision Tree). Alerts are streamed through Kafka and displayed via a web dashboard.

---

## Architecture

```
Network Traffic
      │
      ▼
  Producer  ──────►  Kafka  ──────►  Consumer (ML inference)
                                           │
                                           ▼
                                     Alerts Topic
                                           │
                                           ▼
                                     Web Dashboard
```

**Components:**
- **Producer** — captures and sends network packets to Kafka
- **Consumer** — receives packets, extracts NSL-KDD features, runs ML models
- **Alerts service** — listens for alerts and logs/displays them
- **File Processor** — processes PCAP files offline
- **Web App** — dashboard for visualizing alerts in real time

---

## Tech Stack

- **Python 3.10+**
- **Apache Kafka** + Zookeeper (via Confluent)
- **Scikit-learn** — Random Forest & Decision Tree models
- **Scapy** — packet capture and analysis
- **Docker** + Docker Compose
- **NSL-KDD Dataset** — for model training

---

## Requirements

- [Docker](https://www.docker.com/) and Docker Compose
- Git

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/bladeinrush/NIDS.git
cd NIDS
```

### 2. Build and start all services

```bash
docker-compose up --build
```

This will start:
- Zookeeper
- Kafka broker
- Producer (network traffic capture)
- Consumer (ML inference engine)
- Alerts service
- Web dashboard

### 3. Access the dashboard

Open your browser and go to:
```
http://localhost:5000
```

### 4. Stop the system

```bash
docker-compose down
```

---

## ML Models

The system uses an **ensemble approach** combining two models:

| Model | Description |
|---|---|
| Random Forest | High accuracy, handles noisy data well |
| Decision Tree | Fast inference, interpretable results |

Final prediction is based on the **average probability** of both models. A packet is flagged as an attack if the probability exceeds `0.5`.

### Features

The system extracts **41 NSL-KDD features** from live network traffic including:
- Connection duration and protocol type
- Service and flag information
- Source/destination byte counts
- Connection rate statistics (same_srv_rate, diff_srv_rate, etc.)
- Host-based traffic features

---

## Dataset

Models are trained on the **NSL-KDD** dataset — an improved version of the KDD Cup 1999 dataset, widely used for evaluating intrusion detection systems.

- Training set: `KDDTrain+.arff`
- Test set: `KDDTest-21.arff`

---

## Project Structure

```
NIDS/
├── consumer.py                  # ML inference engine (Kafka consumer)
├── producer.py                  # Network packet producer
├── alerts.py                    # Alert handler
├── train.py                     # Model training script
├── train_kddcup.py              # KDD Cup training utilities
├── file_processor/              # Offline PCAP file analysis
│   ├── file_processor.py
│   └── models/                  # Trained ML models
├── webapp/                      # Web dashboard
├── models/                      # Serialized models (.pkl)
├── docker-compose.yml           # Full system orchestration
├── Dockerfile-consumer
├── Dockerfile-producer
├── Dockerfile-alerts
├── Dockerfile-train
├── requirements.txt
└── .gitignore
```

---

## Web Interface

The system includes a Flask-based web dashboard accessible at `http://localhost:5000`.

### Pages

**Dashboard (`/`)** — main monitoring page:
- Total alerts counter
- Attacks detected counter
- Real-time alerts table with live updates via SSE (Server-Sent Events)
- Pause / Resume live feed
- Clear alerts button

**Upload (`/upload`)** — upload a `.pcap` or `.pcapng` file for offline analysis. The file is queued and processed by the File Processor service.

### API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Main dashboard |
| `/upload` | GET / POST | Upload PCAP file |
| `/alerts` | GET | Returns all alerts as JSON |
| `/stream` | GET | SSE stream of live alerts |



