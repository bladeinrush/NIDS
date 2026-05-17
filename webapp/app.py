from flask import Flask, render_template, Response, jsonify, request
import logging
from kafka import KafkaProducer, KafkaConsumer
import json
from datetime import datetime
import threading
import os
import time  # Добавлен импорт time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("webapp.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Reduce Kafka logging noise
logging.getLogger('kafka').setLevel(logging.WARNING)

app = Flask(__name__)

# Kafka settings
producer = KafkaProducer(
    bootstrap_servers=['kafka:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Store real-time alerts
alerts = []

# Consumer for real-time alerts
def consume_alerts():
    """Consume real-time alerts from Kafka topic 'alerts'."""
    consumer = KafkaConsumer(
        'alerts',
        bootstrap_servers=['kafka:9093'],
        auto_offset_reset='earliest',
        group_id='webapp-group-rt',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True,
        consumer_timeout_ms=10000
    )
    logger.info("Starting consumer for topic 'alerts'")
    for message in consumer:
        if message:
            try:
                data = message.value
                if isinstance(data['timestamp'], (int, float)):
                    data['timestamp'] = datetime.fromtimestamp(data['timestamp']).isoformat()
                elif isinstance(data['timestamp'], str):
                    try:
                        data['timestamp'] = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')).isoformat()
                    except ValueError:
                        logger.warning(f"Invalid timestamp format: {data['timestamp']}")
                        data['timestamp'] = datetime.now().isoformat()
                with threading.Lock():
                    alerts.append(data)
                    if len(alerts) > 1000:
                        alerts.pop(0)
                    logger.info(f"Received real-time alert: {data}")
            except Exception as e:
                logger.error(f"Error processing message from 'alerts': {e}", exc_info=True)

# Start consumer in a separate thread
threading.Thread(target=consume_alerts, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html', alerts=alerts)

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        try:
            file = request.files['file']
            if not file or not file.filename.endswith(('.pcap', '.pcapng')):
                return jsonify({"error": "Please upload a valid PCAP file"}), 400
            
            filepath = os.path.join('/app/uploads', file.filename)
            logger.info(f"Saving file to {filepath}")
            file.save(filepath)
            logger.info(f"Uploaded file saved to {filepath}")
            
            return jsonify({"message": f"File {file.filename} uploaded and queued for processing. View Alerts"})
        except Exception as e:
            logger.error(f"Error processing PCAP upload: {e}")
            return jsonify({"error": str(e)}), 500
    return render_template('upload.html', alerts=[])

@app.route('/alerts')
def get_alerts():
    logger.info(f"Sending {len(alerts)} real-time alerts to client")
    return jsonify(alerts)

@app.route('/stream')
def stream():
    def event_stream():
        while True:
            with threading.Lock():
                for alert in alerts:
                    yield f"data: {json.dumps(alert)}\n\n"
            time.sleep(1)  # Теперь работает, так как time импортирован
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    logger.info("Starting Flask application...")
    os.makedirs('/app/uploads', exist_ok=True)
    app.run(host='0.0.0.0', port=5000)