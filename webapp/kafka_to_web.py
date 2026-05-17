import logging
from kafka import KafkaConsumer, KafkaAdminClient
from flask import Flask, jsonify, Response
from flask_cors import CORS
import threading
import json
from datetime import datetime
import time
from flask import Flask, Response, request

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("kafka_to_web.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("KafkaToWeb")

app = Flask(__name__)
CORS(app)

# Store file alerts
file_alerts = []
last_sent_indices = {}  # Храним last_sent для каждого клиента по session_id

def consume_file_alerts():
    """Consume file alerts from Kafka topic 'file_alerts'."""
    while True:
        try:
            logger.info("Attempting to connect to Kafka for topic 'file_alerts'")
            admin_client = KafkaAdminClient(bootstrap_servers=['kafka:9093'])
            topics = admin_client.list_topics()
            logger.info(f"Available Kafka topics: {topics}")
            admin_client.close()

            consumer = KafkaConsumer(
                'file_alerts',
                bootstrap_servers=['kafka:9093'],
                auto_offset_reset='earliest',
                group_id='webapp-group-file-2',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                enable_auto_commit=True,
                consumer_timeout_ms=10000
            )
            logger.info("Successfully started consumer for topic 'file_alerts'")
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
                            file_alerts.append(data)
                            if len(file_alerts) > 1000:
                                file_alerts.pop(0)
                            logger.info(f"Received and stored file alert: {data}")
                    except Exception as e:
                        logger.error(f"Error processing message from 'file_alerts': {e}", exc_info=True)
                else:
                    logger.warning("No message received from 'file_alerts', checking connection...")
        except Exception as e:
            logger.error(f"Failed to initialize or maintain consumer for 'file_alerts': {e}", exc_info=True)
            time.sleep(5)

# Start consumer in a separate thread
threading.Thread(target=consume_file_alerts, daemon=True).start()

@app.route('/file_alerts')
def get_file_alerts():
    logger.info(f"Sending {len(file_alerts)} file alerts to client")
    return jsonify(file_alerts)

@app.route('/file_stream')
def file_stream():
    session_id = request.args.get('session_id', str(time.time()))  # Уникальный ID сессии
    def event_stream():
        logger.info(f"Starting SSE event stream for file alerts, session_id: {session_id}")
        if session_id not in last_sent_indices:
            last_sent_indices[session_id] = 0
        while True:
            try:
                with threading.Lock():
                    last_sent = last_sent_indices[session_id]
                    for i in range(last_sent, len(file_alerts)):
                        alert = file_alerts[i]
                        logger.info(f"Sending alert via SSE (session {session_id}): {alert}")
                        yield f"data: {json.dumps(alert)}\n\n"
                    last_sent_indices[session_id] = len(file_alerts)
                time.sleep(1)
            except GeneratorExit:
                logger.info(f"SSE stream closed for session {session_id}")
                break
            except Exception as e:
                logger.error(f"Error in SSE stream (session {session_id}): {e}")
                time.sleep(5)
    response = Response(event_stream(), mimetype="text/event-stream")
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    return response

@app.route('/clear_file_alerts', methods=['POST'])
def clear_file_alerts():
    with threading.Lock():
        global file_alerts
        file_alerts = []
        # Сбрасываем last_sent для всех сессий
        for session_id in last_sent_indices:
            last_sent_indices[session_id] = 0
        logger.info("File alerts cleared on server, last_sent indices reset")
    return jsonify({"message": "File alerts cleared"})

if __name__ == '__main__':
    logger.info("Starting Kafka to Web bridge...")
    try:
        app.run(host='0.0.0.0', port=5001)
    except Exception as e:
        logger.error(f"Failed to start Flask server: {e}", exc_info=True)