import logging
import json
from kafka import KafkaConsumer

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Alerts")

# Настройка Kafka
ALERTS_TOPIC = "alerts"
consumer = KafkaConsumer(
    ALERTS_TOPIC,
    bootstrap_servers=['kafka:9093'],  # Используем INTERNAL listener
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def main():
    logger.info("Запущен сервис оповещений")
    with open("alerts.log", "a") as f:
        for message in consumer:
            alert = message.value
            log_line = f"[{alert['timestamp']}] {alert['attack_status']} | RF: {alert['rf_prediction']}, DT: {alert['dt_prediction']} | Source: {alert['src_ip']}:{alert['src_port']} -> Dest: {alert['dst_ip']}:{alert['dst_port']} | Protocol: {alert['protocol']}\n"
            logger.info(log_line.strip())
            f.write(log_line)
            f.flush()

if __name__ == "__main__":
    main()