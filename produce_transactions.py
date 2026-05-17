import logging
import json
import base64
import time
import os
from scapy.all import sniff, IP, TCP, UDP
from kafka import KafkaProducer

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ProduceTransactions")

# Настройки Kafka
KAFKA_TOPIC = "network_logs"
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def process_packet(packet):
    if IP in packet:
        log = {
            "timestamp": time.time(),
            "src_ip": packet[IP].src,
            "dst_ip": packet[IP].dst,
            "protocol": packet[IP].proto,
            "src_port": packet[TCP].sport if TCP in packet else packet[UDP].sport if UDP in packet else 0,
            "dst_port": packet[TCP].dport if TCP in packet else packet[UDP].dport if UDP in packet else 0,
            "payload": base64.b64encode(bytes(packet)).decode('utf-8')
        }
        logger.info(f"Захвачен пакет: {log}")
        producer.send(KAFKA_TOPIC, value=log)
        producer.flush()

def main():
    # Получаем интерфейс из переменной окружения или используем дефолтный
    interface = os.getenv("NETWORK_INTERFACE", "Ethernet")  # Дефолт для Windows
    logger.info(f"Начинаем захват трафика на интерфейсе: {interface}")
    sniff(iface=interface, prn=process_packet, store=False)

if __name__ == "__main__":
    main()