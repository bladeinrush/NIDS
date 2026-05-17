import base64
import logging
import json
import os
import pandas as pd
import joblib
from scapy.all import rdpcap, IP, TCP, UDP, ICMP
from collections import defaultdict
import time
import numpy
from kafka import KafkaProducer

print("numpy version:", numpy.__version__)

# Настройка логирования
log_file = "/tmp/file_processor.log"
fallback_log_file = "/var/log/file_processor.log"

try:
    log_dir = os.path.dirname(log_file) or "/tmp"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    with open(log_file, 'a') as f:
        f.write("Log file test write\n")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging to file initialized successfully at {log_file}")
except Exception as e:
    logging.warning(f"Cannot write to {log_file}: {e}")
    try:
        log_dir = os.path.dirname(fallback_log_file) or "/var/log"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        with open(fallback_log_file, 'a') as f:
            f.write("Log file test write\n")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(fallback_log_file),
                logging.StreamHandler()
            ]
        )
        logging.info(f"Logging to fallback file initialized successfully at {fallback_log_file}")
    except Exception as e:
        logging.warning(f"Cannot write to {fallback_log_file}: {e}")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[logging.StreamHandler()]
        )
        logging.warning("Falling back to console logging only")

logger = logging.getLogger("FileProcessor")

# Инициализация Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9093'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        max_block_ms=10000
    )
    logger.info("Successfully initialized Kafka Producer at kafka:9093")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")
    raise

# Проверка подключения к Kafka
try:
    producer.bootstrap_connected()
    logger.info("Successfully connected to Kafka at kafka:9093")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    raise

# Load models and preprocessors
try:
    logger.info("Listing files in models directory:")
    logger.info(os.listdir("models/"))
    rf_model = joblib.load("models/rf_model_new.pkl")
    dt_model = joblib.load("models/dt_model_new.pkl")
    encoder = joblib.load("models/encoder_new.pkl")
    scaler = joblib.load("models/scaler_new.pkl")
    feature_order = joblib.load("models/feature_order_new.pkl")
    logger.info("Models and preprocessors loaded successfully")
    logger.info(f"Expected model features (feature_order): {feature_order}")
    log_message = {"message": "Models and preprocessors loaded successfully", "timestamp": time.time(), "file": None}
    producer.send('file_processing_logs', log_message)
    producer.flush()
    logger.info("Sent log to Kafka: %s", log_message)
except Exception as e:
    logger.error(f"Failed to load models: {e}")
    log_message = {"message": f"Failed to load models: {e}", "timestamp": time.time(), "file": None}
    producer.send('file_processing_logs', log_message)
    producer.flush()
    logger.info("Sent error log to Kafka: %s", log_message)
    raise

# Throttling state for NORMAL alerts
last_normal_alert = defaultdict(float)

# Track connection state for computing NSL-KDD-like features
connection_stats = defaultdict(lambda: {
    "start_time": None,
    "count": 0,
    "srv_count": 0,
    "same_srv_rate": 0.0,
    "diff_srv_rate": 0.0,
    "serror_rate": 0.0,
    "rerror_rate": 0.0,
    "dst_host_count": 0,
    "dst_host_srv_count": 0,
    "services": set(),
    "src_ports": set(),
})

def transform_log_to_features(log):
    try:
        payload_bytes = log.get("payload", b"").encode() if isinstance(log.get("payload"), str) else log.get("payload", b"")
        
        conn_key = (log["src_ip"], log["dst_ip"], log["dst_port"], log["protocol"])
        stats = connection_stats[conn_key]
        
        if stats["start_time"] is None:
            stats["start_time"] = log["timestamp"]
        stats["count"] += 1
        stats["dst_host_count"] += 1
        
        service = "http" if log["dst_port"] == 80 else "https" if log["dst_port"] == 443 else \
                  "dns" if log["dst_port"] == 53 else "kafka" if log["dst_port"] == 9092 else "unknown"
        stats["services"].add(service)
        if service == next(iter(stats["services"])):
            stats["srv_count"] += 1
        stats["dst_host_srv_count"] = len(stats["services"])
        
        stats["same_srv_rate"] = stats["srv_count"] / stats["count"] if stats["count"] > 0 else 0.0
        stats["diff_srv_rate"] = 1.0 - stats["same_srv_rate"]
        
        stats["src_ports"].add(log["src_port"])
        
        flag = "SF"
        
        duration = log["timestamp"] - stats["start_time"] if stats["start_time"] else 0.0
        
        features = {
            "duration": duration,
            "protocol_type": "tcp" if log["protocol"] == 6 else "udp" if log["protocol"] == 17 else "icmp",
            "service": service,
            "flag": flag,
            "src_bytes": len(payload_bytes),
            "dst_bytes": log.get("dst_bytes", 0),
            "land": 1 if log["src_ip"] == log["dst_ip"] and log["src_port"] == log["dst_port"] else 0,
            "wrong_fragment": 0,
            "urgent": 0,
            "hot": 0,
            "num_failed_logins": 0,
            "logged_in": 0,
            "num_compromised": 0,
            "root_shell": 0,
            "su_attempted": 0,
            "num_root": 0,
            "num_file_creations": 0,
            "num_shells": 0,
            "num_access_files": 0,
            "num_outbound_cmds": 0,
            "is_host_login": 0,
            "is_guest_login": 0,
            "count": stats["count"],
            "srv_count": stats["srv_count"],
            "serror_rate": stats["serror_rate"],
            "srv_serror_rate": stats["serror_rate"],
            "rerror_rate": stats["rerror_rate"],
            "srv_rerror_rate": stats["rerror_rate"],
            "same_srv_rate": stats["same_srv_rate"],
            "diff_srv_rate": stats["diff_srv_rate"],
            "srv_diff_host_rate": 0.0,
            "dst_host_count": stats["dst_host_count"],
            "dst_host_srv_count": stats["dst_host_srv_count"],
            "dst_host_same_srv_rate": stats["same_srv_rate"],
            "dst_host_diff_srv_rate": stats["diff_srv_rate"],
            "dst_host_same_src_port_rate": len(stats["src_ports"]) / stats["count"] if stats["count"] > 0 else 0.0,
            "dst_host_srv_diff_host_rate": 0.0,
            "dst_host_serror_rate": stats["serror_rate"],
            "dst_host_srv_serror_rate": stats["serror_rate"],
            "dst_host_rerror_rate": stats["rerror_rate"],
            "dst_host_srv_rerror_rate": stats["rerror_rate"],
        }
        file_name = os.path.basename(log.get("file", "unknown"))
        log_message = {"message": f"Transformed features for packet in {file_name}", "timestamp": time.time(), "file": file_name}
        producer.send('file_processing_logs', log_message)
        producer.flush()
        logger.info("Sent log to Kafka: %s", log_message)
        logger.info(f"Transformed features for packet in {file_name}")
        return pd.DataFrame([features])
    except Exception as e:
        file_name = os.path.basename(log.get("file", "unknown"))
        log_message = {"message": f"Error transforming log to features in {file_name}: {e}", "timestamp": time.time(), "file": file_name}
        producer.send('file_processing_logs', log_message)
        producer.flush()
        logger.info("Sent error log to Kafka: %s", log_message)
        logger.error(f"Error transforming log to features: {e}")
        return None

def preprocess_features(df):
    try:
        cat_cols = ["protocol_type", "service", "flag"]
        num_cols = [col for col in df.columns if col not in cat_cols]
        
        encoded = pd.DataFrame(encoder.transform(df[cat_cols]), columns=encoder.get_feature_names_out(cat_cols))
        
        scaled_num = scaler.transform(df[num_cols])
        df_num = pd.DataFrame(scaled_num, columns=num_cols)
        
        final_data = pd.concat([df_num, encoded], axis=1)
        
        for col in feature_order:
            if col not in final_data.columns:
                final_data[col] = 0
        final_data = final_data[feature_order]
        
        file_name = os.path.basename(df.index.name or "unknown")
        log_message = {"message": f"Preprocessed features for packet in {file_name}", "timestamp": time.time(), "file": file_name}
        producer.send('file_processing_logs', log_message)
        producer.flush()
        logger.info("Sent log to Kafka: %s", log_message)
        logger.info(f"Processed feature count: {final_data.shape[1]}")
        if final_data.shape[1] != len(feature_order):
            raise ValueError(f"Expected {len(feature_order)} features, got {final_data.shape[1]}")
        return final_data
    except Exception as e:
        file_name = os.path.basename(df.index.name or "unknown")
        log_message = {"message": f"Error preprocessing features in {file_name}: {e}", "timestamp": time.time(), "file": file_name}
        producer.send('file_processing_logs', log_message)
        producer.flush()
        logger.info("Sent error log to Kafka: %s", log_message)
        logger.error(f"Error preprocessing features: {e}")
        return None

def process_pcap_file(pcap_file):
    try:
        file_name = os.path.basename(pcap_file)
        log_message = {"message": f"Starting processing of {file_name}", "timestamp": time.time(), "file": file_name}
        producer.send('file_processing_logs', log_message)
        producer.flush()
        logger.info("Sent log to Kafka: %s", log_message)
        logger.info(f"Starting processing of {file_name}")

        packets = rdpcap(pcap_file)
        results = []
        
        for idx, pkt in enumerate(packets):
            if not pkt.haslayer(IP):
                log_message = {"message": f"Skipping packet {idx} without IP layer in {file_name}", "timestamp": time.time(), "file": file_name}
                producer.send('file_processing_logs', log_message)
                producer.flush()
                logger.info("Sent log to Kafka: %s", log_message)
                logger.debug(f"Skipping packet {idx} without IP layer")
                continue
                
            log = {
                "timestamp": float(pkt.time),
                "src_ip": pkt[IP].src,
                "dst_ip": pkt[IP].dst,
                "src_port": pkt[TCP].sport if pkt.haslayer(TCP) else pkt[UDP].sport if pkt.haslayer(UDP) else 0,
                "dst_port": pkt[TCP].dport if pkt.haslayer(TCP) else pkt[UDP].dport if pkt.haslayer(UDP) else 0,
                "protocol": 6 if pkt.haslayer(TCP) else 17 if pkt.haslayer(UDP) else 1 if pkt.haslayer(ICMP) else 0,
                "payload": base64.b64encode(pkt[IP].payload.original).decode('utf-8') if pkt[IP].payload else "",
                "file": file_name
            }
            
            log_message = {"message": f"Processing packet {idx} from {file_name}: {log}", "timestamp": time.time(), "file": file_name}
            producer.send('file_processing_logs', log_message)
            producer.flush()
            logger.info("Sent log to Kafka: %s", log_message)
            logger.info(f"Processing packet {idx} from {file_name}: {log}")
            
            raw_features = transform_log_to_features(log)
            if raw_features is None:
                log_message = {"message": f"Failed to transform features for packet {idx} in {file_name}, skipping", "timestamp": time.time(), "file": file_name}
                producer.send('file_processing_logs', log_message)
                producer.flush()
                logger.info("Sent log to Kafka: %s", log_message)
                logger.error(f"Failed to transform features for packet {idx} in {file_name}, skipping")
                continue
            
            processed_features = preprocess_features(raw_features)
            if processed_features is None:
                log_message = {"message": f"Failed to preprocess features for packet {idx} in {file_name}, skipping", "timestamp": time.time(), "file": file_name}
                producer.send('file_processing_logs', log_message)
                producer.flush()
                logger.info("Sent log to Kafka: %s", log_message)
                logger.error(f"Failed to preprocess features for packet {idx} in {file_name}, skipping")
                continue
            
            rf_proba = rf_model.predict_proba(processed_features)[:, 1][0]
            dt_proba = dt_model.predict_proba(processed_features)[:, 1][0]
            avg_proba = (rf_proba + dt_proba) / 2
            attack_status = "ATTACK DETECTED" if avg_proba > 0.5 else "NORMAL"
            
            result = {
                "timestamp": log["timestamp"],
                "attack_status": attack_status,
                "rf_prediction": 1 if rf_proba > 0.5 else 0,
                "dt_prediction": 1 if dt_proba > 0.5 else 0,
                "src_ip": log["src_ip"],
                "dst_ip": log["dst_ip"],
                "src_port": log["src_port"],
                "dst_port": log["dst_port"],
                "protocol": log["protocol"],
                "probability": avg_proba,
                "file": file_name
            }
            
            log_message = {"message": f"Processed packet {idx} result from {file_name}: {result}", "timestamp": time.time(), "file": file_name}
            producer.send('file_processing_logs', log_message)
            producer.flush()
            logger.info("Sent log to Kafka: %s", log_message)
            logger.info(f"Processed packet {idx} result from {file_name}: {result}")
            
            ip_pair = (log["src_ip"], log["dst_ip"])
            current_time = time.time()
            if result["attack_status"] == "NORMAL":
                if current_time - last_normal_alert[ip_pair] < 1:
                    continue
                last_normal_alert[ip_pair] = current_time
            
            results.append(result)
            producer.send("file_alerts", result)
            producer.flush()
            logger.info(f"Sent alert to Kafka topic 'file_alerts': {result}")
        
        log_message = {"message": f"Completed processing of {file_name} with {len(results)} results", "timestamp": time.time(), "file": file_name}
        producer.send('file_processing_logs', log_message)
        producer.flush()
        logger.info("Sent log to Kafka: %s", log_message)
        logger.info(f"Completed processing of {file_name} with {len(results)} results")
        
        # Удаляем файл после обработки
        try:
            os.remove(pcap_file)
            logger.info(f"Deleted file after processing: {pcap_file}")
        except Exception as e:
            logger.error(f"Failed to delete file {pcap_file}: {e}")
        
        return results
    except Exception as e:
        file_name = os.path.basename(pcap_file)
        log_message = {"message": f"Error processing PCAP file {file_name}: {str(e)}", "timestamp": time.time(), "file": file_name}
        producer.send('file_processing_logs', log_message)
        producer.flush()
        logger.info("Sent error log to Kafka: %s", log_message)
        logger.error(f"Error processing PCAP file {file_name}: {e}")
        return []

def main(producer):
    logger.info("File processor started")
    log_message = {"message": "File processor started", "timestamp": time.time(), "file": None}
    producer.send('file_processing_logs', log_message)
    producer.flush()
    logger.info("Sent log to Kafka: %s", log_message)
    upload_dir = "/app/uploads"
    try:
        os.makedirs(upload_dir, exist_ok=True)
        os.chown(upload_dir, 1000, 1000)
        os.chmod(upload_dir, 0o777)  # Устанавливаем более широкие права для теста
        logger.info(f"Ensured upload directory exists with permissions: {upload_dir}")
    except Exception as e:
        logger.error(f"Failed to create or set permissions on upload directory {upload_dir}: {e}")
        raise
    processed_files = set()  # Отслеживаем обработанные файлы
    while True:
        try:
            # Проверяем права доступа к директории
            logger.debug(f"Checking directory {upload_dir} permissions: {oct(os.stat(upload_dir).st_mode)[-3:]}")
            files = os.listdir(upload_dir)
            logger.info(f"Found files in {upload_dir}: {files}")
            for filename in files:
                file_path = os.path.join(upload_dir, filename)
                # Проверяем, является ли это файлом и не обрабатывали ли его ранее
                if os.path.isfile(file_path) and filename.endswith((".pcap", ".pcapng")) and filename not in processed_files:
                    logger.info(f"Found new file: {file_path}")
                    try:
                        # Проверяем права доступа к файлу
                        if not os.access(file_path, os.R_OK):
                            logger.error(f"No read access to {file_path}")
                            continue
                        if not os.access(file_path, os.W_OK):
                            logger.error(f"No write access to {file_path}")
                            continue
                        os.chown(file_path, 1000, 1000)
                        os.chmod(file_path, 0o666)  # Устанавливаем права на чтение/запись
                        logger.info(f"Set permissions for {file_path}: {oct(os.stat(file_path).st_mode)[-3:]}")
                    except Exception as e:
                        logger.error(f"Cannot set permissions for {file_path}: {e}")
                        continue
                    log_message = {"message": f"Processing uploaded file: {filename}", "timestamp": time.time(), "file": filename}
                    producer.send('file_processing_logs', log_message)
                    producer.flush()
                    logger.info("Sent log to Kafka: %s", log_message)
                    logger.info(f"Processing uploaded file: {filename}")
                    results = process_pcap_file(file_path)
                    for result in results:
                        producer.send("file_alerts", result)
                        producer.flush()
                        logger.info(f"Sent alert to Kafka topic 'file_alerts': {result}")
                    processed_files.add(filename)
                    logger.info(f"File {file_path} marked as processed")
                else:
                    logger.debug(f"Skipping {file_path}: already processed or not a PCAP file")
            time.sleep(1)
        except Exception as e:
            log_message = {"message": f"Error in main loop: {str(e)}", "timestamp": time.time(), "file": None}
            producer.send('file_processing_logs', log_message)
            producer.flush()
            logger.info("Sent error log to Kafka: %s", log_message)
            logger.error(f"Error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main(producer)