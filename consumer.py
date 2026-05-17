import logging
import json
import base64
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import joblib
from scapy.all import rdpcap, IP, TCP, UDP, ICMP
from datetime import datetime
from collections import defaultdict
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("consumer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Consumer")

# Kafka settings
KAFKA_TOPIC = "network_logs"
ALERTS_TOPIC = "alerts"
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=['kafka:9093'],
    auto_offset_reset='earliest',
    group_id='consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers=['kafka:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load models and preprocessors
try:
    rf_model = joblib.load("models/rf_model_new.pkl")
    dt_model = joblib.load("models/dt_model_new.pkl")
    encoder = joblib.load("models/encoder_new.pkl")
    scaler = joblib.load("models/scaler_new.pkl")
    feature_order = joblib.load("models/feature_order_new.pkl")
    logger.info("Models and preprocessors loaded successfully")
    logger.info(f"Expected model features (feature_order): {feature_order}")
except Exception as e:
    logger.error(f"Failed to load models: {e}")
    raise

# Throttling state for NORMAL alerts
last_normal_alert = defaultdict(float)  # Track last time a NORMAL alert was sent per IP pair

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
        payload_bytes = base64.b64decode(log["payload"]) if isinstance(log["payload"], str) else log["payload"]
        
        # Identify connection key (src_ip, dst_ip, dst_port, protocol)
        conn_key = (log["src_ip"], log["dst_ip"], log["dst_port"], log["protocol"])
        stats = connection_stats[conn_key]
        
        # Update connection stats
        if stats["start_time"] is None:
            stats["start_time"] = log["timestamp"]
        stats["count"] += 1
        stats["dst_host_count"] += 1
        
        # Determine service
        service = "http" if log["dst_port"] == 80 else "https" if log["dst_port"] == 443 else \
                  "dns" if log["dst_port"] == 53 else "kafka" if log["dst_port"] == 9092 else "unknown"
        stats["services"].add(service)
        if service == stats["services"].__iter__().__next__():  # First service in set
            stats["srv_count"] += 1
        stats["dst_host_srv_count"] = len(stats["services"])
        
        # Update rates
        stats["same_srv_rate"] = stats["srv_count"] / stats["count"] if stats["count"] > 0 else 0.0
        stats["diff_srv_rate"] = 1.0 - stats["same_srv_rate"]
        
        # Update src ports for same_src_port_rate
        stats["src_ports"].add(log["src_port"])
        
        # Determine flag (simplified, can be improved with TCP flags)
        flag = "SF"  # Simplified; can add logic for SYN, FIN, RST, etc., using Scapy
        
        # Compute duration
        duration = log["timestamp"] - stats["start_time"] if stats["start_time"] else 0.0
        
        features = {
            "duration": duration,
            "protocol_type": "tcp" if log["protocol"] == 6 else "udp" if log["protocol"] == 17 else "icmp",
            "service": service,
            "flag": flag,
            "src_bytes": len(payload_bytes),
            "dst_bytes": log.get("dst_bytes", 0),
            "land": 1 if log["src_ip"] == log["dst_ip"] and log["src_port"] == log["dst_port"] else 0,
            "wrong_fragment": 0,  # Requires deeper packet inspection
            "urgent": 0,  # Requires TCP URG flag check
            "hot": 0,  # Requires application-layer analysis (e.g., login attempts)
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
            "serror_rate": stats["serror_rate"],  # Requires SYN error detection
            "srv_serror_rate": stats["serror_rate"],
            "rerror_rate": stats["rerror_rate"],  # Requires RST error detection
            "srv_rerror_rate": stats["rerror_rate"],
            "same_srv_rate": stats["same_srv_rate"],
            "diff_srv_rate": stats["diff_srv_rate"],
            "srv_diff_host_rate": 0.0,  # Requires tracking hosts per service
            "dst_host_count": stats["dst_host_count"],
            "dst_host_srv_count": stats["dst_host_srv_count"],
            "dst_host_same_srv_rate": stats["same_srv_rate"],
            "dst_host_diff_srv_rate": stats["diff_srv_rate"],
            "dst_host_same_src_port_rate": len(stats["src_ports"]) / stats["count"] if stats["count"] > 0 else 0.0,
            "dst_host_srv_diff_host_rate": 0.0,  # Requires more complex tracking
            "dst_host_serror_rate": stats["serror_rate"],
            "dst_host_srv_serror_rate": stats["serror_rate"],
            "dst_host_rerror_rate": stats["rerror_rate"],
            "dst_host_srv_rerror_rate": stats["rerror_rate"],
        }
        return pd.DataFrame([features])
    except Exception as e:
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
        
        logger.info(f"Processed feature count: {final_data.shape[1]}")
        if final_data.shape[1] != len(feature_order):
            raise ValueError(f"Expected {len(feature_order)} features, got {final_data.shape[1]}")
        return final_data
    except Exception as e:
        logger.error(f"Error preprocessing features: {e}")
        return None

def analyze_pcap_file(pcap_file):
    try:
        packets = rdpcap(pcap_file)
        results = []
        
        for pkt in packets:
            if not pkt.haslayer(IP):
                logger.debug("Skipping packet without IP layer")
                continue
                
            log = {
                "timestamp": float(pkt.time),
                "src_ip": pkt[IP].src,
                "dst_ip": pkt[IP].dst,
                "src_port": pkt[TCP].sport if pkt.haslayer(TCP) else pkt[UDP].sport if pkt.haslayer(UDP) else 0,
                "dst_port": pkt[TCP].dport if pkt.haslayer(TCP) else pkt[UDP].dport if pkt.haslayer(UDP) else 0,
                "protocol": 6 if pkt.haslayer(TCP) else 17 if pkt.haslayer(UDP) else 1 if pkt.haslayer(ICMP) else 0,
                "payload": base64.b64encode(pkt[IP].payload.original).decode('utf-8') if pkt[IP].payload else "",
            }
            
            logger.info(f"Processing packet from PCAP: {log}")
            
            raw_features = transform_log_to_features(log)
            if raw_features is None:
                logger.error("Failed to transform features for PCAP packet, skipping")
                continue
            
            processed_features = preprocess_features(raw_features)
            if processed_features is None:
                logger.error("Failed to preprocess features for PCAP packet, skipping")
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
                "probability": avg_proba
            }
            
            # Throttle NORMAL alerts: send only 1 per second per IP pair
            ip_pair = (log["src_ip"], log["dst_ip"])
            current_time = time.time()
            if result["attack_status"] == "NORMAL":
                if current_time - last_normal_alert[ip_pair] < 1:  # 1-second throttle
                    continue
                last_normal_alert[ip_pair] = current_time
            
            results.append(result)
            logger.info(f"PCAP packet result: {result}")
        
        return results
    except Exception as e:
        logger.error(f"Error analyzing PCAP file {pcap_file}: {e}")
        return []

def main():
    logger.info("Consumer started")
    for message in consumer:
        try:
            log = message.value
            logger.info(f"Received log: {log}")
            
            raw_features = transform_log_to_features(log)
            if raw_features is None:
                logger.error("Failed to transform features, skipping")
                continue
            
            processed_features = preprocess_features(raw_features)
            if processed_features is None:
                logger.error("Failed to preprocess features, skipping")
                continue
            
            rf_proba = rf_model.predict_proba(processed_features)[:, 1][0]
            dt_proba = dt_model.predict_proba(processed_features)[:, 1][0]
            avg_proba = (rf_proba + dt_proba) / 2
            attack_status = "ATTACK DETECTED" if avg_proba > 0.5 else "NORMAL"
            
            alert = {
                "timestamp": log["timestamp"],
                "attack_status": attack_status,
                "rf_prediction": 1 if rf_proba > 0.5 else 0,
                "dt_prediction": 1 if dt_proba > 0.5 else 0,
                "src_ip": log["src_ip"],
                "dst_ip": log["dst_ip"],
                "src_port": log["src_port"],
                "dst_port": log["dst_port"],
                "protocol": log["protocol"]
            }
            
            # Throttle NORMAL alerts: send only 1 per second per IP pair
            ip_pair = (log["src_ip"], log["dst_ip"])
            current_time = time.time()
            if alert["attack_status"] == "NORMAL":
                if current_time - last_normal_alert[ip_pair] < 1:  # 1-second throttle
                    continue
                last_normal_alert[ip_pair] = current_time
            
            producer.send(ALERTS_TOPIC, value=alert)
            producer.flush()
            logger.info(f"Sent alert: {alert}")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            continue

if __name__ == "__main__":
    main()