from scapy.all import rdpcap
import base64
import pandas as pd

def extract_features(pcap_file, label):
    packets = rdpcap(pcap_file)
    data = []
    for pkt in packets:
        if not pkt.haslayer(IP):
            continue
        protocol = 6 if pkt.haslayer(TCP) else 17 if pkt.haslayer(UDP) else 1 if pkt.haslayer(ICMP) else 0
        payload = base64.b64encode(pkt[IP].payload.original).decode('utf-8') if pkt[IP].payload else ""
        features = {
            "src_port": pkt[TCP].sport if pkt.haslayer(TCP) else pkt[UDP].sport if pkt.haslayer(UDP) else 0,
            "dst_port": pkt[TCP].dport if pkt.haslayer(TCP) else pkt[UDP].dport if pkt.haslayer(UDP) else 0,
            "protocol": protocol,
            "payload_length": len(payload),
            "is_tcp": 1 if protocol == 6 else 0,
            "is_udp": 1 if protocol == 17 else 0,
            "is_icmp": 1 if protocol == 1 else 0,
            "label": label,
        }
        data.append(features)
    return pd.DataFrame(data)

# Extract features from normal and malicious PCAPs
normal_df = extract_features("normal.pcap", 0)
malicious_df = extract_features("malicious.pcap", 1)
df = pd.concat([normal_df, malicious_df], ignore_index=True)
df.to_csv("dataset.csv", index=False)