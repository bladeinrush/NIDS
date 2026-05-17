def detect_intrusion(packet):
    # Example rules for intrusion detection
    if packet.get('protocol') == 6 and 'SYN' in packet.get('payload', ''):  # TCP SYN
        return "Possible SYN flood detected"
    if 'SELECT' in packet.get('payload', '').upper():
        return "Possible SQL injection detected"
    if packet.get('protocol') == 17 and 'malicious' in packet.get('payload', '').lower():  # UDP
        return "Possible malicious UDP packet"
    return None