from scapy.all import get_if_list, get_if_addr, get_if_hwaddr

for iface in get_if_list():
    try:
        print(f"Interface: {iface}")
        print(f"IP Address: {get_if_addr(iface)}")
        print(f"MAC Address: {get_if_hwaddr(iface)}")
        print("-" * 50)
    except Exception as e:
        print(f"Error for {iface}: {e}")
        print("-" * 50)