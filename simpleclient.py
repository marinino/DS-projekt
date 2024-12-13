import random
import re
import socket
import struct

def get_broadcast_address():
    """
    Ermittelt die Broadcast-Adresse basierend auf der lokalen IP-Adresse und Subnetzmaske.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        # Verbindung herstellen, um die lokale IP-Adresse zu ermitteln
        s.connect(("8.8.8.8", 80))  # Google DNS als Ziel
        local_ip = s.getsockname()[0]
    
    # Beispiel-Subnetzmaske (normalerweise automatisch verfügbar, hier hartcodiert)
    # Für echte Szenarien könntest du die Subnetzmaske auch dynamisch holen (z. B. mit netifaces)
    subnet_mask = "255.255.255.0"
    
    # Konvertiere IP und Maske in binäre Form
    ip_int = struct.unpack("!I", socket.inet_aton(local_ip))[0]
    mask_int = struct.unpack("!I", socket.inet_aton(subnet_mask))[0]
    
    # Berechne die Broadcast-Adresse
    broadcast_int = ip_int | ~mask_int
    broadcast_address = socket.inet_ntoa(struct.pack("!I", broadcast_int & 0xFFFFFFFF))
    return broadcast_address


def discover_existing_server(broadcast_port, communication_port, timeout=2):
    """
    Sendet einen Broadcast, um nach existierenden Servern im Netzwerk zu suchen.
    """
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.settimeout(timeout)

    broadcast_message = f"DISCOVER_BY_CLIENT {communication_port}"
    # Berechne die richtige Broadcast-Adresse
    broadcast_address = get_broadcast_address()
    broadcast_socket.sendto(broadcast_message.encode(), (broadcast_address, broadcast_port))
    print(f"Broadcast gesendet: {broadcast_message}")

    try:
        data, addr = broadcast_socket.recvfrom(1024)
        print(f"Antwort von bestehendem Server erhalten: {data.decode()} von {addr}")
        return data, addr  # Adresse des bestehenden Servers
    except socket.timeout:
        print("Keine Antwort von bestehenden Servern erhalten.")
        return None  # Kein Server gefunden


# Create a UDP socket
client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

BROADCAST_PORT = 5973
COMMUNICATION_PORT = random.randint(10000, 11000)

# Buffer size
buffer_size = 1024

# Timeout for receiving response
client_socket.settimeout(5)

print("Type 'exit' to close the client.")

try:
    try:
        data, addr = discover_existing_server(BROADCAST_PORT, COMMUNICATION_PORT)
    except socket.timeout:
        print("No response from server. Exiting client...")
        exit()

    # Interact with the discovered server
    while True:

        server_ip = addr[0]
        

        match = re.search(r"SERVER_RESPONSE:\d{1,3}(?:\.\d{1,3}){3}:(\d+)", data.decode())
        if match:
            server_communication_port  = match.group(1)
            print("Extrahierter Port:", server_communication_port )
        else:
            print("Port nicht gefunden")
        # Input message from user
        message = input('Please enter message: ')

        # Exit condition
        if message.lower() == 'exit':
            print("Exiting client...")
            break

        # Send data to server
        client_socket.sendto(message.encode(), (server_ip, int(server_communication_port)))
        print('Sent to server:', message)

        # Receive response from server
        print('Waiting for response...')
        try:
            data, server = client_socket.recvfrom(buffer_size)
            print('Received message from server:', data.decode())
        except socket.timeout:
            print("No response from server.")

finally:
    client_socket.close()
    print('Socket closed')

