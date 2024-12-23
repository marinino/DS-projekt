import random
import re
import socket
import struct
import threading

server_ip = ""
server_communication_port = -1

def get_local_ip():
    """
    Ermittelt die lokale IP-Adresse des Geräts.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        # Verbindung zu einer externen Adresse simulieren
        s.connect(("8.8.8.8", 80))  # Google-DNS-Server als Ziel
        return s.getsockname()[0]

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


def listen_for_broadcast(client_broadcast_port):
    """
    Lauscht auf Broadcast-Nachrichten vom Netzwerk.
    """

    global server_ip, server_communication_port

    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_socket.bind(("", client_broadcast_port))  # Verwende einen anderen Port

    print(f"Listening for broadcast messages on port {client_broadcast_port}...")

    while True:
        try:
            data, addr = broadcast_socket.recvfrom(1024)

            if "sender" in data.decode():
                print(data.decode().split("sender: ")[1].strip().replace('"',''))
                print(f"('{get_local_ip()}', {COMMUNICATION_PORT})")

                if data.decode().split("sender: ")[1].strip().replace('"','') == (f"('{get_local_ip()}', {COMMUNICATION_PORT})"):
                    
                    continue

                print(f"Broadcast-Nachricht von {addr}: {data.decode()}")
            elif "NEW_LEADER" in data.decode():
                print(f"Broadcast-Nachricht von {addr}: {data.decode()}")

                server_ip = addr[0]
        

                match = re.search(r"NEW_LEADER: \d{1,3}(?:\.\d{1,3}){3}:(\d+)", data.decode())
                if match:
                    server_communication_port  = match.group(1)
                    print("Extrahierter Port:", server_communication_port )
                else:
                    print("Port nicht gefunden")

            
        except Exception as e:
            print(f"Fehler beim Empfangen der Broadcast-Nachricht: {e}")


# Client setup
client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
BROADCAST_PORT = 5973  # Vom Server gesendet
CLIENT_BROADCAST_PORT = 5974  # Client lauscht auf diesem Port
COMMUNICATION_PORT = random.randint(10000, 11000)

# Binde den Client-Socket explizit an den COMMUNICATION_PORT
client_socket.bind(("", COMMUNICATION_PORT))
buffer_size = 1024
client_socket.settimeout(5)

# Start Broadcast-Listener in einem separaten Thread
listener_thread = threading.Thread(target=listen_for_broadcast, args=(CLIENT_BROADCAST_PORT,))
listener_thread.daemon = True
listener_thread.start()

print(f"Type 'exit' to close the client. {COMMUNICATION_PORT}")

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