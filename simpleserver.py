import socket
import random
import time
import threading

def discover_existing_server(broadcast_port=5973, timeout=2):
    """
    Sendet einen Broadcast, um nach existierenden Servern im Netzwerk zu suchen.
    """
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.settimeout(timeout)

    broadcast_message = "DISCOVER_SERVER"
    broadcast_socket.sendto(broadcast_message.encode(), ('<broadcast>', broadcast_port))
    print(f"Broadcast gesendet: {broadcast_message}")

    try:
        data, addr = broadcast_socket.recvfrom(1024)
        print(f"Antwort von bestehendem Server erhalten: {data.decode()} von {addr}")
        return addr  # Adresse des bestehenden Servers
    except socket.timeout:
        print("Keine Antwort von bestehenden Servern erhalten.")
        return None  # Kein Server gefunden

def active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT):
    """
    Der Server läuft im aktiven Modus, beantwortet Broadcasts und verarbeitet direkte Nachrichten.
    """
    BUFFER_SIZE = 1024

    # Broadcast-Socket erstellen
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_socket.bind(('', BROADCAST_PORT))
    print(f"Aktiver Server hört auf Broadcast-Nachrichten auf Port {BROADCAST_PORT}")

    # Kommunikations-Socket erstellen
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((MY_IP, COMMUNICATION_PORT))
    print(f"Aktiver Server bereit für direkte Kommunikation auf {MY_IP}:{COMMUNICATION_PORT}")

    while True:
        # Broadcast-Nachrichten empfangen
        try:
            broadcast_socket.settimeout(0.5)
            data, address = broadcast_socket.recvfrom(BUFFER_SIZE)
            if data.decode().strip() == "DISCOVER_SERVER":
                response_message = f"SERVER_RESPONSE:{MY_IP}:{COMMUNICATION_PORT}"
                broadcast_socket.sendto(response_message.encode(), address)
                print(f"Broadcast-Antwort gesendet an {address}: {response_message}")
        except socket.timeout:
            pass

        # Direkte Nachrichten empfangen
        try:
            server_socket.settimeout(0.5)
            data, address = server_socket.recvfrom(BUFFER_SIZE)
            print(f"Direkte Nachricht von {address}: {data.decode()}")
            response_message = "Hello, Client!"
            server_socket.sendto(response_message.encode(), address)
        except socket.timeout:
            pass

def passive_mode(BROADCAST_PORT):
    """
    Der Server läuft im passiven Modus und lauscht nur auf Broadcast-Nachrichten.
    """
    BUFFER_SIZE = 1024

    # Broadcast-Socket erstellen
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_socket.bind(('', BROADCAST_PORT))
    print(f"Passiver Server hört auf Broadcast-Nachrichten auf Port {BROADCAST_PORT}")

    while True:
        try:
            data, address = broadcast_socket.recvfrom(BUFFER_SIZE)
            print(f"Broadcast-Nachricht empfangen: {data.decode()} von {address}")
        except socket.timeout:
            pass  # Keine Broadcast-Nachricht empfangen

def start_server():
    """
    Startet den Server. Entscheidet zwischen aktivem und passivem Modus.
    """
    BROADCAST_PORT = 5973
    COMMUNICATION_PORT = random.randint(10000, 11000)

    # Lokale IP-Adresse
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))
        MY_IP = s.getsockname()[0]

    print(f"Server startet auf {MY_IP}...")

    # Nach bestehendem Server suchen
    existing_server = discover_existing_server(BROADCAST_PORT)
    if existing_server:
        print(f"Bestehender Server gefunden bei {existing_server}. Wechsel in passiven Modus.")
        passive_mode(BROADCAST_PORT)  # In passiven Modus wechseln
    else:
        print("Kein Server gefunden. Wechsel in aktiven Modus.")
        active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT)

if __name__ == '__main__':
    start_server()
