import socket
import random
import time
import threading
import struct
import re

ring_members = []

def get_broadcast_address():
    """
    Ermittelt die Broadcast-Adresse basierend auf der lokalen IP-Adresse und Subnetzmaske.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        # Verbindung herstellen, um die lokale IP-Adresse zu ermitteln
        s.connect(("8.8.8.8", 80))  # Google DNS als Ziel
        local_ip = s.getsockname()[0]
    
    # Beispiel-Subnetzmaske (normalerweise automatisch verfügbar, hier hartcodiert)
    # Für echte Szenarien könntest du die Subnetzmaske auch dynamisch holen (z. B. mit `netifaces`)
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

    broadcast_message = f"DISCOVER_SERVER {communication_port}"
    # Berechne die richtige Broadcast-Adresse
    broadcast_address = get_broadcast_address()
    broadcast_socket.sendto(broadcast_message.encode(), (broadcast_address, broadcast_port))
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
            if "DISCOVER_SERVER" in data.decode().strip():
                response_message = f"SERVER_RESPONSE:{MY_IP}:{COMMUNICATION_PORT}, {ring_members}"
                broadcast_socket.sendto(response_message.encode(), address)
                print(f"Broadcast-Antwort gesendet an {address}: {response_message}")
                print('Neighbour', get_neighbour(ring_members, (f"('{MY_IP}', {COMMUNICATION_PORT})")))
                
                print('GOT BROADCAST MESSAGE', data.decode())
                print('PORT', data.decode().split(' ')[1])
                
                print(address[0])
                print('MEMBER FOR RING', f"{address[0]}:{data.decode().split(' ')[1]}")

                ring_members.append(f"{address[0]}:{data.decode().split(' ')[1]}")
                new_ring_members_message = f"RING_MEMBERS: {ring_members}"
                broadcast_socket.sendto(new_ring_members_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                print(f"New members broadcast")

                print('MASTER NEIGHBOUR', get_neighbour(ring_members, f'{MY_IP}:{COMMUNICATION_PORT}'))
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

def passive_mode(BROADCAST_PORT, MY_IP, COMMUNICATION_PORT):
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
            ring_members = data.decode()
            print("RING MEMBER MESSAGE", ring_members)

            match = re.search(r"\[.*\]", ring_members)
            if match:
                array_string = match.group()  # Enthält den Inhalt der eckigen Klammern als String
                # Konvertiere den String in eine Liste
                ring_members_array = eval(array_string)
                print(f"Extrahiertes Array: {ring_members_array}")

            print("CURRENT NEIGHBOUR", get_neighbour(ring_members_array, (f"{MY_IP}:{COMMUNICATION_PORT}")))
        except socket.timeout:
            pass  # Keine Broadcast-Nachricht empfangen

def get_local_ip():
    """Ermittelt die lokale IP-Adresse des Geräts."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        # Verbindung zu einem externen Server herstellen (keine tatsächlichen Daten werden gesendet)
        s.connect(("8.8.8.8", 80))  # Google DNS als Ziel
        return s.getsockname()[0]
    
def get_neighbour(ring, current_node_ip, direction='left'):
    current_node_index = ring.index(current_node_ip) if current_node_ip in ring else -1
    if current_node_index != -1:
        if direction == 'left':
            if current_node_index + 1 == len(ring):
                return ring[0]
            else:
                return ring[current_node_index + 1]
        else:
            if current_node_index == 0:
                return ring[len(ring) - 1]
            else:
                return ring[current_node_index - 1]
    else:
        return None

def start_server():
    """
    Startet den Server. Entscheidet zwischen aktivem und passivem Modus.
    """
    BROADCAST_PORT = 5973
    COMMUNICATION_PORT = random.randint(10000, 11000)
    
   
    MY_IP = get_local_ip()

    print(f"Server startet auf {MY_IP}... und Port {COMMUNICATION_PORT}")

    # Nach bestehendem Server suchen
    existing_server = discover_existing_server(BROADCAST_PORT, COMMUNICATION_PORT)
    if existing_server:
        print(f"Bestehender Server gefunden bei {existing_server}. Wechsel in passiven Modus.")
        passive_mode(BROADCAST_PORT, MY_IP, COMMUNICATION_PORT)  # In passiven Modus wechseln
    else:
        print("Kein Server gefunden. Wechsel in aktiven Modus.")
        ring_members.append(f"{MY_IP}:{COMMUNICATION_PORT}")
        active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT)



if __name__ == '__main__':
    start_server()
