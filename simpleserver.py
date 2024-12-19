import socket
import random
import time
import threading
import struct
import re

ring_members = []
last_heartbeat = {}  # Speichert den letzten Heartbeat-Zeitstempel für jeden Nachbarn
client_list = []
leader = None
listener_thread = None
CLIENT_BROADCAST_PORT = 5974

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

def active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT):
    """
    Der Server läuft im aktiven Modus, beantwortet Broadcasts und verarbeitet direkte Nachrichten.
    """
    BUFFER_SIZE = 1024
    global ring_members, client_list, leader, CLIENT_BROADCAST_PORT  # Greife auf die globale Variable zu

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

    leader = f"{MY_IP}:{COMMUNICATION_PORT}"  # Setze diesen Server als Leader
    print(f"Ich bin der aktive Server (Leader): {leader}")

    new_ring_members_message = f"RING_MEMBERS: {ring_members}"
    client_list_message = f"CLIENT_LIST: {client_list}"
    new_leader_message = f"NEW_LEADER: {leader}"
    time.sleep(1)  # Warte 1 Sekunde vor dem ersten Broadcast
    broadcast_socket.sendto(new_ring_members_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(client_list_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    
    broadcast_socket.sendto(new_leader_message.encode(), (get_broadcast_address(), BROADCAST_PORT))

    start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT)

    while True:
        # Broadcast-Nachrichten empfangen
        try:
            broadcast_socket.settimeout(0.5)
            data, address = broadcast_socket.recvfrom(BUFFER_SIZE)
            if "DISCOVER_SERVER" in data.decode().strip():
                ring_members.append(f"{address[0]}:{data.decode().split(' ')[1]}")
                response_message = f"SERVER_RESPONSE:{MY_IP}:{COMMUNICATION_PORT}, {ring_members}"
                broadcast_socket.sendto(response_message.encode(), address)
                print(f"Broadcast-Antwort gesendet an {address}: {response_message}")
                
                
                new_ring_members_message = f"RING_MEMBERS: {ring_members}"
                client_list_message = f"CLIENT_LIST: {client_list}"
                new_leader_message = f"NEW_LEADER: {leader}"
                time.sleep(1)  # Warte 1 Sekunde vor dem ersten Broadcast
                broadcast_socket.sendto(new_ring_members_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                broadcast_socket.sendto(client_list_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                
                broadcast_socket.sendto(new_leader_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
            elif "DISCOVER_BY_CLIENT" in data.decode().strip():
                response_message = f"SERVER_RESPONSE:{MY_IP}:{COMMUNICATION_PORT}, {ring_members}, {BROADCAST_PORT}"
                broadcast_socket.sendto(response_message.encode(), address)
                print(f"Broadcast-Antwort gesendet an {address}: {response_message}")

                client_list.append(address)
                client_list_message = f"CLIENT_LIST: {client_list}"
                broadcast_socket.sendto(client_list_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
        
        except socket.timeout:
            pass

        # Direkte Nachrichten empfangen
        try:
            server_socket.settimeout(0.5)
            data, address = server_socket.recvfrom(BUFFER_SIZE)
            print(f"Direkte Nachricht von {address}: {data.decode()}")
            response_message = "Hello, Client!"
            server_socket.sendto(response_message.encode(), address)

            broadcast_socket.sendto(data, (get_broadcast_address(), CLIENT_BROADCAST_PORT))

            
        except socket.timeout:
            pass

def passive_mode(BROADCAST_PORT, MY_IP, COMMUNICATION_PORT, LISTENER_PORT):
    """
    Der Server läuft im passiven Modus und lauscht nur auf Broadcast-Nachrichten.
    """
    BUFFER_SIZE = 1024
    global ring_members, leader  # Greife auf die globale Variable zu

    # Broadcast-Socket erstellen
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_socket.bind(('', BROADCAST_PORT))
    print(f"Passiver Server hört auf Broadcast-Nachrichten auf Port {BROADCAST_PORT}")

    # Kommunikations-Socket erstellen (für direkte Nachrichten)
    communication_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    communication_socket.bind(('', COMMUNICATION_PORT))
    print(f"Passiver Server lauscht auf direkte Nachrichten auf Port {COMMUNICATION_PORT}")

    start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT)

    while True:
        try:
            data, address = broadcast_socket.recvfrom(BUFFER_SIZE)
            print(f"Broadcast-Nachricht empfangen: {data.decode()} von {address}")

            broadcast_msg = data.decode()

            if "RING_MEMBERS" in broadcast_msg:

                match = re.search(r"\[.*\]", broadcast_msg)
                if match:
                    array_string = match.group()  # Enthält den Inhalt der eckigen Klammern als String
                    # Konvertiere den String in eine Liste
                    ring_members_array = eval(array_string)
                    ring_members = ring_members_array

            elif "NEW_LEADER" in broadcast_msg:
                # Antwort vom aktiven Server
                leader = f"{address[0]}:{data.decode().split(' ')[1].split(':')[1]}"
                print(f"Aktiver Server (Leader) ist: {leader}")

        except socket.timeout:
            pass  # Keine Broadcast-Nachricht empfangen

        # Direkte Nachrichten empfangen (z. B. Heartbeats)
        try:
            communication_socket.settimeout(0.5)
            data, address = communication_socket.recvfrom(BUFFER_SIZE)
            message = data.decode()

            if "HEARTBEAT_FROM" in message:
                print(f"Heartbeat empfangen von {message.split(':')[1]}:{message.split(':')[2]}")

                # Optional: Antworte auf den Heartbeat
                response_message = f"HEARTBEAT_ACK:{MY_IP}:{COMMUNICATION_PORT}"
                communication_socket.sendto(response_message.encode(), address)
                print(f"Heartbeat-Antwort gesendet an {address}")
        except socket.timeout:
            pass  # Keine Direkt-Nachricht empfangen

def get_local_ip():
    """Ermittelt die lokale IP-Adresse des Geräts."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        # Verbindung zu einem externen Server herstellen (keine tatsächlichen Daten werden gesendet)
        s.connect(("8.8.8.8", 80))  # Google DNS als Ziel
        return s.getsockname()[0]
    
def get_neighbour(ring, current_node_ip, direction):

    global ring_members

    current_node_index = ring_members.index(current_node_ip) if current_node_ip in ring_members else -1
    if current_node_index != -1:
        if direction == 'left':
            if current_node_index + 1 == len(ring_members):
                return ring_members[0]
            else:
                return ring_members[current_node_index + 1]
        else:
            if current_node_index == 0:
                return ring_members[len(ring) - 1]
            else:
                return ring_members[current_node_index - 1]
    else:
        return None

def start_server():
    """
    Startet den Server. Entscheidet zwischen aktivem und passivem Modus.
    """
    BROADCAST_PORT = 5973
    
    COMMUNICATION_PORT = random.randint(10000, 11000)
    LISTENER_PORT = COMMUNICATION_PORT + 1            # Listener-Port
    global ring_members  # Greife auf die globale Variable zu
    MY_IP = get_local_ip()

    print(f"Server startet auf {MY_IP}... und Port {COMMUNICATION_PORT}")

    # Startet die Überwachung der Heartbeats
    start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT)

    # Nach bestehendem Server suchen
    existing_server = discover_existing_server(BROADCAST_PORT, COMMUNICATION_PORT)
    if existing_server:
        print(f"Bestehender Server gefunden bei {existing_server}. Wechsel in passiven Modus.")
        ring_members.append(f"{MY_IP}:{COMMUNICATION_PORT}")
        passive_mode(BROADCAST_PORT, MY_IP, COMMUNICATION_PORT, LISTENER_PORT)  # In passiven Modus wechseln
    else:
        print("Kein Server gefunden. Wechsel in aktiven Modus.")
        ring_members.append(f"{MY_IP}:{COMMUNICATION_PORT}")
        active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT)

def send_heartbeat(MY_IP, COMMUNICATION_PORT):
    """
    Sendet alle 5 Sekunden einen Heartbeat an den Nachbarn.
    """
    BUFFER_SIZE = 1024
    global ring_members  # Greife auf die globale Variable zu

    # Socket für Heartbeat-Kommunikation erstellen
    heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    while True:
        # Ermittle den Nachbarn
        neighbour = get_neighbour(ring_members, f"{MY_IP}:{COMMUNICATION_PORT}", direction='left')
        print('NACHBAR', neighbour, 'KNOTEN', f"{MY_IP}:{COMMUNICATION_PORT}", 'RING MEMBER', ring_members)
        if neighbour:
            neighbour_ip, neighbour_port = neighbour.split(':')
            neighbour_port = int(neighbour_port) + 1

            # Sende Heartbeat
            try:
                heartbeat_message = f"HEARTBEAT_FROM:{MY_IP}:{COMMUNICATION_PORT}"
                heartbeat_socket.sendto(heartbeat_message.encode(), (neighbour_ip, neighbour_port))
                print(f"Heartbeat gesendet an {neighbour_ip}:{neighbour_port}")
            except Exception as e:
                print(f"Fehler beim Senden des Heartbeats: {e}")
        
        # Warte 5 Sekunden
        time.sleep(5)

def listen_for_heartbeat(LISTENER_PORT):
    """
    Lauscht auf Heartbeat-Nachrichten von anderen Servern.
    """
    BUFFER_SIZE = 1024
    global last_heartbeat

    # Socket für Heartbeat-Kommunikation erstellen
    listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Port-Wiederverwendung erlauben
    listener_socket.bind(('', LISTENER_PORT))

    while True:
        try:
            data, address = listener_socket.recvfrom(BUFFER_SIZE)
            message = data.decode()
            if "HEARTBEAT_FROM" in message:
                print(f"Heartbeat empfangen von {message.split(':')[1]}:{message.split(':')[2]}")

                sender_ip = message.split(':')[1]
                sender_port = message.split(':')[2]

                neighbour = f"{sender_ip}:{sender_port}"

                # Aktualisiere den Zeitstempel des Nachbarn
                last_heartbeat[neighbour] = time.time()
        except Exception as e:
            print(f"Fehler beim Empfangen des Heartbeats: {e}")

def start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT):
    """
    Startet die Heartbeat-Sender- und Listener-Threads.
    """
    global listener_thread

    # Heartbeat-Sender starten
    sender_thread = threading.Thread(target=send_heartbeat, args=(MY_IP, COMMUNICATION_PORT))
    sender_thread.daemon = True
    sender_thread.start()

    # Heartbeat-Listener nur starten, wenn er nicht bereits läuft
    if not listener_thread or not listener_thread.is_alive():
        listener_thread = threading.Thread(target=listen_for_heartbeat, args=(LISTENER_PORT,))
        listener_thread.daemon = True
        listener_thread.start()


def monitor_heartbeats(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT, timeout=10):
    """
    Überwacht Heartbeats des aktuellen Nachbarn und entfernt ihn bei Timeout.
    """
    global ring_members, last_heartbeat, leader

    while True:
        current_time = time.time()

        # Ermittle den aktuellen Nachbarn
        current_neighbour = get_neighbour(ring_members, f"{MY_IP}:{COMMUNICATION_PORT}", direction='right')

        # Überprüfe nur den aktuellen Nachbarn
        if current_neighbour in last_heartbeat:
            if current_time - last_heartbeat[current_neighbour] > timeout:
                print(f"Nachbar {current_neighbour} hat Timeout überschritten. Entferne aus Ring.")
                ring_members.remove(current_neighbour)
                del last_heartbeat[current_neighbour]

                # Sende die aktualisierte Ringliste per Broadcast
                updated_ring_message = f"RING_MEMBERS: {ring_members}"
                send_broadcast(updated_ring_message, BROADCAST_PORT)
                print(f"Broadcast mit aktualisierter Ringliste gesendet: {ring_members}")

                if current_neighbour == leader:
                    active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT)

        time.sleep(2)  # Alle 5 Sekunden prüfen

def send_broadcast(message, BROADCAST_PORT):
    """
    Sendet eine Broadcast-Nachricht mit der aktuellen Serverliste.
    """
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    # Berechne die Broadcast-Adresse
    broadcast_address = get_broadcast_address()

    # Sende die Nachricht
    try:
        broadcast_socket.sendto(message.encode(), (broadcast_address, BROADCAST_PORT))
        print(f"Broadcast gesendet: {message}")
    except Exception as e:
        print(f"Fehler beim Senden des Broadcasts: {e}")
    finally:
        broadcast_socket.close()



def start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT):
    """
    Startet die Überwachung der Heartbeats.
    """
    monitor_thread = threading.Thread(target=monitor_heartbeats, args=(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT))
    monitor_thread.daemon = True
    monitor_thread.start()


if __name__ == '__main__':
    start_server()