import ast
import json
import socket
import random
import time
import threading
import struct
import re
import uuid

ring_members = []
last_heartbeat = {}  # Speichert den letzten Heartbeat-Zeitstempel für jeden Nachbarn
client_list = []
groups = {}
leader = None
listener_thread = None
broadcast_thread = None
monitor_thread = None
sender_thread = None
CLIENT_BROADCAST_PORT = 5974
self_uuid = None
participant = False  # Ob der Knoten ein Teilnehmer ist
broadcast_socket = None
communication_socket = None
server_socket = None
global_sequence_numbers = {'public': 0}  # Globale Sequenznummer
listener_socket = None
stalled_from_election = False

uuid_mapping = {}  # Format: {UUID: (IP, PORT)}

def generate_uuid():
    return str(uuid.uuid4())

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
    #print('BROTKASTENADRESSE', broadcast_address)
    return broadcast_address



def discover_existing_server(broadcast_port, communication_port, MY_IP, LISTENER_PORT, timeout=5):
    """
    Sendet einen Broadcast, um nach existierenden Servern im Netzwerk zu suchen und wartet maximal `timeout` Sekunden auf eine Antwort.
    """

    global stalled_from_election, self_uuid, uuid_mapping, ring_members, leader

    discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)  # Erlaube Broadcasts
    discovery_socket.settimeout(0.5)  # Kurzer Timeout für Schleifen-Iteration

    broadcast_message = f"DISCOVER_SERVER;{communication_port}"
    broadcast_address = get_broadcast_address()
    print(broadcast_port)
    discovery_socket.sendto(broadcast_message.encode(), (broadcast_address, broadcast_port))

    start_time = time.time()  # Startzeit speichern

    while time.time() - start_time < timeout:  # Wartezeit begrenzen
        try:
            data, addr = discovery_socket.recvfrom(1024)

            if int(data.decode().split(';')[-1]) != int(communication_port):
                if data.decode().startswith("ANSWER_FROM_PASSIVE"):
                    print('Passiver Server gefunden')
                    stalled_from_election = True

                    start_time2 = time.time()
                    while time.time() - start_time2 < 30:
                        if leader is not None:  # Falls ein Leader in der Zwischenzeit gefunden wurde, abbrechen
                            print("Leader wurde gesetzt, Wechsel in passive Rolle.")
                            #stalled_from_election = False
                            break
                        time.sleep(0.5)  # Warte 0.5 Sekunden, um CPU-Last zu vermeiden

                    if leader is None:  # Falls nach 10 Sekunden immer noch kein Leader existiert, aktiver Server werden
                        print("Kein Leader nach 10 Sekunden gefunden. Wechsel in aktiven Modus.", leader)
                        active_mode_conversion(MY_IP, broadcast_port, communication_port, LISTENER_PORT)
                        #stalled_from_election = False
                else:

                    self_uuid = data.decode().split(";")[1]
                    uuid_mapping = ast.literal_eval(data.decode().split(";")[2])
                    ring_members = eval(data.decode().split(";")[3])

                print(f"Antwort von bestehendem Server erhalten: {data.decode()} von {addr}")
                
                return addr  # Server gefunden, also Adresse zurückgeben
        except socket.timeout:
            pass  # Falls kein Paket ankommt, einfach weiter versuchen

    print("Keine Antwort von bestehenden Servern erhalten nach 3 Sekunden.")
    return None  # Kein Server gefunden nach Ablauf des Timeouts

def active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT):
    """
    Der Server läuft im aktiven Modus, beantwortet Broadcasts und verarbeitet direkte Nachrichten.
    """
    BUFFER_SIZE = 1024
    global ring_members, client_list, leader, CLIENT_BROADCAST_PORT, uuid_mapping, server_socket, broadcast_socket, global_sequence_numbers  # Greife auf die globale Variable zu

    print(f"Aktiver Server hört auf Broadcast-Nachrichten auf Port {BROADCAST_PORT}")

    # Kommunikations-Socket erstellen
    
    print(f"Aktiver Server bereit für direkte Kommunikation auf {MY_IP}:{COMMUNICATION_PORT}")

    leader = f"{MY_IP}:{COMMUNICATION_PORT}"  # Setze diesen Server als Leader
    print(f"Ich bin der aktive Server (Leader): {leader}")

    new_ring_members_message = f"RING_MEMBERS;{ring_members};{COMMUNICATION_PORT}"
    client_list_message = f"CLIENT_LIST;{client_list};{COMMUNICATION_PORT}"
    new_leader_message = f"NEW_LEADER;{leader};{COMMUNICATION_PORT}"
    uuid_mapping_message = f"NEW_UUID_MAPPING;{uuid_mapping};{COMMUNICATION_PORT}"
    time.sleep(1)  # Warte 1 Sekunde vor dem ersten Broadcast
    broadcast_socket.sendto(new_ring_members_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(client_list_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(uuid_mapping_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(new_leader_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(new_leader_message.encode(), (get_broadcast_address(), 5974))

    start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
    start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT)
            

def passive_mode(BROADCAST_PORT, MY_IP, COMMUNICATION_PORT, LISTENER_PORT):
    """
    Der Server läuft im passiven Modus und lauscht nur auf Broadcast-Nachrichten.
    """
    BUFFER_SIZE = 1024
    global ring_members, leader, client_list, self_uuid, uuid_mapping, groups, participant, broadcast_socket, communication_socket, global_sequence_numbers  # Greife auf die globale Variable zu

   
    print(f"Passiver Server hört auf Broadcast-Nachrichten auf Port {BROADCAST_PORT}")
    start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
    start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT)
    start_listen_for_broadcast_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
    start_listen_for_direct_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)

def get_local_ip():
    """Ermittelt die lokale IP-Adresse des Geräts."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        # Verbindung zu einem externen Server herstellen (keine tatsächlichen Daten werden gesendet)
        s.connect(("8.8.8.8", 80))  # Google DNS als Ziel
        #print("IT HURTS WHEN IP", s.getsockname()[0])

        return s.getsockname()[0]
    
def get_neighbour(ring, member_uuid, direction):

    global ring_members, uuid_mapping

    #print(member_uuid)

    current_node_index = ring_members.index(member_uuid) if member_uuid in ring_members else -1
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
    global ring_members, uuid_mapping, self_uuid, server_socket  # Greife auf die globale Variable zu
    MY_IP = get_local_ip()

    print(f"Server startet auf {MY_IP}... und Port {COMMUNICATION_PORT}")

    # Client-Monitor starten
    monitor_clients_thread = threading.Thread(target=monitor_clients, args=(BROADCAST_PORT,COMMUNICATION_PORT))
    monitor_clients_thread.daemon = True
    monitor_clients_thread.start()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((MY_IP, COMMUNICATION_PORT))
    start_listen_for_direct_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
    start_listen_for_broadcast_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)

    # Nach bestehendem Server suchen
    existing_server = discover_existing_server(BROADCAST_PORT, COMMUNICATION_PORT, MY_IP, LISTENER_PORT)
    
    if existing_server:
        print(f"Bestehender Server gefunden bei {existing_server}. Wechsel in passiven Modus.") 
        passive_mode(BROADCAST_PORT, MY_IP, COMMUNICATION_PORT, LISTENER_PORT)  # In passiven Modus wechseln
    else:
        print("Kein Server gefunden. Wechsel in aktiven Modus.")
        new_uuid = generate_uuid()
        self_uuid = new_uuid
        uuid_mapping[new_uuid] = (MY_IP, COMMUNICATION_PORT)
        ring_members.append(new_uuid)
        active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT)

    while True:
        continue

def send_heartbeat(MY_IP, COMMUNICATION_PORT):
    """
    Sendet alle 5 Sekunden einen Heartbeat an den Nachbarn.
    """
    BUFFER_SIZE = 1024
    global ring_members, self_uuid, uuid_mapping  # Greife auf die globale Variable zu

    # Socket für Heartbeat-Kommunikation erstellen
    heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    while True:
        # Ermittle den Nachbarn
        neighbour_uuid = get_neighbour(ring_members, self_uuid, direction='left')
        #print('NACHBAR', neighbour_uuid, 'KNOTEN', self_uuid, 'RING MEMBER', ring_members)
        if neighbour_uuid:
            neighbour_adress = uuid_mapping[neighbour_uuid]
            neighbour_ip = neighbour_adress[0]
            neighbour_port = neighbour_adress[1]
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

def listen_for_direct_messages(LISTENER_PORT, COMMUNICATION_PORT, MY_IP, BROADCAST_PORT):
    """
    Lauscht auf Heartbeat-Nachrichten von anderen Servern.
    """
    BUFFER_SIZE = 1024
    global last_heartbeat, participant, listener_socket

    # Socket für Heartbeat-Kommunikation erstellen
    listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Port-Wiederverwendung erlauben
    listener_socket.bind((MY_IP, LISTENER_PORT))

    while True:
        try:
            data, address = listener_socket.recvfrom(BUFFER_SIZE)
            message = data.decode()
            print(f"Direkte Nachricht von {address}: {data.decode()}")
            #print("DIREKTNACHRICHT IST DA")

            try:
                msg_content = json.loads(message)

                if "GROUP_REG" in msg_content['content']:
                    print("Received group registration")

                    groupname = msg_content['content'].split(":")[1]
                    clientdata = msg_content['content'].split(":")[2]

                    if groupname not in groups:
                        print("Neue Gruppe erstellt")
                        groups[groupname] = []
                        global_sequence_numbers[groupname] = 0

                        response_message = f"GROUP_CREATED;{groupname}"
                        server_socket.sendto(response_message.encode(), address)

                    if clientdata not in groups[groupname]:
                        groups[groupname].append(address)

                        response_message = f"GROUP_MEMBER_ADDED;{global_sequence_numbers[groupname]};{groupname}"
                        #print('CORRECT ADDR', address)
                        server_socket.sendto(response_message.encode(), address)
                    
                    #print(groups)
                    

                else:
                    sender_groups = []
                    message_to_forward = json.loads(data.decode())

                    for group_name, members in groups.items():
                        #print(members)
                        #print(address[0], address[1])
                        if (address[0], address[1]) in members:
                            sender_groups.append(group_name)
                    
                    #print('SENDER GROUPS', sender_groups)
                    server_socket.sendto(f'ACK;{message_to_forward["message_id"]}'.encode(), address)

                    if len(sender_groups) > 0:
                        for group in sender_groups:
                            #print("GROUP", groups[group])
                            global_sequence_numbers[group] += 1
                            message_to_forward['group'] = group
                            message_to_forward["seq_no"] = global_sequence_numbers[group]
                            for member in groups[group]:
                                #print("MEMBER", member)
                                time.sleep(1)
                                if member != address:
                                    server_socket.sendto(json.dumps(message_to_forward).encode(), member)
                                    #print('MSG FORWAREDED')
                    else:
                        global_sequence_numbers["public"] += 1
                        #print(message_to_forward)

                        message_to_forward["seq_no"] = global_sequence_numbers["public"]
                        message_to_forward["group"] = "public"
                        message_to_forward["sender"] = address
                        print(f"Leader: Sequenznummer {global_sequence_numbers['public']} zugewiesen für Nachricht {message_to_forward}")
                        if f"{MY_IP}:{COMMUNICATION_PORT}" == leader:
                            new_global_seq_nums_msg = f"SEQ_NUMBERS;{global_sequence_numbers};{COMMUNICATION_PORT}"
                            broadcast_socket.sendto(new_global_seq_nums_msg.encode(), (get_broadcast_address(), int(BROADCAST_PORT)))

                            response_message = f"ACK;{message_to_forward['message_id']}"
                            server_socket.sendto(response_message.encode(), address)

                            broadcast_back = json.dumps(message_to_forward)
                            broadcast_back_json = broadcast_back.encode()
                            broadcast_socket.sendto(broadcast_back_json, (get_broadcast_address(), CLIENT_BROADCAST_PORT))
                
            except json.JSONDecodeError:
                if message.startswith("HEARTBEAT"):
                    process_heartbeat(message, address)
                elif message.startswith("E("):
                    process_election(message, address, BROADCAST_PORT, LISTENER_PORT, COMMUNICATION_PORT, MY_IP)
            

            
        except socket.timeout:
            time.sleep(1)

        except Exception as e:
            print(f"Fehler beim Empfangen des Heartbeats: {e}")
            time.sleep(1)

   

def listen_for_broadcast_messages(LISTENER_PORT, COMMUNICATION_PORT, MY_IP, BROADCAST_PORT):
    """
    Lauscht auf Heartbeat-Nachrichten von anderen Servern.
    """
    BUFFER_SIZE = 1024
    global stalled_from_election, self_uuid, last_heartbeat, participant, broadcast_socket, uuid_mapping, ring_members, client_list, leader, groups, global_sequence_numbers, server_socket

    # Broadcast-Socket erstellen
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_socket.bind(('', BROADCAST_PORT))

    while True:
        try:
            broadcast_socket.settimeout(0.5)
            data, address = broadcast_socket.recvfrom(BUFFER_SIZE)

            #print('LEADER', leader)
            print('NEW BORADCAST', data.decode())

            communication_port_sender = data.decode().split(';')[-1]
            if int(communication_port_sender) != int(COMMUNICATION_PORT):
                #print(communication_port_sender, data.decode())
                if data.decode().startswith("DISCOVER_SERVER") and f"{MY_IP}:{COMMUNICATION_PORT}" == leader  and not stalled_from_election:
                    new_uuid = generate_uuid()
                    uuid_mapping[new_uuid] = (address[0], int(data.decode().split(';')[1]))
                    ring_members.append(new_uuid)
                    response_message = f"SERVER_RESPONSE;{new_uuid};{uuid_mapping};{ring_members};{COMMUNICATION_PORT}"
                    server_socket.sendto(response_message.encode(), address)
                    print(f"Broadcast-Antwort gesendet an {address}: {response_message}")
                    
                    
                    new_ring_members_message = f"RING_MEMBERS;{ring_members};{COMMUNICATION_PORT}"
                    client_list_message = f"CLIENT_LIST;{client_list};{COMMUNICATION_PORT}"
                    new_leader_message = f"NEW_LEADER;{leader};{COMMUNICATION_PORT}"
                    uuid_mapping_message = f"NEW_UUID_MAPPING;{uuid_mapping};{COMMUNICATION_PORT}"
                    groups_message = f"GROUPS;{groups};{COMMUNICATION_PORT}"
                    groups_seq_numbers = f"SEQ_NUMBERS;{global_sequence_numbers};{COMMUNICATION_PORT}"
                    time.sleep(1)  # Warte 1 Sekunde vor dem ersten Broadcast
                    broadcast_socket.sendto(groups_seq_numbers.encode(), (get_broadcast_address(), BROADCAST_PORT))
                    broadcast_socket.sendto(new_ring_members_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                    broadcast_socket.sendto(client_list_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                    broadcast_socket.sendto(uuid_mapping_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                    broadcast_socket.sendto(groups_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                    broadcast_socket.sendto(new_leader_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                elif data.decode().startswith("DISCOVER_BY_CLIENT") and f"{MY_IP}:{COMMUNICATION_PORT}" == leader and not stalled_from_election:
                    new_uuid = generate_uuid()
                    uuid_mapping[new_uuid] = (address[0], int(data.decode().split(";")[1]))
                    client_list.append(new_uuid)
                    response_message = f"DISCOVER_SERVER_RESPONSE;{COMMUNICATION_PORT};{global_sequence_numbers['public']}"
                    server_socket.sendto(response_message.encode(), (address[0], int(data.decode().split(";")[1])))
                    print(f"Broadcast-Antwort gesendet an {address}: {response_message}")

                    client_list_message = f"CLIENT_LIST;{client_list};{COMMUNICATION_PORT}"
                    broadcast_socket.sendto(client_list_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                    new_uuid_message = f"NEW_UUID_MAPPING;{uuid_mapping};{COMMUNICATION_PORT}"
                    broadcast_socket.sendto(new_uuid_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                elif data.decode().startswith("RING_MEMBERS") and not stalled_from_election:

                    
                    ring_members_array = eval(data.decode().split(';')[1])
                    ring_members = ring_members_array
                    #print("RING MEMEBERS REC", ring_members)

                elif data.decode().startswith("NEW_LEADER"):
                    # Antwort vom aktiven Server
                    leader = f"{address[0]}:{data.decode().split(';')[1].split(':')[1]}"
                    print(f"Aktiver Server (Leader) ist: {leader}")

                    if stalled_from_election:
                        time.sleep(1)
                        # Nach bestehendem Server suchen
                        # Nach bestehendem Server suchen

                        

                        existing_server = discover_existing_server(BROADCAST_PORT, COMMUNICATION_PORT, MY_IP, LISTENER_PORT)
                        
                        if existing_server:
                            print(f"Bestehender Server gefunden bei {existing_server}. Wechsel in passiven Modus.") 
                            passive_mode(BROADCAST_PORT, MY_IP, COMMUNICATION_PORT, LISTENER_PORT)  # In passiven Modus wechseln
                        else:
                            print("Kein Server gefunden. Wechsel in aktiven Modus.")
                            new_uuid = generate_uuid()
                            self_uuid = new_uuid
                            uuid_mapping[new_uuid] = (MY_IP, COMMUNICATION_PORT)
                            ring_members.append(new_uuid)
                            active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT)

                        stalled_from_election = False
                        print('SET STALLED FROM ELECTION', stalled_from_election)
                        start_listen_for_direct_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
                        print('case 1')
                        #start_listen_for_broadcast_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
                        print('case 2')
                        start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT)
                        print('case 3')
                        while True:
                            continue

                elif data.decode().startswith("CLIENT_LIST") and not stalled_from_election:
                    match = re.search(r"\[.*\]", data.decode())
                    if match:
                        array_string = match.group()  # Enthält den Inhalt der eckigen Klammern als String
                        # Konvertiere den String in eine Liste
                        ring_members_array = eval(array_string)
                        client_list = ring_members_array
                
                elif data.decode().startswith("NEW_UUID_MAPPING") and not stalled_from_election:
                    #print('GOT NEW UUID MAPPING')
                    uuid_mapping = ast.literal_eval(data.decode().split(";")[1])

                elif data.decode().startswith("GROUPS") and not stalled_from_election:
                    groups = ast.literal_eval(data.decode().split(";")[1])
                    print('FROM GROUPS')
                    start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
                    start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT)
                elif data.decode().startswith("SEQ_NUMBERS;") and not stalled_from_election: 
                    #print('GOT SEQUENCE NUMBER ARRAY')

                    global_sequence_numbers = ast.literal_eval(data.decode().split(';')[1])
                elif data.decode().startswith("DISCOVER_SERVER") and not stalled_from_election:
                    def delayed_send():
                        passive_server_response = f"ANSWER_FROM_PASSIVE;{COMMUNICATION_PORT}"
                        broadcast_socket.sendto(passive_server_response.encode(), address)
                        print('ONBHIBUVBUZBUBINIHBGUVUBOJNBUZHVUGB')

                    # Starte einen Timer, der nach 2 Sekunden die Nachricht sendet
                    timer = threading.Timer(2, delayed_send)
                    timer.start()
                else:
                    print("GOT DIFFERENT MESSAGE", data.decode())

                    
        except socket.timeout:
            time.sleep(1)

def start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT):
    """
    Startet die Heartbeat-Sender- und Listener-Threads.
    """
    global listener_thread, sender_thread

    # Heartbeat-Sender starten
    if not sender_thread or not sender_thread.is_alive():
        sender_thread = threading.Thread(target=send_heartbeat, args=(MY_IP, COMMUNICATION_PORT))
        sender_thread.daemon = True
        sender_thread.start()

def start_listen_for_direct_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT):

    global listener_thread

    # Heartbeat-Listener nur starten, wenn er nicht bereits läuft
    if not listener_thread or not listener_thread.is_alive():
        listener_thread = threading.Thread(target=listen_for_direct_messages, args=(LISTENER_PORT,COMMUNICATION_PORT, MY_IP, BROADCAST_PORT))
        listener_thread.daemon = True
        listener_thread.start()

def start_listen_for_broadcast_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT):

    global broadcast_thread

    # Heartbeat-Listener nur starten, wenn er nicht bereits läuft
    #if not broadcast_thread or not broadcast_thread.is_alive():
    print('STARTED BROADCAST LISTENER')
    broadcast_thread = threading.Thread(target=listen_for_broadcast_messages, args=(LISTENER_PORT,COMMUNICATION_PORT, MY_IP, BROADCAST_PORT))
    broadcast_thread.daemon = True
    broadcast_thread.start()


def monitor_heartbeats(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT, timeout=20):
    """
    Überwacht Heartbeats des aktuellen Nachbarn und entfernt ihn bei Timeout.
    """
    global ring_members, last_heartbeat, leader, self_uuid, uuid_mapping

    not_found_counter = 0

    while True:
        current_time = time.time()
        #print("HIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")
        # Ermittle den aktuellen Nachbarn
        current_neighbour_uuid = get_neighbour(ring_members, self_uuid, direction='right')
        ###print('NACHBAR', current_neighbour_uuid, 'KNOTEN', self_uuid, 'RING MEMBER', ring_members)
        #print(current_neighbour_uuid)
        #print(str(uuid_mapping))
        current_neighbour = uuid_mapping[current_neighbour_uuid]
        #print(current_neighbour)
        # Überprüfe nur den aktuellen Nachbarn
        #print("CURRENNT_NEI", current_neighbour)
        #print(last_heartbeat)
        if current_neighbour in last_heartbeat:
            if current_time - last_heartbeat[current_neighbour] > timeout:
                print(f"Nachbar {current_neighbour} hat Timeout überschritten. Entferne aus Ring.")
                ring_members.remove(current_neighbour_uuid)
                del last_heartbeat[current_neighbour]

                # Sende die aktualisierte Ringliste per Broadcast
                updated_ring_message = f"RING_MEMBERS;{ring_members};{COMMUNICATION_PORT}"
                send_broadcast(updated_ring_message, BROADCAST_PORT)
                print(f"Broadcast mit aktualisierter Ringliste gesendet: {ring_members}")

                #print(current_neighbour, leader)
                current_neighbour_compare_leader = f"{current_neighbour[0]}:{current_neighbour[1]}"

                if current_neighbour_compare_leader == leader:
                    start_election(COMMUNICATION_PORT)

        else:
            not_found_counter = not_found_counter + 1
            if not_found_counter > 5:
                print(f"Nachbar {current_neighbour} hat Timeout überschritten. Entferne aus Ring.")
                ring_members.remove(current_neighbour_uuid)

                # Sende die aktualisierte Ringliste per Broadcast
                updated_ring_message = f"RING_MEMBERS;{ring_members};{COMMUNICATION_PORT}"
                send_broadcast(updated_ring_message, BROADCAST_PORT)
                print(f"Broadcast mit aktualisierter Ringliste gesendet: {ring_members}")

                #print(current_neighbour, leader)
                current_neighbour_compare_leader = f"{current_neighbour[0]}:{current_neighbour[1]}"

                if current_neighbour_compare_leader == leader:
                    start_election(COMMUNICATION_PORT)
                not_found_counter = 0


        time.sleep(2)  # Alle 5 Sekunden prüfen

def send_broadcast(message, BROADCAST_PORT):
    """
    Sendet eine Broadcast-Nachricht mit der aktuellen Serverliste.
    """
    global broadcast_socket

    # Berechne die Broadcast-Adresse
    broadcast_address = get_broadcast_address()

    # Sende die Nachricht
    try:
        broadcast_socket.sendto(message.encode(), (broadcast_address, BROADCAST_PORT))
        print(f"Broadcast gesendet: {message}")

       
    except Exception as e:
        print(f"Fehler beim Senden des Broadcasts: {e}")



def start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT):
    """
    Startet die Überwachung der Heartbeats.
    """
    global monitor_thread

    if not monitor_thread or not monitor_thread.is_alive():
        monitor_thread = threading.Thread(target=monitor_heartbeats, args=(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT))
        monitor_thread.daemon = True
        monitor_thread.start()

    

def monitor_clients(BROADCAST_PORT, COMMUNICATION_PORT):
    """
    Überwacht die Clients in `client_list`, indem alle 30 Sekunden ein Ping gesendet wird.
    Entfernt Clients, die nicht antworten.
    """
    global client_list, groups, uuid_mapping

    BUFFER_SIZE = 1024
    ping_message = "PING"

    while True:
        inactive_clients = []
        for client_uuid in client_list:
            try:
                client_address = uuid_mapping[client_uuid]
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                client_socket.settimeout(5)  # Warte maximal 5 Sekunden auf eine Antwort
                #print(client_address)
                
                # Sende den Ping
                client_socket.sendto(ping_message.encode(), (client_address[0], client_address[1]))
                print(f"Ping gesendet an {client_uuid} ({client_address})")

                # Auf Antwort warten
                data, address = client_socket.recvfrom(BUFFER_SIZE)
                if data.decode() == "PONG":
                    print(f"Antwort von {client_uuid} ({client_address}): PONG")
                else:
                    raise Exception("Unerwartete Antwort")
            
            except Exception as e:
                # Wenn keine Antwort oder ein Fehler auftritt, markiere den Client als inaktiv
                print(f"Client {client_uuid} ({client_address}) hat nicht geantwortet. Fehler: {e}")
                inactive_clients.append(client_uuid)
            
            finally:
                client_socket.close()

        # Entferne inaktive Clients
        if inactive_clients:
            for client_uuid in inactive_clients:
                client_list.remove(client_uuid)
                print(f"Client {client_uuid} wurde aus der Liste entfernt.")

            # Entferne aus den Gruppen
            for group_name, members in groups.items():
                #print(uuid_mapping[client_uuid])
                #print(members)
                if uuid_mapping[client_uuid] in members:
                    members.remove(uuid_mapping[client_uuid])
                    print(f"Client {client_uuid} wurde aus der Gruppe {group_name} entfernt.")

            # Sende die aktualisierte Client-Liste per Broadcast
            updated_client_list_message = f"CLIENT_LIST;{client_list};{COMMUNICATION_PORT}"
            updated_groups_message = f"GROUPS;{groups};{COMMUNICATION_PORT}"
            time.sleep(1)
            send_broadcast(updated_client_list_message, BROADCAST_PORT)
            send_broadcast(updated_groups_message, BROADCAST_PORT)
            print(f"Broadcast mit aktualisierter Client-Liste gesendet: {client_list}")

            

        # Warte 30 Sekunden vor der nächsten Überprüfung
        time.sleep(30)

# Nachricht senden
def send_message(message, neighbor):

    global uuid_mapping, communication_socket, server_socket

    neighbor_data = uuid_mapping[neighbor]
    #print(neighbor_data[0], neighbor_data[1], message)


    try:
        server_socket.sendto(message.encode(), (neighbor_data[0], neighbor_data[1] + 1))

    except Exception as e:
        print(f"Fehler beim Senden der Nachricht: {e}")

# Wahl starten
def start_election(port):
    global participant
    print(f"Wahl wird gestartet von {port}")
    participant = True
    send_message(f"E({port}, False)", get_neighbour(ring_members, self_uuid, "left"))

def process_heartbeat(message, address):

    global last_heartbeat
    print(f"Heartbeat empfangen von {message.split(':')[1]}:{message.split(':')[2]}")

    sender_ip = message.split(':')[1]
    sender_port = message.split(':')[2]

    neighbour = (sender_ip, int(sender_port))

    # Aktualisiere den Zeitstempel des Nachbarn
    last_heartbeat[neighbour] = time.time()

def process_election(message, address, BROADCAST_PORT, LISTENER_PORT, COMMUNICATION_PORT, MY_IP):
    global participant, leader

    #print('HIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII')
    mid, flag = eval(message[1:])  # Parse der Nachricht
    #print(mid, flag)
    if flag == False:  # Wahl-Nachricht
        if mid < COMMUNICATION_PORT and not participant:
            #print('CASE 1')
            time.sleep(0.6)
            send_message(f"E({COMMUNICATION_PORT}, False)", get_neighbour(ring_members, self_uuid, "left"))
            participant = True
        elif mid > COMMUNICATION_PORT:
            #print('CASE 2')
            time.sleep(0.6)
            send_message(f"E({mid}, False)", get_neighbour(ring_members, self_uuid, "left"))
            participant = True
        elif mid == COMMUNICATION_PORT:
            #print('CASE 3')
            time.sleep(0.6)
            send_message(f"E({COMMUNICATION_PORT}, True)", get_neighbour(ring_members, self_uuid, "left"))
            participant = False
            active_mode_conversion(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT)
            
    elif flag == True:  # Leader-Ankündigung
        leader = f"{get_local_ip()}:{mid}"
        print(f"Leader ist: {leader}")
        if mid != COMMUNICATION_PORT:
            time.sleep(0.6)
            send_message(f"E({mid}, True)", get_neighbour(ring_members, self_uuid, "left"))
        participant = False

def active_mode_conversion(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT):
    """
    Der Server läuft im aktiven Modus, beantwortet Broadcasts und verarbeitet direkte Nachrichten.
    """
    BUFFER_SIZE = 1024
    global ring_members, client_list, leader, CLIENT_BROADCAST_PORT, uuid_mapping, server_socket, broadcast_socket  # Greife auf die globale Variable zu

    leader = f"{MY_IP}:{COMMUNICATION_PORT}"  # Setze diesen Server als Leader
    print(f"Ich bin der aktive Server (Leader): {leader}")

    new_ring_members_message = f"RING_MEMBERS;{ring_members};{COMMUNICATION_PORT}"
    client_list_message = f"CLIENT_LIST;{client_list};{COMMUNICATION_PORT}"
    new_leader_message = f"NEW_LEADER;{leader};{COMMUNICATION_PORT}"
    uuid_mapping_message = f"NEW_UUID_MAPPING;{uuid_mapping};{COMMUNICATION_PORT}"
    groups_message = f"GROUPS;{groups};{COMMUNICATION_PORT}"
    groups_seq_numbers = f"SEQ_NUMBERS;{global_sequence_numbers};{COMMUNICATION_PORT}"
    time.sleep(1)  # Warte 1 Sekunde vor dem ersten Broadcast
    broadcast_socket.sendto(groups_seq_numbers.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(new_ring_members_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(client_list_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(uuid_mapping_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(groups_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(new_leader_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(new_leader_message.encode(), (get_broadcast_address(), CLIENT_BROADCAST_PORT))
    

    


if __name__ == '__main__':
    start_server()