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
last_heartbeat = {}  # Saves timestamp
client_list = []
groups = {}
leader = None
listener_thread = None
broadcast_thread = None
monitor_thread = None
sender_thread = None
CLIENT_BROADCAST_PORT = 5974
self_uuid = None
participant = False  # Marks election participant
broadcast_socket = None
communication_socket = None
server_socket = None
global_sequence_numbers = {'public': 0}  # Global Sequence numbers
listener_socket = None
stalled_from_election = False

uuid_mapping = {}  # Format: {UUID: (IP, PORT)}

def generate_uuid():
    return str(uuid.uuid4())

def get_broadcast_address():
    """
    Returns broadcast IP of network
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
    

    subnet_mask = "255.255.255.0"
    
   
    ip_int = struct.unpack("!I", socket.inet_aton(local_ip))[0]
    mask_int = struct.unpack("!I", socket.inet_aton(subnet_mask))[0]
    
    broadcast_int = ip_int | ~mask_int
    broadcast_address = socket.inet_ntoa(struct.pack("!I", broadcast_int & 0xFFFFFFFF))

    return broadcast_address



def discover_existing_server(broadcast_port, communication_port, MY_IP, LISTENER_PORT, timeout=5):
    """
    Sends broadcast to see if there are servers in the network and waits on answers
    """

    global stalled_from_election, self_uuid, uuid_mapping, ring_members, leader

    discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1) 
    discovery_socket.settimeout(0.5) 

    broadcast_message = f"DISCOVER_SERVER;{communication_port}"
    broadcast_address = get_broadcast_address()
    discovery_socket.sendto(broadcast_message.encode(), (broadcast_address, broadcast_port))

    start_time = time.time()  # Safe request time

    while time.time() - start_time < timeout:  # Cap waiting time
        try:
            data, addr = discovery_socket.recvfrom(1024)

            if int(data.decode().split(';')[-1]) != int(communication_port):
                if data.decode().startswith("ANSWER_FROM_PASSIVE"):
                    print('Passive Server found')
                    stalled_from_election = True

                    start_time2 = time.time()
                    while time.time() - start_time2 < 30:
                        if leader is not None:  # Stop waiting if leader is announced
                            print("Leader was found, go to passive mode")
                            break
                        time.sleep(0.5)

                    if leader is None:  # Become master if no leader announces itself
                        print("No leader found after 10 seconds change to active mode", leader)
                        active_mode_conversion(MY_IP, broadcast_port, communication_port, LISTENER_PORT)
                else:

                    self_uuid = data.decode().split(";")[1]
                    uuid_mapping = ast.literal_eval(data.decode().split(";")[2])
                    ring_members = eval(data.decode().split(";")[3])

                print(f"Got answer by existing server: {data.decode()} von {addr}")
                
                return addr  # Return adress of answering server
        except socket.timeout:
            pass 

    print("No answer within 3 seconds received.")
    return None  # No server was found

def active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT):
    """
    Server running in active mode
    """
    BUFFER_SIZE = 1024
    global ring_members, client_list, leader, CLIENT_BROADCAST_PORT, uuid_mapping, server_socket, broadcast_socket, global_sequence_numbers  # Greife auf die globale Variable zu
    
    print(f"Active server ready for direct communication at {MY_IP}:{COMMUNICATION_PORT}")

    leader = f"{MY_IP}:{COMMUNICATION_PORT}"  # Setting himself as leader
    print(f"I am the active server (Leader): {leader}")

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
    Server runs in passive mode
    """
    BUFFER_SIZE = 1024
    global ring_members, leader, client_list, self_uuid, uuid_mapping, groups, participant, broadcast_socket, communication_socket, global_sequence_numbers  # Greife auf die globale Variable zu

   
    print(f"Passive server listens to broadcasts on port {BROADCAST_PORT}")
    start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
    start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT)
    start_listen_for_broadcast_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
    start_listen_for_direct_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)

def get_local_ip():
    """Get IP of own device"""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        
        s.connect(("8.8.8.8", 80))  # Google DNS as target
        return s.getsockname()[0]
    
def get_neighbour(ring, member_uuid, direction):
    """Get ring neighbours for servers"""
    global ring_members, uuid_mapping


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
    Server startup method
    """

    global ring_members, uuid_mapping, self_uuid, server_socket  

    BROADCAST_PORT = 5973
    COMMUNICATION_PORT = random.randint(10000, 11000)
    LISTENER_PORT = COMMUNICATION_PORT + 1            # Listener-Port
    MY_IP = get_local_ip()

    print(f"Server starts with {MY_IP}... and Port {COMMUNICATION_PORT}")

    # Starting Client-Monitor
    monitor_clients_thread = threading.Thread(target=monitor_clients, args=(BROADCAST_PORT,COMMUNICATION_PORT))
    monitor_clients_thread.daemon = True
    monitor_clients_thread.start()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((MY_IP, COMMUNICATION_PORT))
    start_listen_for_direct_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
    start_listen_for_broadcast_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)

    # Search for existing servers
    existing_server = discover_existing_server(BROADCAST_PORT, COMMUNICATION_PORT, MY_IP, LISTENER_PORT)
    
    if existing_server:
        print(f"Exisitng server found at {existing_server}. Change in passiv mode.") 
        passive_mode(BROADCAST_PORT, MY_IP, COMMUNICATION_PORT, LISTENER_PORT)
    else:
        print("No server found. Going to active mode")
        new_uuid = generate_uuid()
        self_uuid = new_uuid
        uuid_mapping[new_uuid] = (MY_IP, COMMUNICATION_PORT)
        ring_members.append(new_uuid)
        active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT)

    while True:
        continue

def send_heartbeat(MY_IP, COMMUNICATION_PORT):
    """
    Sends heartbeat to neighbour every 5 seconds
    """
    BUFFER_SIZE = 1024
    global ring_members, self_uuid, uuid_mapping

    # Creating seperate heartbeat socket
    heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    while True:

        neighbour_uuid = get_neighbour(ring_members, self_uuid, direction='left')
        if neighbour_uuid:
            neighbour_adress = uuid_mapping[neighbour_uuid]
            neighbour_ip = neighbour_adress[0]
            neighbour_port = neighbour_adress[1]
            neighbour_port = int(neighbour_port) + 1

            # Sending Heartbeat
            try:
                heartbeat_message = f"HEARTBEAT_FROM:{MY_IP}:{COMMUNICATION_PORT}"
                heartbeat_socket.sendto(heartbeat_message.encode(), (neighbour_ip, neighbour_port))
                print(f"Heartbeat sent to {neighbour_ip}:{neighbour_port}")
            except Exception as e:
                print(f"Error sending heartbeat: {e}")
        
        # Wait 5 seconds
        time.sleep(5)

def listen_for_direct_messages(LISTENER_PORT, COMMUNICATION_PORT, MY_IP, BROADCAST_PORT):
    """
    Listens for direct messages to this server
    """
    BUFFER_SIZE = 1024
    global last_heartbeat, participant, listener_socket

    # Create socket for direct communication
    listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # 
    listener_socket.bind((MY_IP, LISTENER_PORT))

    while True:
        try:
            data, address = listener_socket.recvfrom(BUFFER_SIZE)
            message = data.decode()
            print(f"Direct message from {address}: {data.decode()}")

            try:
                msg_content = json.loads(message)

                if "GROUP_REG" in msg_content['content']:
                    print("Received group registration")

                    groupname = msg_content['content'].split(":")[1]
                    clientdata = msg_content['content'].split(":")[2]

                    if groupname not in groups:
                        print("New group created")
                        groups[groupname] = []
                        global_sequence_numbers[groupname] = 0

                        response_message = f"GROUP_CREATED;{groupname}"
                        server_socket.sendto(response_message.encode(), address)

                    if clientdata not in groups[groupname]:
                        groups[groupname].append(address)

                        response_message = f"GROUP_MEMBER_ADDED;{global_sequence_numbers[groupname]};{groupname}"
                        server_socket.sendto(response_message.encode(), address)
                    
                    

                else:
                    sender_groups = []
                    message_to_forward = json.loads(data.decode())

                    for group_name, members in groups.items():
                        
                        if (address[0], address[1]) in members:
                            sender_groups.append(group_name)
                    
                    server_socket.sendto(f'ACK;{message_to_forward["message_id"]}'.encode(), address)

                    if len(sender_groups) > 0:
                        for group in sender_groups:
                            global_sequence_numbers[group] += 1
                            message_to_forward['group'] = group
                            message_to_forward["seq_no"] = global_sequence_numbers[group]
                            for member in groups[group]:
                                time.sleep(1)
                                if member != address:
                                    server_socket.sendto(json.dumps(message_to_forward).encode(), member)
                    else:
                        global_sequence_numbers["public"] += 1

                        message_to_forward["seq_no"] = global_sequence_numbers["public"]
                        message_to_forward["group"] = "public"
                        message_to_forward["sender"] = address
                        print(f"Leader: Sequence number {global_sequence_numbers['public']} allocated for message {message_to_forward}")
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
            print(f"Error receving heartbeat: {e}")
            time.sleep(1)

   

def listen_for_broadcast_messages(LISTENER_PORT, COMMUNICATION_PORT, MY_IP, BROADCAST_PORT):
    """
    Listens for any broadcasts to this server
    """
    BUFFER_SIZE = 1024
    global stalled_from_election, self_uuid, last_heartbeat, participant, broadcast_socket, uuid_mapping, ring_members, client_list, leader, groups, global_sequence_numbers, server_socket

    # Create broadcast socket
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_socket.bind(('', BROADCAST_PORT))

    while True:
        try:
            broadcast_socket.settimeout(0.5)
            data, address = broadcast_socket.recvfrom(BUFFER_SIZE)


            communication_port_sender = data.decode().split(';')[-1]
            if int(communication_port_sender) != int(COMMUNICATION_PORT):
                if data.decode().startswith("DISCOVER_SERVER") and f"{MY_IP}:{COMMUNICATION_PORT}" == leader  and not stalled_from_election:
                    new_uuid = generate_uuid()
                    uuid_mapping[new_uuid] = (address[0], int(data.decode().split(';')[1]))
                    ring_members.append(new_uuid)
                    response_message = f"SERVER_RESPONSE;{new_uuid};{uuid_mapping};{ring_members};{COMMUNICATION_PORT}"
                    server_socket.sendto(response_message.encode(), address)
                    print(f"Broadcast answer send to {address}: {response_message}")
                    
                    
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
                    print(f"Broadcast answer send to {address}: {response_message}")

                    client_list_message = f"CLIENT_LIST;{client_list};{COMMUNICATION_PORT}"
                    broadcast_socket.sendto(client_list_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                    new_uuid_message = f"NEW_UUID_MAPPING;{uuid_mapping};{COMMUNICATION_PORT}"
                    broadcast_socket.sendto(new_uuid_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
                elif data.decode().startswith("RING_MEMBERS") and not stalled_from_election:

                    
                    ring_members_array = eval(data.decode().split(';')[1])
                    ring_members = ring_members_array

                elif data.decode().startswith("NEW_LEADER"):
                    leader = f"{address[0]}:{data.decode().split(';')[1].split(':')[1]}"
                    print(f"Active server (Leader) is: {leader}")

                    if stalled_from_election:
                        time.sleep(1)
            
                        

                        existing_server = discover_existing_server(BROADCAST_PORT, COMMUNICATION_PORT, MY_IP, LISTENER_PORT)
                        
                        if existing_server:
                            print(f"Existing server found {existing_server}. Going into passive mode.") 
                            passive_mode(BROADCAST_PORT, MY_IP, COMMUNICATION_PORT, LISTENER_PORT)  # In passiven Modus wechseln
                        else:
                            print("No server was found. Switch to active mode")
                            new_uuid = generate_uuid()
                            self_uuid = new_uuid
                            uuid_mapping[new_uuid] = (MY_IP, COMMUNICATION_PORT)
                            ring_members.append(new_uuid)
                            active_mode(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT)

                        stalled_from_election = False
                       
                        while True:
                            continue

                elif data.decode().startswith("CLIENT_LIST") and not stalled_from_election:
                    match = re.search(r"\[.*\]", data.decode())
                    if match:
                        array_string = match.group()  
                        ring_members_array = eval(array_string)
                        client_list = ring_members_array
                
                elif data.decode().startswith("NEW_UUID_MAPPING") and not stalled_from_election:
                    uuid_mapping = ast.literal_eval(data.decode().split(";")[1])

                elif data.decode().startswith("GROUPS") and not stalled_from_election:
                    groups = ast.literal_eval(data.decode().split(";")[1])
                    start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT)
                    start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT)
                elif data.decode().startswith("SEQ_NUMBERS;") and not stalled_from_election: 

                    global_sequence_numbers = ast.literal_eval(data.decode().split(';')[1])
                elif data.decode().startswith("DISCOVER_SERVER") and not stalled_from_election:
                    def delayed_send():
                        passive_server_response = f"ANSWER_FROM_PASSIVE;{COMMUNICATION_PORT}"
                        broadcast_socket.sendto(passive_server_response.encode(), address)

                    # Waits two seconds to answer, to ensure active answers first
                    timer = threading.Timer(2, delayed_send)
                    timer.start()

                    
        except socket.timeout:
            time.sleep(1)

def start_heartbeat(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT):
    """
    Start heartbeat sender thread
    """
    global listener_thread, sender_thread

    if not sender_thread or not sender_thread.is_alive():
        sender_thread = threading.Thread(target=send_heartbeat, args=(MY_IP, COMMUNICATION_PORT))
        sender_thread.daemon = True
        sender_thread.start()

def start_listen_for_direct_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT):
    """
    Start heartbeat listener
    """
    global listener_thread

    if not listener_thread or not listener_thread.is_alive():
        listener_thread = threading.Thread(target=listen_for_direct_messages, args=(LISTENER_PORT,COMMUNICATION_PORT, MY_IP, BROADCAST_PORT))
        listener_thread.daemon = True
        listener_thread.start()

def start_listen_for_broadcast_message(MY_IP, COMMUNICATION_PORT, LISTENER_PORT, BROADCAST_PORT):
    """
    Start listener for broadcast messages
    """
    global broadcast_thread

    broadcast_thread = threading.Thread(target=listen_for_broadcast_messages, args=(LISTENER_PORT,COMMUNICATION_PORT, MY_IP, BROADCAST_PORT))
    broadcast_thread.daemon = True
    broadcast_thread.start()


def monitor_heartbeats(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT, timeout=20):
    """
    Checks if neighbour sends heartbeat in time
    """
    global ring_members, last_heartbeat, leader, self_uuid, uuid_mapping

    not_found_counter = 0

    while True:
        current_time = time.time()
       
        current_neighbour_uuid = get_neighbour(ring_members, self_uuid, direction='right')
    
        current_neighbour = uuid_mapping[current_neighbour_uuid]
       
        if current_neighbour in last_heartbeat:
            if current_time - last_heartbeat[current_neighbour] > timeout:
                print(f"Neighbour {current_neighbour} exceeded time limit. Remove from ring.")
                ring_members.remove(current_neighbour_uuid)
                del last_heartbeat[current_neighbour]

                # Sends new ring members as broadcast
                updated_ring_message = f"RING_MEMBERS;{ring_members};{COMMUNICATION_PORT}"
                send_broadcast(updated_ring_message, BROADCAST_PORT)

                current_neighbour_compare_leader = f"{current_neighbour[0]}:{current_neighbour[1]}"

                if current_neighbour_compare_leader == leader:
                    start_election(COMMUNICATION_PORT)

        else:
            not_found_counter = not_found_counter + 1
            if not_found_counter > 5:
                print(f"Neighbour {current_neighbour} exceeded time limit. Remove from ring.")
                ring_members.remove(current_neighbour_uuid)

                # Sends new ring members as broadcast
                updated_ring_message = f"RING_MEMBERS;{ring_members};{COMMUNICATION_PORT}"
                send_broadcast(updated_ring_message, BROADCAST_PORT)

                current_neighbour_compare_leader = f"{current_neighbour[0]}:{current_neighbour[1]}"

                if current_neighbour_compare_leader == leader:
                    start_election(COMMUNICATION_PORT)
                not_found_counter = 0


        time.sleep(2)  # Check every 2 seconds

def send_broadcast(message, BROADCAST_PORT):
    """
    Used to send broadcasts
    """
    global broadcast_socket

    broadcast_address = get_broadcast_address()

    try:
        broadcast_socket.sendto(message.encode(), (broadcast_address, BROADCAST_PORT))
        print(f"Broadcast sent: {message}")

       
    except Exception as e:
        print(f"Error sending Broadcasts: {e}")



def start_heartbeat_monitor(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT):
    """
    Starts thread to monitor heartbeat
    """
    global monitor_thread

    if not monitor_thread or not monitor_thread.is_alive():
        monitor_thread = threading.Thread(target=monitor_heartbeats, args=(MY_IP, COMMUNICATION_PORT, BROADCAST_PORT, LISTENER_PORT))
        monitor_thread.daemon = True
        monitor_thread.start()

    

def monitor_clients(BROADCAST_PORT, COMMUNICATION_PORT):
    """
    Checks on clients once in a while if they are still up and running
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
                client_socket.settimeout(5) 
                
                # Send Ping
                client_socket.sendto(ping_message.encode(), (client_address[0], client_address[1]))
                print(f"Ping sent to {client_uuid} ({client_address})")

                # Wait on answer
                data, address = client_socket.recvfrom(BUFFER_SIZE)
                if data.decode() == "PONG":
                    print(f"Answer from {client_uuid} ({client_address}): PONG")
                else:
                    raise Exception("Unerwartete Antwort")
            
            except Exception as e:
                # Client did not answer
                print(f"Client {client_uuid} ({client_address}) did not answer. Error: {e}")
                inactive_clients.append(client_uuid)
            
            finally:
                client_socket.close()

        # Remove inactive clients
        if inactive_clients:
            for client_uuid in inactive_clients:
                client_list.remove(client_uuid)
                print(f"Client {client_uuid} was removed from client list.")

            for group_name, members in groups.items():

                if uuid_mapping[client_uuid] in members:
                    members.remove(uuid_mapping[client_uuid])
                    print(f"Client {client_uuid} was removed from group {group_name}.")

            updated_client_list_message = f"CLIENT_LIST;{client_list};{COMMUNICATION_PORT}"
            updated_groups_message = f"GROUPS;{groups};{COMMUNICATION_PORT}"
            time.sleep(1)
            send_broadcast(updated_client_list_message, BROADCAST_PORT)
            send_broadcast(updated_groups_message, BROADCAST_PORT)

            

        time.sleep(30) # Wait 30 seconds

# Nachricht senden
def send_message(message, neighbor):
    """
    Used to send direct messages
    """
    global uuid_mapping, communication_socket, server_socket

    neighbor_data = uuid_mapping[neighbor]
    try:
        server_socket.sendto(message.encode(), (neighbor_data[0], neighbor_data[1] + 1))

    except Exception as e:
        print(f"Error sending direct message: {e}")

def start_election(port):
    """
    Method called to start a leader election
    """
    global participant
    print(f"Election was started by {port}")
    participant = True
    send_message(f"E({port}, False)", get_neighbour(ring_members, self_uuid, "left"))

def process_heartbeat(message, address):
    """
    Helper method to process broadcasts
    """
    global last_heartbeat

    print(f"Heartbeat received from {message.split(':')[1]}:{message.split(':')[2]}")

    sender_ip = message.split(':')[1]
    sender_port = message.split(':')[2]

    neighbour = (sender_ip, int(sender_port))

    last_heartbeat[neighbour] = time.time()

def process_election(message, address, BROADCAST_PORT, LISTENER_PORT, COMMUNICATION_PORT, MY_IP):
    global participant, leader

    mid, flag = eval(message[1:])  # Parse message
    if flag == False:  
        if mid < COMMUNICATION_PORT and not participant:
            time.sleep(0.6)
            send_message(f"E({COMMUNICATION_PORT}, False)", get_neighbour(ring_members, self_uuid, "left"))
            participant = True
        elif mid > COMMUNICATION_PORT:
            time.sleep(0.6)
            send_message(f"E({mid}, False)", get_neighbour(ring_members, self_uuid, "left"))
            participant = True
        elif mid == COMMUNICATION_PORT:
            time.sleep(0.6)
            send_message(f"E({COMMUNICATION_PORT}, True)", get_neighbour(ring_members, self_uuid, "left"))
            participant = False
            active_mode_conversion(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT)
            
    elif flag == True: 
        leader = f"{get_local_ip()}:{mid}"
        print(f"Leader is: {leader}")
        if mid != COMMUNICATION_PORT:
            time.sleep(0.6)
            send_message(f"E({mid}, True)", get_neighbour(ring_members, self_uuid, "left"))
        participant = False

def active_mode_conversion(MY_IP, BROADCAST_PORT, COMMUNICATION_PORT, LISTENER_PORT):
    """
    Converts server from passive mode to leader if needed
    """
    BUFFER_SIZE = 1024
    global ring_members, client_list, leader, CLIENT_BROADCAST_PORT, uuid_mapping, server_socket, broadcast_socket 

    leader = f"{MY_IP}:{COMMUNICATION_PORT}"  
    print(f"I am the active server (Leader): {leader}")

    new_ring_members_message = f"RING_MEMBERS;{ring_members};{COMMUNICATION_PORT}"
    client_list_message = f"CLIENT_LIST;{client_list};{COMMUNICATION_PORT}"
    new_leader_message = f"NEW_LEADER;{leader};{COMMUNICATION_PORT}"
    uuid_mapping_message = f"NEW_UUID_MAPPING;{uuid_mapping};{COMMUNICATION_PORT}"
    groups_message = f"GROUPS;{groups};{COMMUNICATION_PORT}"
    groups_seq_numbers = f"SEQ_NUMBERS;{global_sequence_numbers};{COMMUNICATION_PORT}"
    time.sleep(1)  # Wait a second
    broadcast_socket.sendto(groups_seq_numbers.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(new_ring_members_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(client_list_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(uuid_mapping_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(groups_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(new_leader_message.encode(), (get_broadcast_address(), BROADCAST_PORT))
    broadcast_socket.sendto(new_leader_message.encode(), (get_broadcast_address(), CLIENT_BROADCAST_PORT))
    

    


if __name__ == '__main__':
    start_server()