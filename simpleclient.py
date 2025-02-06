import json
from queue import PriorityQueue
import random
import re
import socket
import struct
import threading
import time
import traceback
import uuid

server_ip = ""
server_communication_port = -1
uuid_mapping = {}
client_socket = None
next_global_seq_no = 1  # Erwartete Sequenznummer
lock = threading.Lock()  # Lock zur Synchronisation von Threads
broadcast_socket = None
group_seq_nums = {}
message_queue_failed_messages = []  # Warteschlange für Nachrichten

def generate_uuid():
    return str(uuid.uuid4())

def get_local_ip():
    """
    Ermittelt die lokale IP-Adresse des Geräts.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))  # Google-DNS-Server als Ziel
        return s.getsockname()[0]

def get_broadcast_address():
    """
    Ermittelt die Broadcast-Adresse basierend auf der lokalen IP-Adresse und Subnetzmaske.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))  # Google DNS als Ziel
        local_ip = s.getsockname()[0]

    subnet_mask = "255.255.255.0"
    ip_int = struct.unpack("!I", socket.inet_aton(local_ip))[0]
    mask_int = struct.unpack("!I", socket.inet_aton(subnet_mask))[0]
    broadcast_int = ip_int | ~mask_int
    broadcast_address = socket.inet_ntoa(struct.pack("!I", broadcast_int & 0xFFFFFFFF))
    return broadcast_address


def listen_for_direct_messages():
    """
    Lauscht auf direkte Nachrichten vom Server.
    """
    global next_global_seq_no, server_communication_port, server_ip, client_socket, group_seq_nums, message_queue_failed_messages


    received_messages = set()
    message_queue = PriorityQueue()

    while True:
        try:
            data, addr = client_socket.recvfrom(1024)
            print(f"Direkte Nachricht von {addr}: {data.decode()}")

            if data.decode().startswith("DISCOVER_SERVER_RESPONSE;"):
                with lock:
                    server_ip = addr[0]
                    server_communication_port = data.decode().split(';')[1]
                    #print('IM AM THE DEVIL')
                    group_seq_nums["public"] = int(data.decode().split(';')[2]) + 1
            elif data.decode().startswith('GROUP_MEMBER_ADDED;'):

                groupname = data.decode().split(';')[2]
                group_seq_num = data.decode().split(';')[1]

                with lock:
                    group_seq_nums[groupname] = int(group_seq_num) + 1
                    #print(group_seq_nums)
            elif data.decode() == 'Public message distributed': 
                print('Public message was distributed')
            else:

                message = json.loads(data.decode())
                seq_no = message["seq_no"]
                group_of_message = message['group']

                with lock:
                    if seq_no in received_messages:
                        #print("Got double")
                        continue

                    received_messages.add(seq_no)
                    #print("Added to set of sequence numbers")

                    message_queue.put((seq_no, message))
                    #print("Added to queue")

                    while not message_queue.empty():
                        seq, msg = message_queue.queue[0]
                        if seq == group_seq_nums[group_of_message]:
                            message_queue.get()
                            print(f"Nachricht verarbeitet: {msg['content']}")
                            group_seq_nums[group_of_message] += 1
                        else:
                            #print("EXPECTED SEQUENCE NUMBER:", group_seq_nums[group_of_message])
                            break
        except socket.timeout:
            pass
        except json.JSONDecodeError:
            if data.decode() == "PING":
                print(f"Ping von {addr} erhalten. Sende PONG.")
                client_socket.sendto("PONG".encode(), addr)
            elif data.decode().startswith('ACK;'):
                ## FINISH ACK SHIT HANDLE REC ACK AND DELETE FROM LIST message_queue_failed_messages
                msg_id = data.decode().split(';')[1]
                for msg in message_queue_failed_messages:
                    msg_content = json.loads(msg)
                    if msg_content['message_id'] == msg_id:
                        message_queue_failed_messages.remove(msg)
        except Exception as e:
            
            print(f"Fehler beim Empfang direkter Nachrichten: {e}")


def listen_for_broadcast(client_broadcast_port):
    """
    Lauscht auf Broadcast-Nachrichten vom Netzwerk.
    """
    global server_ip, server_communication_port, broadcast_socket, group_seq_nums, message_queue_failed_messages, client_socket

    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_socket.bind(("", client_broadcast_port))

    print(f"Listening for broadcast messages on port {client_broadcast_port}...")

    received_messages_broadcast = set()
    message_queue_broadcast = PriorityQueue()

    while True:
        try:
            data, addr = broadcast_socket.recvfrom(1024)

            if data.decode().startswith("sender"):
                if data.decode().split("sender: ")[1].strip().replace('"', '') == (f"('{get_local_ip()}', {COMMUNICATION_PORT})"):
                    continue

                print(f"Broadcast-Nachricht von {addr}: {data.decode()}")
            elif "NEW_LEADER" in data.decode():
                print(f"Broadcast-Nachricht von {addr}: {data.decode()}")

                with lock:
                    server_ip = addr[0]

                
                
                with lock:
                    server_communication_port = data.decode().split(";")[2]
                print("Extrahierter Port:", server_communication_port)

                for message in message_queue_failed_messages:
                    time.sleep(0.1)
                    client_socket.sendto(message.encode(), (server_ip, int(server_communication_port) + 1))
                message_queue_failed_messages.clear()
               
            else:
            
                message = json.loads(data.decode())
                seq_no = message["seq_no"]
                group_of_message = message['group']
                client_sending_broadcast = message['sender']

                if client_sending_broadcast != [get_local_ip(), COMMUNICATION_PORT]:
                    

                    with lock:
                        if seq_no in received_messages_broadcast:
                            #print("Got double")
                            continue

                        received_messages_broadcast.add(seq_no)
                        #print("Added to set of sequence numbers")

                        message_queue_broadcast.put((seq_no, message))
                        #print("Added to queue")

                        #print(str(message_queue_broadcast.queue[0]))

                        while not message_queue_broadcast.empty():
                            seq, msg = message_queue_broadcast.queue[0]
                            if seq == group_seq_nums[group_of_message]:
                                message_queue_broadcast.get()
                                print(f"Nachricht verarbeitet: {msg['content']}")
                                group_seq_nums[group_of_message] += 1
                                #print('UPDATED SEQ NUMS TO', group_seq_nums[group_of_message])
                            else:
                                #print("EXPECTED SEQUENCE NUMBER:", group_seq_nums[group_of_message], seq, message['message_id'])
                                break
        except Exception as e:
            print(f"Fehler beim Empfangen der Broadcast-Nachricht: {e}")
            traceback.print_exc()


BROADCAST_PORT = 5973
CLIENT_BROADCAST_PORT = 5974
COMMUNICATION_PORT = random.randint(10000, 11000)

broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
broadcast_socket.settimeout(2)

client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((get_local_ip(), COMMUNICATION_PORT))
client_socket.settimeout(5)
client_socket.setblocking(True)

buffer_size = 1024

listener_thread = threading.Thread(target=listen_for_broadcast, args=(CLIENT_BROADCAST_PORT,))
listener_thread.daemon = True
listener_thread.start()
#print("THREAD GESTARTET")

direct_message_thread = threading.Thread(target=listen_for_direct_messages)
direct_message_thread.daemon = True
direct_message_thread.start()
#print("THREAD GESTARTET")

print(f"Type 'exit' to close the client. {COMMUNICATION_PORT}")

try:
    try:
        broadcast_message = f"DISCOVER_BY_CLIENT;{COMMUNICATION_PORT}"
        broadcast_address = get_broadcast_address()
        broadcast_socket.sendto(broadcast_message.encode(), (broadcast_address, BROADCAST_PORT))
        print(f"Broadcast gesendet: {broadcast_message}")
    except socket.timeout:
        print("No response from server. Exiting client...")
        exit()

    while True:       

        message = input("Please enter message(For group registration use GROUP_REG groupname): ")

        if message.lower() == 'exit':
            print("Exiting client...")
            break

        if "GROUP_REG" in message:
            message = f"{message.split(' ')[0]}:{message.split(' ')[1]}:({get_local_ip()}, {COMMUNICATION_PORT})"

        message_id = f"msg-{uuid.uuid4()}"

        try:

            with lock:
                client_socket.sendto(json.dumps({'content': message, 'message_id': message_id}).encode(), (server_ip, int(server_communication_port) + 1))
                #print("MESSAGE SENT TO:", (server_ip, int(server_communication_port)))

                if "GROUP_REG" not in message:
                    if len(group_seq_nums.keys()) == 1:
                        group_seq_nums["public"] += 1
                        #print('UPDATED SEQ NUM IN PÜBLIC', group_seq_nums["public"])
                    else:
                        for group in group_seq_nums.keys():
                            if group != "public":
                                #print('UPDATING AFTER SEND FOR GROUP', group)
                                group_seq_nums[group] += 1
                print("Sent to server:", message)
                message_queue_failed_messages.append(json.dumps({'content': message, 'message_id': message_id}))
        except:
            print('failed to send message')
            
finally:
    client_socket.close()
    print("Socket closed")
