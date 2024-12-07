import socket

# Create a UDP socket
client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Server address and port
server_address = '192.168.230.104'  # Listen on all interfaces
server_port = 10001

# Buffer size
buffer_size = 1024

print("Type 'exit' to close the client.")

try:
    while True:
        # Input message from user
        message = input('Please enter message: ')
        
        # Exit condition
        if message.lower() == 'exit':
            print("Exiting client...")
            break
        
        # Send data to server
        client_socket.sendto(message.encode(), (server_address, server_port))
        print('Sent to server:', message)

        # Receive response from server
        print('Waiting for response...')
        data, server = client_socket.recvfrom(buffer_size)
        print('Received message from server:', data.decode())

finally:
    client_socket.close()
    print('Socket closed')
