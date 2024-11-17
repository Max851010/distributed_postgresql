import socket

# Server A details
HOST_MASTER = '192.168.12.140'  # Replace with Server A's IP address
PORT_MASTER = 8888

# Create Client A
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST_MASTER, PORT_MASTER))

user_input = input("Enter a query: ")

# Send data to Server A
client_socket.send(user_input.encode())

# Receive response from Server A
response = client_socket.recv(1024).decode()
print(f"Response from Server A: {response}")

client_socket.close()
