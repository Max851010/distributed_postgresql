import socket
import os
from pprint import pprint

# Master Server details
HOST_MASTER = '10.128.0.2'  # Replace with Master Server's IP address
PORT_MASTER = 12347
print(f"Connecting to Master Server {HOST_MASTER}:{PORT_MASTER}")

# Create Client A
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST_MASTER, PORT_MASTER))

user_input = input("Enter your query: ")

# Send data to Master Server
client_socket.send(user_input.encode())

# Receive response from Master Server
response = client_socket.recv(1024).decode()
print("Response from Master Server:")
pprint(response)

client_socket.close()
