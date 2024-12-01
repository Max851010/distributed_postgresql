import socket
import signal
import sys
from pprint import pprint

# Master Server details
HOST_MASTER = '192.168.12.140'  # Replace with Master Server's IP address
PORT_MASTER = 12346
print(f"Connecting to Master Server {HOST_MASTER}:{PORT_MASTER}")

client_socket = None  # Define global variable to store the client socket


# Signal handler for Ctrl + C (SIGINT)
def signal_handler(sig, frame):
    print("\nCtrl + C detected. Closing the socket...")
    if client_socket:
        client_socket.close()
    sys.exit(0)  # Exit the program


# Set up signal handling
signal.signal(signal.SIGINT, signal_handler)

# Create Client A and handle user input loop
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST_MASTER, PORT_MASTER))

while True:
    try:
        user_input = input("Enter your query: ")

        # Send data to Master Server
        client_socket.send(user_input.encode())

        # Receive response from Master Server
        response = client_socket.recv(1024).decode()
        print("Response from Master Server:")
        pprint(response)

    except KeyboardInterrupt:
        # Handle interruption (Ctrl + C) if it occurs in the input section
        print("\nInterrupted by user, closing the socket...")
        if client_socket:
            client_socket.close()
        sys.exit(0)  # Exit the program
