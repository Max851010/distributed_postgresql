import socket
import select
import signal
import sys

# Server A config
HOST_A = '0.0.0.0'
PORT_A = 8888

# Server B and Server C config
HOST_B = '192.168.12.47'
PORT_B = 12347
HOST_C = 'localhost'
PORT_C = 65434

# Flag to indicate server shutdown
shutdown_flag = False
server_socket = None  # Reference to the server socket


def handle_request(client_socket):
    query = client_socket.recv(1024).decode('utf-8')
    print(f"[Server A] Received query: {query}")  # Troubleshooting print

    if query.startswith("SELECT"):
        print(
            "[Server A] Forwarding query to Server B")  # Troubleshooting print
        with socket.socket(socket.AF_INET,
                           socket.SOCK_STREAM) as server_b_socket:
            server_b_socket.connect((HOST_B, PORT_B))
            server_b_socket.sendall(query.encode('utf-8'))
            response = server_b_socket.recv(1024)
            print(
                f"[Server A] Received response from Server B: {response.decode('utf-8')}"
            )  # Troubleshooting print
            client_socket.sendall(response)
    else:
        print(
            "[Server A] Forwarding query to Server C")  # Troubleshooting print
        with socket.socket(socket.AF_INET,
                           socket.SOCK_STREAM) as server_c_socket:
            server_c_socket.connect((HOST_C, PORT_C))
            server_c_socket.sendall(query.encode('utf-8'))
            response = server_c_socket.recv(1024)
            print(
                f"[Server A] Received response from Server C: {response.decode('utf-8')}"
            )  # Troubleshooting print
            client_socket.sendall(response)


def handle_sigint(signum, frame):
    global shutdown_flag, server_socket
    print("[Server A] Received SIGINT. Shutting down...")
    shutdown_flag = True
    if server_socket:
        print("[Server A] Closing server socket and releasing port...")
        server_socket.close()  # Close the server socket to release the port


def run_server():
    global shutdown_flag
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST_A, PORT_A))
    server_socket.listen(5)
    server_socket.setblocking(False)
    print(f"[Server A] Listening on {HOST_A}:{PORT_A}")

    poller = select.poll()
    poller.register(server_socket, select.POLLIN)

    # Set up SIGINT handler
    signal.signal(signal.SIGINT, handle_sigint)

    # Dictionary to track registered sockets
    registered_sockets = {}

    while not shutdown_flag:
        events = poller.poll(1)  # Timeout of 1 second to check for shutdown
        for fileno, event in events:
            if fileno == server_socket.fileno():
                # Accept new connection
                client_socket, client_address = server_socket.accept()
                print(f"[Server A] Accepted connection from {client_address}")
                client_socket.setblocking(False)
                poller.register(client_socket, select.POLLIN)
                registered_sockets[client_socket] = True  # Track the socket
            elif event == select.POLLIN:
                # Handle incoming data
                print("[Server A] Ready to handle a request")
                client_socket = socket.fromfd(fileno, socket.AF_INET,
                                              socket.SOCK_STREAM)
                handle_request(client_socket)

                # Safe unregistration and cleanup
                if client_socket in registered_sockets and registered_sockets[
                        client_socket]:
                    poller.unregister(client_socket)
                    registered_sockets[
                        client_socket] = False  # Mark as unregistered
                client_socket.close()

    # Clean up when server shuts down
    print("[Server A] Server has shut down cleanly.")


if __name__ == "__main__":
    run_server()
