import socket
import select
import signal
import sys
import hashlib
from datetime import datetime
import redis
import sqlparse

# Server A config
HOST_MASTER = '0.0.0.0'
PORT_MASTER = 8888

# Server Partition 1 and Server Partition 2 config
HOST_PARTITION1 = '192.168.12.47'
PORT_PARTITION2 = 12347
HOST_PARTITION1 = 'localhost'
PORT_PARTITION2 = 65434

HOST_REPLICA1 = '192.168.12.47'
PORT_REPLICA2 = 12347
HOST_REPLICA1 = 'localhost'
PORT_REPLICA2 = 65434

# Redis setup
HOST_REDIS = 'localhost'
PORT_REDIS = 6379
REDIS_DB = 0
redis_client = redis.StrictRedis(host=HOST_REDIS, port=PORT_REDIS, db=REDIS_DB)

# Flag to indicate server shutdown
shutdown_flag = False
server_socket = None  # Reference to the server socket
PARTITION_NODES = {
    0: {
        'host': HOST_PARTITION1,
        'port': PORT_PARTITION1
    },
    1: {
        'host': HOST_PARTITION2,
        'port': PORT_PARTITION2
    }
}


# Helper function to get the next ID from Redis
def get_next_id(table_name, partition_id):
    redis_key = f"{table_name}:partition:{partition_id}:last_id"
    # Increment and get the next available ID
    next_id = redis_client.incr(redis_key)
    return next_id


def compute_partition_id(record_id, total_partitions):
    return record_id % total_partitions


def parse_insert_query(query):
    """Parse an INSERT SQL query using sqlparse."""
    parsed = sqlparse.parse(query)[0]
    tokens = parsed.tokens

    # Extract table name and values
    table_name = None
    values = None
    columns = None

    for i, token in enumerate(tokens):
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper(
        ) == "INTO":
            table_name = str(tokens[i + 2]).strip()  # Get table name
        elif token.ttype is sqlparse.tokens.Keyword and token.value.upper(
        ) == "VALUES":
            values = str(tokens[i + 2]).strip()  # Get values
            columns = str(tokens[i - 1]).strip()  # Get columns if present

    if not table_name or not values:
        raise ValueError("Invalid INSERT query format.")

    return table_name, columns, values


def handle_request(client_socket):
    query = client_socket.recv(1024).decode('utf-8')
    print(f"[Master Server] Received query: {query}")  # Troubleshooting print
    sql_commands = ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP")
    if query.startswith("SELECT"):
        print("[Master Server] Forwarding query to Partition B Server"
             )  # Troubleshooting print
        with socket.socket(socket.AF_INET,
                           socket.SOCK_STREAM) as server_b_socket:
            server_b_socket.connect((HOST_B, PORT_B))
            server_b_socket.sendall(query.encode('utf-8'))
            response = server_b_socket.recv(1024)
            print(
                f"[Master Server] Received response from Partition B Server: {response.decode('utf-8')}"
            )  # Troubleshooting print
            client_socket.sendall(response)
    elif query.startswith("INSERT"):
        print("[Master Server] Handling INSERT query")
        print("[Master Server] Forwarding query to Partition C Server"
             )  # Troubleshooting print
        # Example: Parsing the INSERT query to extract table name and values
        # Assumes the query is in the form: "INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...)"

        table_name, columns, query_values = parse_insert_query(query)
        record_id = get_next_id(table_name)
        partition_id = compute_partition_id(record_id, total_partitions)
        query_values = query_values.replace("(", f"({next_id},", 1)
        updated_query = f"INSERT INTO {table_name} {columns} VALUES {query_values}"
        print(
            f"[Master Server] Forwarding query to Partition C Server: {updated_query}"
        )
        with socket.socket(socket.AF_INET,
                           socket.SOCK_STREAM) as server_c_socket:
            server_c_socket.connect((PARTITION_NODES[partition_id]['host'],
                                     PARTITION_NODES[partition_id]['port']))
            server_c_socket.sendall(query.encode('utf-8'))
            response = server_c_socket.recv(1024)
            print(
                f"[Master Server] Received response from {if partition_id == 0 'partition 1' else 'partition 2'} Server: {response.decode('utf-8')}"
            )  # Troubleshooting print
            client_socket.sendall(response)
    else:
        print("[Master Server] Invalid query.")  # Troubleshooting print
        client_socket.sendall(b"Invalid query.")


def handle_sigint(signum, frame):
    global shutdown_flag, server_socket
    print("[Master Server] Received SIGINT. Shutting down...")
    shutdown_flag = True
    if server_socket:
        print("[Master Server] Closing server socket and releasing port...")
        server_socket.close()  # Close the server socket to release the port


def run_server():
    global shutdown_flag
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST_MASTER, PORT_MASTER))
    server_socket.listen(5)
    server_socket.setblocking(False)
    print(f"[Master Server] Listening on {HOST_MASTER}:{PORT_MASTER}")

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
                print(
                    f"[Master Server] Accepted connection from {client_address}"
                )
                client_socket.setblocking(False)
                poller.register(client_socket, select.POLLIN)
                registered_sockets[client_socket] = True  # Track the socket
            elif event == select.POLLIN:
                # Handle incoming data
                print("[Master Server] Ready to handle a request")
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
    print("[Master Server] Server has shut down cleanly.")


if __name__ == "__main__":
    run_server()
