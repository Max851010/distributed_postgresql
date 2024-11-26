import socket
import select
import signal
import sys
import hashlib
from datetime import datetime
import sqlparse

# Server A config
HOST_MASTER = '0.0.0.0'
PORT_MASTER = 8888

# Server Sharding 1 and Server Sharding 2 config
HOST_SHARDING1 = '192.168.12.47'
PORT_SHARDING1 = 12347
HOST_SHARDING2 = 'localhost'
PORT_SHARDING2 = 65434

HOST_REPLICA1 = '192.168.12.47'
PORT_REPLICA1 = 12347
HOST_REPLICA2 = 'localhost'
PORT_REPLICA2 = 65434

# Flag to indicate server shutdown
shutdown_flag = False
server_socket = None  # Reference to the server socket
SHARDING_NODES = {
    0: {
        'host': HOST_SHARDING1,
        'port': PORT_SHARDING1
    },
    1: {
        'host': HOST_SHARDING2,
        'port': PORT_SHARDING2
    }
}
REPLICA_NODES = {
    0: {
        'host': HOST_REPLICA1,
        'port': PORT_REPLICA1
    },
    1: {
        'host': HOST_REPLICA2,
        'port': PORT_REPLICA2
    }
}

NODE_PAIRS = [{
    'main': 0,
    0: SHARDING_NODES[0],
    1: REPLICA_NODES[0]
}, {
    'main': 0,
    0: SHARDING_NODES[1],
    1: REPLICA_NODES[1]
}]


def get_shard(state_abbreviation):
    """
    Determine which shard (0 or 1) a state belongs to, with validation.

    :param state_abbreviation: The state abbreviation (e.g., "AL", "NY").
    :return: The shard number (0 or 1) if the abbreviation is valid; otherwise, None.
    """
    valid_states = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
        "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
        "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
        "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
        "WI", "WY"
    }

    if state_abbreviation not in valid_states:
        raise ValueError(f"Invalid state abbreviation: {state_abbreviation}")

    hash_value = hashlib.md5(state_abbreviation.encode()).hexdigest()
    return int(hash_value, 16) % 2  # Modulo 2 for two shards


def parse_select_query(query):
    """Parse a SELECT SQL query using sqlparse."""
    parsed = sqlparse.parse(query)[0]
    tokens = parsed.tokens

    table_name = None
    columns = None

    for i, token in enumerate(tokens):
        print("token: ", token.value.upper(), ", type: ", token.ttype)
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper(
        ) == "FROM":
            table_name = str(tokens[i + 2]).strip()
        elif token.ttype is sqlparse.tokens.Punctuation and token.value == ";":
            columns = tokens[i - 1].value
            break

    if not table_name or not columns:
        raise ValueError("Invalid SELECT query format.")
    return table_name, columns, sharding_id


def parse_insert_query(query):
    """
    Parse an INSERT SQL query using sqlparse and determine the sharding ID based on the 'state' column.

    :param query: The SQL query string to parse.
    :return: A tuple (table_name, columns, values, sharding_id).
    """
    # Parse the SQL query
    parsed = sqlparse.parse(query)[0]
    tokens = parsed.tokens

    # Extract table name, columns, and values
    table_name = None
    columns = None
    values = None

    for i, token in enumerate(tokens):
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper(
        ) == "INTO":
            table_name = str(tokens[i +
                                    2]).split("(")[0].strip()  # Get table name
            columns = "(" + str(
                tokens[i + 2]).split("(")[1].strip()  # Get columns if present
        elif token.ttype is sqlparse.tokens.Punctuation and token.value.upper(
        ) == ";":
            values = str(tokens[i - 1]).replace("VALUES",
                                                "").strip()  # Get values

    if not table_name or not values:
        raise ValueError("Invalid INSERT query format.")

    # Ensure columns and values are provided
    if not columns or not columns.startswith("(") or not columns.endswith(")"):
        raise ValueError("Invalid columns format.")
    if not values.startswith("(") or not values.endswith(")"):
        raise ValueError("Invalid values format.")

    # Remove parentheses and split into lists
    columns_list = [col.strip() for col in columns[1:-1].split(",")]
    values_list = [val.strip().strip("'") for val in values[1:-1].split(",")]

    # Check if 'state' column exists and get its value
    if not any(col.lower() == "state" for col in columns_list):
        raise ValueError("'state' column is required for sharding.")
    state_index = next(
        (i for i, col in enumerate(columns_list) if col.lower() == "state"),
        None)
    if state_index is None:
        raise ValueError(
            "'state' column (case-insensitive) is required for sharding.")

    state_value = values_list[state_index]

    # Determine shard ID
    sharding_id = get_shard(state_value)

    return table_name, columns, values, sharding_id


def parse_create_query(query):
    """Parse a CREATE SQL query using sqlparse."""
    parsed = sqlparse.parse(query)[0]
    tokens = parsed.tokens

    table_name = None
    columns_definition = None

    for i, token in enumerate(tokens):
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper(
        ) == "TABLE":
            # The table name comes after the TABLE keyword
            table_name = str(tokens[i + 2]).strip()
        elif token.ttype is sqlparse.tokens.Punctuation and token.value == ";":
            # The column definitions are inside parentheses
            columns_definition = tokens[i - 1].value
            break

    if not table_name or not columns_definition:
        raise ValueError("Invalid CREATE TABLE query format.")
    if not "state" in columns_definition.lower():
        raise ValueError("Missing 'state' column in CREATE TABLE query.")
    return table_name, columns_definition


def handle_request(client_socket):
    query = client_socket.recv(1024).decode('utf-8')
    print(f"[Master Server] Received query: {query}")  # Troubleshooting print
    sql_commands = ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP")
    if query.startswith("SELECT"):
        print("[Master Server] Forwarding query to Replica Servers"
             )  # Troubleshooting print
        with socket.socket(socket.AF_INET,
                           socket.SOCK_STREAM) as server_b_socket:
            server_b_socket.connect((HOST_SHARDING2, PORT_SHARDING2))
            server_b_socket.sendall(query.encode('utf-8'))
            response = server_b_socket.recv(1024)
            print(
                f"[Master Server] Received response from Partition B Server: {response.decode('utf-8')}"
            )  # Troubleshooting print
            client_socket.sendall(response)
    elif query.startswith("INSERT"):
        print("[Master Server] Handling INSERT query")
        print("[Master Server] Forwarding query to Sharding Servers"
             )  # Troubleshooting print
        # Example: Parsing the INSERT query to extract table name and values
        # Assumes the query is in the form: "INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...)"

        table_name, columns, query_values, sharding_id = parse_insert_query(
            query)
        record_id = get_next_id(table_name)
        query_values = query_values.replace("(", f"({next_id},", 1)
        updated_query = f"INSERT INTO {table_name} {columns} VALUES {query_values}"
        print(
            f"[Master Server] Forwarding query to {'sharding 1' if sharding_id == 0 else 'sharding 2'} Server: {updated_query}"
        )
        with socket.socket(socket.AF_INET,
                           socket.SOCK_STREAM) as server_c_socket:
            target_pair = NODE_PAIRS[sharding_id]
            main_node = target_pair[target_pair['main']]

            server_c_socket.connect(main_node['host'], main_node['port'])
            server_c_socket.sendall(query.encode('utf-8'))
            response = server_c_socket.recv(1024)
            print(
                f"[Master Server] Received response from {'sharding 1' if sharding_id == 0 else 'sharding 2'} Server: {response.decode('utf-8')}"
            )  # Troubleshooting print
            client_socket.sendall(response)
    elif query.startswith("CREATE"):
        print("[Master Server] Handling INSERT query")
        print("[Master Server] Forwarding query to Sharding and Replica Server"
             )  # Troubleshooting print
        try:
            table_name, columns_definition = parse_create_query(query)

            # Reconstruct the CREATE TABLE query for consistency
            formatted_query = f"CREATE TABLE {table_name} {columns_definition}"
            for pair in NODE_PAIRS:
                main_node = pair[pair['main']]
                replica_node = pair[pair['main'] ^ 1]

                try:
                    with socket.socket(socket.AF_INET,
                                       socket.SOCK_STREAM) as sharding_socket:
                        sharding_socket.connect(
                            (main_node['host'], main_node['port']))
                        print(
                            f"[Master Server] Sending CREATE query to Sharding {sharding_id + 1}"
                        )
                        sharding_socket.sendall(formatted_query.encode('utf-8'))
                        sharding_response = sharding_socket.recv(1024)
                        print(
                            f"[Master Server] Received response from Sharding {sharding_id + 1}: {sharding_response.decode('utf-8')}"
                        )
                except Exception as e:
                    print(
                        f"[Master Server] Error connecting to Sharding {sharding_id + 1}: {e}"
                    )
            client_socket.sendall(b"CREATE query forwarded to sharding nodes.")

        except Exception as e:
            print(f"[Master Server] Error processing CREATE query: {e}")
            client_socket.sendall(b"Error processing CREATE query.")
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
