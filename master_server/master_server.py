import socket
import select
import signal
import sys
import hashlib
from datetime import datetime
import sqlparse

# Server A config
HOST_MASTER = '0.0.0.0'
PORT_MASTER = 8001

# Server Sharding 1 and Server Sharding 2 config
HOST_SHARDING1 = '192.168.12.154'
PORT_SHARDING1 = 12347
HOST_SHARDING2 = '192.168.12.154'
PORT_SHARDING2 = 12347

HOST_REPLICA1 = '192.168.12.224'
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


def check_node_health_and_send_query(sharding_id, query, query_type):
    node_pair = NODE_PAIRS[sharding_id]
    if query_type == "SELECT":
        main_node = node_pair[node_pair['main'] ^ 1]
    else:
        main_node = node_pair[node_pair['main']]
    print(
        f"[Master Server] Sending {query_type} query to Sharding {sharding_id}, with main node {main_node['host']}:{main_node['port']}"
    )
    try:
        with socket.socket(socket.AF_INET,
                           socket.SOCK_STREAM) as sharding_socket:
            sharding_socket.connect((main_node['host'], main_node['port']))
            print(
                f"[Master Server] Connected to Sharding {sharding_id} with main node {main_node['host']}:{main_node['port']}"
            )
            sharding_socket.sendall(query.encode('utf-8'))
            response = sharding_socket.recv(1024).decode('utf-8')
            print(
                f"[Master Server] Received response from Sharding {sharding_id}: {response}"
            )  # Troubleshooting print

            return response
    except Exception as e:
        print(
            f"[Master Server] Error connecting to Sharding {sharding_id}: {e}")

        print(f"[Master Server] Switching main node for Sharding {sharding_id}")

        node_pair['main'] ^= 1
        main_node = node_pair[node_pair['main']]
        try:
            with socket.socket(socket.AF_INET,
                               socket.SOCK_STREAM) as sharding_socket:
                sharding_socket.connect((main_node['host'], main_node['port']))
                print(
                    f"[Master Server] Connected to Sharding {sharding_id} with main node {main_node['host']}:{main_node['port']}"
                )
                sharding_socket.sendall(query.encode('utf-8'))
                response = sharding_socket.recv(1024).decode('utf-8')
                print(
                    f"[Master Server] Received response from Sharding {sharding_id}: {response}"
                )  # Troubleshooting print

                return response
        except Exception as e:
            print(
                f"[Master Server] Error connecting to Sharding {sharding_id}: {e}"
            )

            print(
                f"[Master Server] Switching main node for Sharding {sharding_id}"
            )
            return "FAILED"

        return "FAILED"


def parse_select_query(query):
    """Parse a SELECT SQL query using sqlparse."""
    parsed = sqlparse.parse(query)[0]
    tokens = parsed.tokens

    table_name = None
    columns = None
    sharding_id = None
    where_condition = None

    for i, token in enumerate(tokens):
        print("token: ", token.value.upper(), ", type: ", token.ttype)
        if token.ttype is sqlparse.tokens.Keyword.DML and token.value.upper(
        ) == "SELECT":
            columns = str(tokens[i + 2]).strip()
        elif token.ttype is sqlparse.tokens.Keyword and token.value == "FROM":
            table_name = str(tokens[i + 2]).strip()
        elif i == len(
                tokens
        ) - 1 and token.ttype is not sqlparse.tokens.Punctuation:  # Where
            where_condition = str(tokens[i]).strip()
            sharding_id = get_shard(
                str(tokens[i]).split('=')[1].strip().strip(";").strip("'"))

    if not table_name or not columns:
        raise ValueError("Invalid SELECT query format.")
    return table_name, columns, sharding_id, where_condition


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
    # Parse values clause into rows
    rows = [row.strip().strip("()") for row in values.split("), (")]

    # Initialize shard-specific data
    values_shard_0 = []
    values_shard_1 = []

    # Process each row and allocate to the appropriate shard
    for row in rows:
        # Split the row by commas and process each value
        values_list = [
            # Handle 'NULL' by converting to None
            None if val.strip() == "NULL" else
            # If it's a number, convert it to an integer or float
            (int(val.strip()) if val.strip().isdigit() else
             (float(val.strip())
              if '.' in val.strip() else val.strip().strip("'").strip("\"")))
            for val in row.split(",")
        ]

        # Extract the state value (assuming you know the index of 'state' column)
        state_value = values_list[state_index]

        # Determine the shard ID based on the state value
        sharding_id = get_shard(state_value)

        # Append the values to the appropriate shard
        if sharding_id == 0:
            values_shard_0.append(values_list)
        elif sharding_id == 1:
            values_shard_1.append(values_list)
        else:
            raise ValueError(f"Invalid sharding ID: {sharding_id}")

    columns_list = "(" + ", ".join(columns_list) + ")"
    values_shard_0 = ", ".join(
        f"({', '.join(repr(value) if isinstance(value, str) else str(value) for value in record)})"
        for record in values_shard_0)
    values_shard_1 = ", ".join(
        f"({', '.join(repr(value) if isinstance(value, str) else str(value) for value in record)})"
        for record in values_shard_1)

    return table_name, columns_list, values_shard_0, values_shard_1


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
        table_name, columns, sharding_id, where_condition = parse_select_query(
            query)
        if sharding_id == 0 or sharding_id == 1:
            query = f"SELECT {columns} FROM {table_name} WHERE {where_condition}"

            response = ""

            for _ in range(2):
                response = check_node_health_and_send_query(
                    sharding_id, query, "SELECT")
                if "FAILED" not in response:
                    client_socket.sendall(response)
                    break

            if "FAILED" in response:
                print(
                    f"[Master Server] Failed to send SELECT query to Sharding {sharding_id}, both nodes are down"
                )

        elif sharding_id is None:
            query = f"SELECT {columns} FROM {table_name};"
            final_response = ""
            for i in range(2):
                response = ""

                for _ in range(2):
                    response = check_node_health_and_send_query(
                        i, query, "SELECT")
                    if "FAILED" not in response:
                        break

                if "FAILED" in response:
                    print(
                        f"[Master Server] Failed to send SELECT query to Sharding {sharding_id}, both nodes are down"
                    )
                else:
                    final_response += response
            client_socket.sendall(final_response.encode('utf-8'))
    elif query.startswith("INSERT"):
        print("[Master Server] Handling INSERT query")
        print("[Master Server] Forwarding query to Sharding Servers"
             )  # Troubleshooting print
        # Example: Parsing the INSERT query to extract table name and values
        # Assumes the query is in the form: "INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...)"

        table_name, columns_list, values_shard_0, values_shard_1 = parse_insert_query(
            query)
        response = ""

        for i in range(2):
            if i == 0 and values_shard_0 != "":
                updated_query = f"INSERT INTO {table_name} {columns_list} VALUES {values_shard_0};"
            elif i == 1 and values_shard_1 != "":
                updated_query = f"INSERT INTO {table_name} {columns_list} VALUES {values_shard_1};"
            print(
                f"[Master Server] Forwarding query to Sharding {i} Server: {updated_query}"
            )

            response += check_node_health_and_send_query(
                i, updated_query, "INSERT") + "\n"
            if "FAILED" not in response:
                client_socket.sendall(response.encode('utf-8'))
                break

        if "FAILED" in response:
            print(
                f"[Master Server] Failed to send INSERT query to Sharding {sharding_id}, both nodes are down"
            )
    elif query.startswith("CREATE"):
        print("[Master Server] Handling CREATE query")
        print("[Master Server] Forwarding query to Sharding and Replica Server"
             )  # Troubleshooting print
        try:
            table_name, columns_definition = parse_create_query(query)

            # Reconstruct the CREATE TABLE query for consistency
            formatted_query = f"CREATE TABLE {table_name} {columns_definition}"
            for sharding_id in range(1):
                response = check_node_health_and_send_query(
                    sharding_id, query, "CREATE")

                if "FAILED" in response:
                    print(
                        f"[Master Server] Failed to send CREATE query to Sharding {sharding_id}, both nodes are down"
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
