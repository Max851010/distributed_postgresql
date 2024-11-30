update_server/update_server.py
import socket
import psycopg2
from psycopg2 import sql
from collections import deque
import threading
import time
import select
import signal

# Update Server details
HOST_OWN_SERVER = '0.0.0.0'  # Bind to all interfaces
PORT_OWN_SERVER = 12347  # Port to listen on

# Replica Server details (replica server)
HOST_ANOTHER_SERVER = '192.168.12.224'  # Change to Server B's address
PORT_ANOTHER_SERVER = 12347  # Change to Server B's port

# Main Server details
HOST_MAIN_SERVER = '192.168.12.140'
PORT_MAIN_SERVER = 8001

# Global variables
shutdown_flag = False
server_socket = None
replica_node_status = "RUNNING"
missing_queries = deque([])
missing_query_manager = None

# PostgreSQL connection details
DB_NAME = "autonomous_car_database_0"
DB_USER = "kenyang"
DB_PASSWORD = "ken890404"
DB_HOST = "localhost"
DB_PORT = "8888"
# TABLE_NAME = "test1_table"

# Log file name
LOG_FILE = "sql_log.txt"
LOG_DIFF_FILE = "sql_log_diff.txt"

# check this server is update server or replica server
is_update_server = True
update_server_host = HOST_OWN_SERVER
update_server_port = PORT_OWN_SERVER
replica_server_host = HOST_ANOTHER_SERVER
replica_server_port = PORT_ANOTHER_SERVER

# --- Database Functions ---


def connect_postgres(dbname="postgres"):
    """Connect to PostgreSQL using psycopg2 with the specified database."""
    try:
        connection = psycopg2.connect(user=DB_USER,
                                      password=DB_PASSWORD,
                                      host=DB_HOST,
                                      dbname=dbname,
                                      port=DB_PORT)
        connection.autocommit = True
        print(f"Connected to PostgreSQL database '{dbname}'.")
        return connection
    except Exception as error:
        print(f"Failed to connect to PostgreSQL: {error}")
        return None


def create_database_if_not_exists():
    """Create the database if it doesn't already exist."""
    connection = connect_postgres()
    if connection is None:
        return

    try:
        cursor = connection.cursor()
        cursor.execute(
            f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {DB_NAME};")
            print(f"Database '{DB_NAME}' created successfully.")
        else:
            print(f"Database '{DB_NAME}' already exists.")
    except Exception as error:
        print(f"Failed to create database: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()


def delete_database(database_name):
    """
    Drops the specified database if it exists.
    """
    try:
        # Connect to the default database (usually "postgres")
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            dbname=
            "postgres"  # Connect to a database other than the one you want to drop
        )
        connection.autocommit = True  # Enable auto-commit for DROP DATABASE
        cursor = connection.cursor()

        # Check if the database exists and drop it
        drop_query = f"DROP DATABASE IF EXISTS {database_name};"
        cursor.execute(drop_query)
        print(f"Database '{database_name}' has been dropped (if it existed).")
    except Exception as e:
        print(f"Error dropping database '{database_name}': {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()


def create_table():
    """Create the 'test1_table' table in the database if it doesn't exist."""
    try:
        conn = connect_postgres(DB_NAME)
        cursor = conn.cursor()
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                message TEXT NOT NULL,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        cursor.execute(create_table_query)
        print(f"Table '{TABLE_NAME}' is ready.")
    except Exception as error:
        print(f"Failed to create table: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()


# --- Read Log File ---


def get_queries():
    """Reads queries from log.txt and returns them as a list."""
    file_path = LOG_FILE
    try:
        with open(file_path, "r") as file:
            return file.readlines()
    except FileNotFoundError:
        return []


def execute_sql_message(sql_message):
    """Execute the SQL message in the database."""
    try:
        # Connect to the specified PostgreSQL database
        conn = connect_postgres(DB_NAME)
        cursor = conn.cursor()

        # Execute the provided SQL command
        cursor.execute(sql_message)
        conn.commit()

        # If execution is successful, return success message with the SQL command
        return f"Ack: {sql_message}"
    except Exception as error:
        print(f"Error executing SQL: {error}")
        # If execution fails, return failure message with the SQL command
        return f"Fail."
    finally:
        # Ensure resources are cleaned up: close the cursor and the connection
        if conn:
            cursor.close()
            conn.close()


# --- Logging Functions ---


def write_log_to_file(sql_message, file):
    """Write the SQL operation to the log file."""
    try:
        with open(file, "a") as log_file:
            log_file.write(sql_message + "\n")
        print("SQL operation logged successfully.")
    except Exception as error:
        print(f"Failed to write to log file: {error}")


# --- Replica Sync Functions ---


def check_replica_node_status():
    while True:
        try:
            replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_socket.connect((HOST_ANOTHER_SERVER, PORT_ANOTHER_SERVER))
            replica_socket.send("TEST".encode())

            # Wait for acknowledgment
            acknowledgment = replica_socket.recv(1024).decode()
            replica_socket.close()
            print(f"Reconnected to Server B and received acknowledgment from Server B: {acknowledgment}")

            break
        except Exception as error:
            print(f"Failed to sync with Server B: {error}")
            time.sleep(5)


def sync_missing_queries():
    global missing_queries

    while missing_queries:
        sql_message = missing_queries[0]

        print(f"Resyncing with Server B: {sql_message}")

        try:
            replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_socket.connect((HOST_ANOTHER_SERVER, PORT_ANOTHER_SERVER))
            replica_socket.send(sql_message.encode())

            # Wait for acknowledgment
            acknowledgment = replica_socket.recv(1024).decode()
            replica_socket.close()

            # Remove the query from the queue
            missing_queries.popleft()
            print(f"Received resync acknowledgment from Server B: {acknowledgment}")
        except Exception as error:
            print(f"Failed to resync with Server B: {error}")
            break

def manage_missing_queries():
    global missing_queries, replica_node_status

    while replica_node_status == "DOWN":
        # Check the status of the replica node
        check_replica_node_status()

        with open(LOG_DIFF_FILE, "r") as log_file:
            lines = log_file.readlines()
            for line in lines:
                missing_queries.append(line.strip())

        replica_node_status = "RECOVERING"

        # resync missing queries with server B
        sync_missing_queries()

        try:
            with open(LOG_DIFF_FILE, "w") as log_file:
                for sql_message in missing_queries:
                    log_file.write(sql_message + "\n")
            print("SQL operation diff logged successfully.")
        except Exception as error:
            print(f"Failed to write to log file: {error}")

        # check if all queries are synced
        if not missing_queries:
            replica_node_status = "RUNNING"
            print("All missing queries synced with Server B.")
        else:
            replica_node_status = "DOWN"

        missing_queries.clear()


def sync_with_replica_server(sql_message):
    """Send the SQL message to replica server B and wait for acknowledgment."""
    global missing_queries, replica_node_status, missing_query_manager

    if replica_node_status == "DOWN":
        write_log_to_file(sql_message, LOG_DIFF_FILE)
        print(f"Server B is down.")
        return "DOWN"
    elif replica_node_status == "RECOVERING":
        missing_queries.append(sql_message)
        print("Recovering with Server B...")
        return "RECOVERING"
    else:
        try:
            replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_socket.connect((HOST_ANOTHER_SERVER, PORT_ANOTHER_SERVER))
            replica_socket.send(sql_message.encode())

            # Wait for acknowledgment
            acknowledgment = replica_socket.recv(1024).decode()
            replica_socket.close()
            print(f"Received acknowledgment from Server B: {acknowledgment}")
            replica_node_status = "RUNNING"

            return acknowledgment
        except Exception as error:
            replica_node_status = "DOWN"
            write_log_to_file(sql_message, LOG_DIFF_FILE)

            missing_query_manager = threading.Thread(
                target=manage_missing_queries)
            missing_query_manager.start()

            print(f"Failed to sync with Server B: {error}")
            return f"Failed: {error}"


# --- SQL Message Processing ---


def process_sql_message(sql_message):
    """
    Process the SQL message:
    - Log to the local file
    - Execute the SQL message
    - Sync with Server B
    - Return response to the client
    """

    # Step 1: Execute in local database
    response = execute_sql_message(sql_message)

    if not response.startswith("Fail"):
        # Step 2: Write to log file
        write_log_to_file(sql_message, LOG_FILE)

    # Step 3: Sync with Server B
    #sync_ack = sync_with_server_b(sql_message)
    #response += f" | Sync Status: {sync_ack}"
    return response


# --- Server Functions ---

# def start_server():
#     """Start Server C to listen for client connections and process SQL messages."""
#     create_database_if_not_exists()
#     create_table()

#     server_own_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server_own_socket.bind((update_server_host, update_server_port))
#     server_own_socket.listen(5)
#     print(f"Own Server listening on {update_server_host}:{update_server_port}...")

#     while True:
#         client_socket, client_address = server_own_socket.accept()
#         print(f"Connection from {client_address}")

#         # Receive SQL message
#         data = client_socket.recv(1024).decode()
#         print(f"Received SQL message: {data}")

#         if client_address == replica_server_host and data == "Switch Role":
#             response = "Ok, let's switch role"
#             client_socket.send(response.encode())
#             client_socket.close()
#             server_own_socket.close()
#             switch_role()

#         # Process the SQL message
#         response = process_sql_message(data)
#         client_socket.send(response.encode())
#         print(f"Sent response: {response}")

#         client_socket.close()


def handle_sigint(signum, frame):
    global shutdown_flag, server_socket
    print("Received SIGINT. Shutting down...")
    shutdown_flag = True
    if server_socket:
        print("Closing server socket and releasing port...")
        server_socket.close()


def start_server():

    create_database_if_not_exists()
    global shutdown_flag
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setblocking(False)
    server_socket.bind((HOST_OWN_SERVER, PORT_OWN_SERVER))
    server_socket.listen(5)
    print(f"Server listening on {server_socket.getsockname()}")

    poller = select.poll()
    poller.register(server_socket, select.POLLIN)
    # fd_to_socket = {server_socket.fileno(): server_socket}
    signal.signal(signal.SIGINT, handle_sigint)
    registered_sockets = {}

    try:
        while not shutdown_flag:
            events = poller.poll(1)
            for fileno, event in events:
                if fileno == server_socket.fileno():
                    client_socket, client_address = server_socket.accept()
                    print(f"Connection from {client_address}")
                    client_socket.setblocking(False)
                    poller.register(client_socket, select.POLLIN)
                    registered_sockets[client_socket] = True
                elif event == select.POLLIN:

                    print("Ready to handle client request")
                    client_socket = socket.fromfd(fileno, socket.AF_INET,
                                                  socket.SOCK_STREAM)
                    handle_client_request(client_socket)

                    # Safe unregistration and cleanup
                    if client_socket in registered_sockets and registered_sockets[
                            client_socket]:
                        poller.unregister(client_socket)
                        registered_sockets[
                            client_socket] = False  # Mark as unregistered
                    client_socket.close()

    except KeyboardInterrupt:
        print("Shutting down server.")
    finally:
        server_socket.close()


def handle_client_request(sock):
    """
    Handle a single client request:
    - Receive SQL message from the client.
    - Process the SQL message (execution and logging).
    - Sync with the replica server if the request is from the main server.
    - Send a response back to the client who made the request.
    """
    try:
        # Receive data from the client
        data = sock.recv(1024).decode()
        print(data)
        if not data:
            raise ConnectionResetError("Client disconnected")

        if data.startswith("Fail") or data.startswith("Ack") or data.startswith("TEST"):
            print(f"Received Message: {data}")
            sock.send(data.encode())
        else:
            print(f"Received Query: {data}")

            # Get client's IP and port
            client_ip, client_port = sock.getpeername()
            print(f"Request from IP: {client_ip}, Port: {client_port}")

            # Process the SQL message
            response = process_sql_message(data)

            # Check if the request is from the main server
            if client_ip == HOST_MAIN_SERVER:
                print(
                    f"Request from main server ({client_ip}), performing sync..."
                )
                # Sync with the replica server and include sync status in the response
                sync_status = sync_with_replica_server(data)
                response += f" | Sync: {sync_status}"
            else:
                print(
                    f"Request from other client ({client_ip}), no sync needed.")

            # Send the response back to the requesting client
            print("\n====================================\n")
            sock.send(response.encode())
    except Exception as e:
        print(f"Error handling client request: {e}")
        # Send error message back to the client
        error_response = f"Error: {str(e)}"
        sock.send(error_response.encode())
    finally:
        # Always close the socket after handling the request
        sock.close()


# def start_replica():
#     create_database_if_not_exists()
#     create_table()

#     server_own_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server_own_socket.bind((replica_server_host, replica_server_port))
#     server_own_socket.listen(5)
#     print(f"Own Server listening on {replica_server_host}:{replica_server_port}...")

#     while True:
#         client_socket, client_address = server_own_socket.accept()
#         print(f"Connection from {client_address}")

#         # Receive SQL message
#         data = client_socket.recv(1024).decode()
#         print(f"Received SQL message: {data}")

#         if data == "Sync":
#             replica_queries = get_queries()
#             client_socket.sendall(str(len(replica_queries)).encode())

#             # Receiving missing queriess
#             while True:
#                 data = client_socket.recv(1024).decode()
#                 if data == "Sync Complete":
#                     break
#                 # Process the SQL message
#                 response = process_sql_message(data)
#                 client_socket.send(response.encode())
#                 print(f"Sent response: {response}")
#             client_socket.close()
#             continue

#         # Process the SQL message
#         response = process_sql_message(data)
#         client_socket.send(response.encode())
#         print(f"Sent response: {response}")
#         client_socket.close()

#         if client_address == HOST_MAIN_SERVER:

#             # Send message to update server to switch role
#             try:
#                 client_socket.connect((update_server_host, update_server_port))
#                 print(f"Successcully Connect to {update_server_port}:{update_server_port}")

#                 message = "Switch Role"
#                 client_socket.sendall(message.encode())
#                 print(f"Send Message to Update Server: {message}")

#                 response = client_socket.recv(1024).decode()
#                 print(f"Receive Response frome Update Server: {response}")

#             except Exception as e:
#                 print(f"Exception: {e}")

#             finally:
#                 client_socket.close()
#                 server_own_socket.close()
#                 switch_role()

# --- Switch role between update server and replica ---
# def switch_role():
#     update_server_host, replica_server_host = replica_server_host, update_server_host
#     update_server_port, replica_server_port = replica_server_port, update_server_port

#     if is_update_server:
#         is_update_server = False
#         start_replica()
#     else:
#         is_update_server = True
#         start_server()

# --- Main Execution ---

if __name__ == "__main__":
    # if is_update_server:
    #     start_server()
    # else:
    #     start_replica()
    start_server()
