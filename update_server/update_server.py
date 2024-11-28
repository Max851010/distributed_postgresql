import socket
import psycopg2
from psycopg2 import sql
from collections import deque
import threading
import time
import select

# Update Server details
HOST_OWN_SERVER = '0.0.0.0'  # Bind to all interfaces
PORT_OWN_SERVER = 12347      # Port to listen on

# Replica Server details (replica server)
HOST_ANOTHER_SERVER = 'localhost'  # Change to Server B's address
PORT_ANOTHER_SERVER = 12348        # Change to Server B's port

# Main Server details
HOST_MAIN_SERVER = '...'
PORT_MAIN_SERVER = '...'

# PostgreSQL connection details
DB_NAME = "test_db"
DB_USER = "kenyang"
DB_PASSWORD = "ken890404"
DB_HOST = "localhost"
DB_PORT = "8888"
TABLE_NAME = "test1_table"

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
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            dbname=dbname,
            port=DB_PORT
        )
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
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
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
        conn = connect_postgres(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(sql_message)
        conn.commit()

        # Handle SELECT queries
        if sql_message.strip().lower().startswith("select"):
            results = cursor.fetchall()
            return f"Query Result: {results}"
        else:
            return "SQL operation executed successfully."
    except Exception as error:
        return f"Failed to execute SQL message: {error}"
    finally:
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
    global replica_node_status

    while True:
        try:
            replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_socket.connect((replica_server_host, replica_server_port))

            # Wait for acknowledgment
            acknowledgment = replica_socket.recv(1024).decode()
            replica_socket.close()
            print(f"Received acknowledgment from Server B: {acknowledgment}")
            print("Connected to Server B")

            break
        except Exception as error:
            print(f"Failed to sync with Server B: {error}")
        finally:
            time.sleep(5)

    
def sync_missing_queries():
    global missing_queries, replica_node_status

    while missing_queries:
        sql_message = missing_queries[0]

        print(f"Resyncing with Server B: {sql_message}")

        try:
            replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_socket.connect((replica_server_host, replica_server_port))
            replica_socket.send(sql_message.encode())

            # Wait for acknowledgment
            acknowledgment = replica_socket.recv(1024).decode()
            replica_socket.close()

            # Remove the query from the queue
            missing_queries.popleft()
            print(f"Received acknowledgment from Server B: {acknowledgment}")
        except Exception as error:
            print(f"Failed to resync with Server B: {error}")
            replica_node_status = "DOWN"
            check_replica_node_status()

def manage_missing_queries():
    global missing_queries, replica_node_status

    # Check the status of the replica node
    check_replica_node_status()

    missing_queries = deque([])

    with open(LOG_DIFF_FILE, "r") as log_file:
        lines = log_file.readlines()
        for line in lines:
            missing_queries.append(line.strip())

    replica_node_status = "RECOVERING"

    # resync missing queries with server B
    sync_missing_queries()
    replica_node_status = "RUNNING"


def sync_with_server_b(sql_message):
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
            replica_socket.connect((replica_server_host, replica_server_port))
            replica_socket.send(sql_message.encode())

            # Wait for acknowledgment
            acknowledgment = replica_socket.recv(1024).decode()
            replica_socket.close()
            print(f"Received acknowledgment from Server B: {acknowledgment}")
            replica_node_status = "RUNNING"
            if missing_query_manager and missing_query_manager.is_alive():
                missing_query_manager.join()
            return acknowledgment
        except Exception as error:
            replica_node_status = "DOWN"
            write_log_to_file(sql_message, LOG_DIFF_FILE)

            missing_query_manager = threading.Thread(target=manage_missing_queries)
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

def start_server():

    create_database_if_not_exists()
    create_table()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setblocking(False)
    server_socket.bind((update_server_host if is_update_server else replica_server_host, 
                        update_server_port if is_update_server else replica_server_port))
    server_socket.listen(5)
    print(f"Server listening on {server_socket.getsockname()}")

    poller = select.poll()
    poller.register(server_socket, select.POLLIN)
    fd_to_socket = {server_socket.fileno(): server_socket}

    try:
        while True:
            events = poller.poll()
            for fd, event in events:
                sock = fd_to_socket[fd]
                if sock is server_socket:
      
                    client_socket, addr = server_socket.accept()
                    print(f"New connection from {addr}")
                    client_socket.setblocking(False)
                    poller.register(client_socket, select.POLLIN)
                    fd_to_socket[client_socket.fileno()] = client_socket
                elif event & select.POLLIN:
   
                    handle_client_request(sock, poller, fd_to_socket)
    except KeyboardInterrupt:
        print("Shutting down server.")
    finally:
        server_socket.close()

def handle_client_request(sock, poller, fd_to_socket):
    try:
        data = sock.recv(1024).decode()
        if not data:
            raise ConnectionResetError("Client disconnected")
        print(f"Received SQL: {data}")

        client_ip, client_port = sock.getpeername()
        print(f"Request from IP: {client_ip}, Port: {client_port}")

        response = process_sql_message(data)

        if client_ip == update_server_host:
            print(f"Request from main server ({client_ip}), performing sync...")
            sync_status = sync_with_server_b(data)
            response += f" | Sync: {sync_status}"
        else:
            print(f"Request from other client ({client_ip}), no sync needed.")

        sock.send(response.encode())
    except Exception as e:
        print(f"Error handling client request: {e}")
    finally:
        poller.unregister(sock)
        sock.close()
        del fd_to_socket[sock.fileno()]


def start_replica():
    create_database_if_not_exists()
    create_table()

    server_own_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_own_socket.bind((replica_server_host, replica_server_port))
    server_own_socket.listen(5)
    print(f"Own Server listening on {replica_server_host}:{replica_server_port}...")

    while True:
        client_socket, client_address = server_own_socket.accept()
        print(f"Connection from {client_address}")

        # Receive SQL message
        data = client_socket.recv(1024).decode()
        print(f"Received SQL message: {data}")

        if data == "Sync":
            replica_queries = get_queries()
            client_socket.sendall(str(len(replica_queries)).encode())

            # Receiving missing queriess
            while True:
                data = client_socket.recv(1024).decode()
                if data == "Sync Complete":
                    break
                # Process the SQL message
                response = process_sql_message(data)
                client_socket.send(response.encode())
                print(f"Sent response: {response}")
            client_socket.close()
            continue

        # Process the SQL message
        response = process_sql_message(data)
        client_socket.send(response.encode())
        print(f"Sent response: {response}")
        client_socket.close()

        if client_address == HOST_MAIN_SERVER:

            # Send message to update server to switch role
            try:
                client_socket.connect((update_server_host, update_server_port))
                print(f"Successcully Connect to {update_server_port}:{update_server_port}")
                
                message = "Switch Role"
                client_socket.sendall(message.encode())
                print(f"Send Message to Update Server: {message}")
               
                response = client_socket.recv(1024).decode()
                print(f"Receive Response frome Update Server: {response}")

            except Exception as e:
                print(f"Exception: {e}")

            finally:
                client_socket.close()
                server_own_socket.close()
                switch_role()
        


# --- Switch role between update server and replica ---
def switch_role():
    update_server_host, replica_server_host = replica_server_host, update_server_host
    update_server_port, replica_server_port = replica_server_port, update_server_port
    
    if is_update_server:
        is_update_server = False
        start_replica()
    else:
        is_update_server = True
        start_server()


# --- Main Execution ---

if __name__ == "__main__":
    # if is_update_server:
    #     start_server()
    # else:
    #     start_replica()
    start_server()
    
