import socket
import psycopg2
from psycopg2 import sql

# Update Server details
HOST_OWN_SERVER = '0.0.0.0'  # Bind to all interfaces
PORT_OWN_SERVER = 12347      # Port to listen on

# Replica Server details (replica server)
HOST_ANOTHER_SERVER = 'localhost'  # Change to Server B's address
PORT_ANOTHER_SERVER = 12348        # Change to Server B's port

# Main Server details
HOST_MAIN_SERVER = '...'
PORT_MAIN_Server = '...'

# PostgreSQL connection details
DB_NAME = "test_db"
DB_USER = "kenyang"
DB_PASSWORD = "ken890404"
DB_HOST = "localhost"
DB_PORT = "8888"
TABLE_NAME = "test1_table"

# Log file name
LOG_FILE = "sql_log.txt"


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

def get_queries():
    """Reads queries from log.txt and returns them as a list."""
    file_path = LOG_FILE
    try:
        with open(file_path, "r") as file:
            return file.readlines()
    except FileNotFoundError:
        return []

def append_queries(queries):
    """Appends missing queries to the log.txt file."""
    file_path = LOG_FILE
    with open(file_path, "a") as file:
        file.writelines(queries)

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

def write_log_to_file(sql_message):
    """Write the SQL operation to the log file."""
    try:
        with open(LOG_FILE, "a") as log_file:
            log_file.write(sql_message + "\n")
        print("SQL operation logged successfully.")
    except Exception as error:
        print(f"Failed to write to log file: {error}")


# --- Replica Sync Functions ---

def sync_with_server_b(sql_message):
    """Send the SQL message to replica server B and wait for acknowledgment."""
    try:
        replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        replica_socket.connect((replica_server_host, replica_server_port))
        replica_socket.send(sql_message.encode())

        # Wait for acknowledgment
        acknowledgment = replica_socket.recv(1024).decode()
        replica_socket.close()
        print(f"Received acknowledgment from Server B: {acknowledgment}")
        return acknowledgment
    except Exception as error:
        print(f"Failed to sync with Server B: {error}")
        return f"Failed: {error}"

def sync_with_replica():
    try:
        replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        replica_socket.connect((replica_server_host, replica_server_port))
        message = "Sync"
        replica_socket.send(message.encode())

        # Wait for Response
        replica_count = replica_socket.recv(1024).decode()
        replica_socket.close()
        print(f"Received response from Server B: {replica_count}")
    except Exception as error:
        print(f"Failed to send Sync with Replica: {error}")
        return f"Failed: {error}"

    update_queries = get_queries()
    missing_queries = update_queries[replica_count:]
    
    # Start Sending Missing Queries
    print(f"Sending {len(missing_queries)} missing queries to replica")
    for query in missing_queries:
        replica_socket.sendall(query.encode())
        # Wait for acknowledgment
        acknowledgment = replica_socket.recv(1024).decode()
        print(f"Received acknowledgment from Replica: {acknowledgment}")
    replica_socket.sendall(b"Sync Complete")
    replica_socket.close()
    

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
    write_log_to_file(sql_message)

    # Step 3: Sync with Server B
    sync_ack = sync_with_server_b(sql_message)
    response += f" | Sync Status: {sync_ack}"
    return response


# --- Server Functions ---

def start_server():
    """Start Server C to listen for client connections and process SQL messages."""
    create_database_if_not_exists()
    create_table()
    sync_with_replica()

    server_own_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_own_socket.bind((update_server_host, update_server_port))
    server_own_socket.listen(5)
    print(f"Own Server listening on {update_server_host}:{update_server_port}...")

    while True:
        client_socket, client_address = server_own_socket.accept()
        print(f"Connection from {client_address}")

        # Receive SQL message
        data = client_socket.recv(1024).decode()
        print(f"Received SQL message: {data}")

        if client_address == replica_server_host and data == "Switch Role":
            response = "Ok, let's switch role"
            client_socket.send(response.encode())
            client_socket.close()
            switch_role()

        # Process the SQL message
        response = process_sql_message(data)
        sync_ack = sync_with_replica()
        response += f" | Sync Status: {sync_ack}"

        client_socket.send(response.encode())
        print(f"Sent response: {response}")

        client_socket.close()



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
    if is_update_server:
        start_server()
    start_replica()
    