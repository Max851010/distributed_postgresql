import socket
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Server C details
HOST_C = '0.0.0.0'  # Bind to all interfaces
PORT_C = 12347      # Port to listen on

# PostgreSQL connection details
DB_NAME = "test_db"
DB_USER = "kenyang"
DB_PASSWORD = "ken890404"
DB_HOST = "localhost"
DB_PORT = "8888"


def create_database(dbname):
# """Connect to the PostgreSQL by calling connect_postgres() function
# Create a database named {DATABASE_NAME}
# Close the connection"""
    connection = connect_postgres()
    
    if connection is None:
        return

    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE {}".format(dbname))
        print(f"Database '{dbname}' created successfully!")

    except Exception as error:
        print(f"Failed to create database: {error}")

    # finally:
    #     if connection:
    #         cursor.close()
    #         connection.close()
    #         print("Default PostgreSQL connection closed.")

def connect_postgres(dbname="postgres"):
# """Connect to the PostgreSQL using psycopg2 with default database
# Return the connection"""
    try:
        
        connection = psycopg2.connect(
            user="kenyang",
            password="ken890404", 
            host="localhost",
            dbname=dbname,
            port='8888'
        )
        connection.autocommit = True 
        print("Connected to PostgreSQL!")
        return connection

    except Exception as error:
        print(f"Failed to connect to PostgreSQL: {error}")
        return None


def create_table():
    """Create the 'test1_table' table in the database if it doesn't exist."""
    try:
        conn = connect_postgres(DB_NAME)
        cursor = conn.cursor()

        # Create the table 'test1_table'
        create_table_query = """
        CREATE TABLE IF NOT EXISTS test1_table (
            id SERIAL PRIMARY KEY,
            message TEXT NOT NULL,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'test1_table' is ready.")
    except Exception as e:
        print(f"Failed to create table: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def delete_database(dbname):

    connection = connect_postgres()

    if connection is None:
        return

    try:
        cursor = connection.cursor()


        cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname)))
        print(f"資料庫 '{dbname}' 刪除成功！")

    except Exception as error:
        print(f"刪除資料庫失敗: {error}")


def insert_sample_data():
    """Insert 10 sample car data into 'test1_table'."""
    try:
        conn = connect_postgres(DB_NAME)
        cursor = conn.cursor()
        sample_data = [
            ("Car 1", "Description of Car 1"),
            ("Car 2", "Description of Car 2"),
            ("Car 3", "Description of Car 3"),
            ("Car 4", "Description of Car 4"),
            ("Car 5", "Description of Car 5"),
            ("Car 6", "Description of Car 6"),
            ("Car 7", "Description of Car 7"),
            ("Car 8", "Description of Car 8"),
            ("Car 9", "Description of Car 9"),
            ("Car 10", "Description of Car 10"),
        ]
        insert_query = """
        INSERT INTO test1_table (message) VALUES (%s);
        """
        for car in sample_data:
            cursor.execute(insert_query, (f"{car[0]} - {car[1]}",))
        print("Inserted 10 sample rows into 'test1_table'.")
    except Exception as e:
        print(f"Failed to insert sample data: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def process_sql_message(sql_message):
    """Parse and execute the received SQL message."""
    try:
        conn = connect_postgres(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(sql_message)
        conn.commit()
        
        # Fetch results if it's a SELECT query
        if sql_message.strip().lower().startswith("select"):
            results = cursor.fetchall()
            return f"Query Result: {results}"
        return "SQL operation executed successfully."
    except Exception as e:
        return f"Failed to execute SQL message: {e}"
    finally:
        if conn:
            cursor.close()
            conn.close()



# Main execution
# create_database(DB_NAME)  # Step 1: Create database if not exists
# delete_database(DB_NAME)
create_table()     # Step 2: Ensure the table exists in the database
insert_sample_data()



# # Step 4: Connect to the created database
# try:
#     conn = connect_postgres(DB_NAME)
#     cursor = conn.cursor()
#     print("Connected to PostgreSQL database.")
# except Exception as e:
#     print(f"Error connecting to the database: {e}")
#     exit()


# Start the server
server_c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_c_socket.bind((HOST_C, PORT_C))
server_c_socket.listen(5)
print(f"Server C listening on {HOST_C}:{PORT_C}...")

while True:
    client_socket, client_address = server_c_socket.accept()
    print(f"Connection from {client_address}")
    
    # Receive SQL message
    data = client_socket.recv(1024).decode()
    print(f"Received SQL message: {data}")
    
    # Process the SQL message and generate a response
    response = process_sql_message(data)
    client_socket.send(response.encode())
    print(f"Sent response: {response}")
    
    client_socket.close()

# Close connections when the server shuts down
cursor.close()
conn.close()
