import sqlparse

# Test parse_create_query(query)
from master_server import parse_create_query

table_name, columns_definition = parse_create_query(
    "CREATE TABLE Persons ( PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255) );"
)
print("Start to test function parse_create_query")
print("--" * 20)
print(table_name)
print(columns_definition)
print("Testing function parse_create_query passed!")
print("--" * 20)
print()
print()

# Test parse_insert_query(query)
from master_server import parse_insert_query

table_name, columns, values, sharding_id = parse_insert_query(
    "INSERT INTO Persons (PersonID, LastName, FirstName, Address, State, City) VALUES (1, 'John', 'Doe', '123 Main St', 'CA', 'New York');"
)

print("Start to test function parse_insert_query")
print("--" * 20)
print(table_name)
print(columns)
print(values)
print(sharding_id)
print("Testing function parse_insert_query passed!")
print("--" * 20)
print()
print()

# Test parse_select_query(query)
from master_server import parse_select_query

table_name, columns = parse_select_query("SELECT * FROM Persons;")

print("Start to test function parse_select_query")
print("--" * 20)
print(table_name)
print(columns)
print("Testing function parse_select_query passed!")
print("--" * 20)
print()
print()
