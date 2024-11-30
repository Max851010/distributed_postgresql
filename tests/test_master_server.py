import sys
import pytest
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from master_server.master_server import parse_create_query, parse_insert_query, parse_select_query


class TestCreateQueryParser:

    def test_parse_create_query_test_case_1(self):
        query = "CREATE TABLE Persons ( PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), State varchar(255), City varchar(255) );"
        table_name, columns_definition = parse_create_query(query)
        assert table_name == "Persons"
        assert columns_definition == "( PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), State varchar(255), City varchar(255) )"

    def test_parse_create_query_test_case_2(self):
        query = "CREATE TABLE Persons1 (PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), State varchar(255), City varchar(255));"
        table_name, columns_definition = parse_create_query(query)
        assert table_name == "Persons1"
        assert columns_definition == "(PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), State varchar(255), City varchar(255))"

    def test_parse_create_query_test_case_can_raise_exception_1(self):
        with pytest.raises(Exception):
            query = "CREATE TABLE Persons ( PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255) );"
            table_name, columns_definition = parse_create_query(query)
            assert table_name == "Persons"
            assert columns_definition == "( PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255) )"

    def test_parse_create_query_test_case_can_raise_exception_2(self):
        with pytest.raises(Exception):
            query = "CREATE TABLE Persons1 (PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255));"
            table_name, columns_definition = parse_create_query(query)
            assert table_name == "Persons1"
            assert columns_definition == "(PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255))"


class TestInsertQueryParser:

    def test_parse_insert_query_test_case_CA(self):
        query = "INSERT INTO Persons (PersonID, LastName, FirstName, Address, State, City) VALUES (1, 'John', 'Doe', '123 Main St', 'CA', 'New York'), (2, 'Jane', 'Smith', '456 Elm St', 'AZ', 'Los Angeles'), (3, 'Emily', 'Jones', '789 Oak St', 'CA', 'San Francisco'), (4, 'Michael', 'Brown', '101 Pine St', 'AZ', 'Phoenix'), (5, 'Chris', 'Davis', '202 Maple St', 'CA', 'San Diego'), (6, 'Pat', 'Johnson', '303 Cedar St', 'AZ', 'Tucson'), (7, 'Taylor', 'Wilson', '404 Birch St', 'CA', 'Sacramento'), (8, 'Jordan', 'Lee', '505 Spruce St', 'AZ', 'Flagstaff'), (9, 'Alex', 'White', '606 Palm St', 'CA', 'Fresno'), (10, 'Morgan', 'Miller', '707 Willow St', 'AZ', 'Mesa');"
        table_name, columns, values_shard_0, values_shard_1 = parse_insert_query(
            query)
        assert table_name == "Persons"
        assert columns == "(PersonID, LastName, FirstName, Address, State, City)"
        assert values_shard_0 == "(2, 'Jane', 'Smith', '456 Elm St', 'AZ', 'Los Angeles'), (4, 'Michael', 'Brown', '101 Pine St', 'AZ', 'Phoenix'), (6, 'Pat', 'Johnson', '303 Cedar St', 'AZ', 'Tucson'), (8, 'Jordan', 'Lee', '505 Spruce St', 'AZ', 'Flagstaff'), (10, 'Morgan', 'Miller', '707 Willow St', 'AZ', 'Mesa')"
        assert values_shard_1 == "(1, 'John', 'Doe', '123 Main St', 'CA', 'New York'), (3, 'Emily', 'Jones', '789 Oak St', 'CA', 'San Francisco'), (5, 'Chris', 'Davis', '202 Maple St', 'CA', 'San Diego'), (7, 'Taylor', 'Wilson', '404 Birch St', 'CA', 'Sacramento'), (9, 'Alex', 'White', '606 Palm St', 'CA', 'Fresno')"

    def test_parse_insert_query_test_case_NY(self):
        query = "INSERT INTO Persons (PersonID, LastName, FirstName, Address, State, City) VALUES (1, 'John', 'Doe', '123 Main St', 'CA', 'New York'), (2, 'Jane', 'Smith', '456 Elm St', 'AZ', 'Los Angeles'), (3, 'Emily', 'Jones', '789 Oak St', 'CA', 'San Francisco'), (4, 'Michael', 'Brown', '101 Pine St', 'AZ', 'Phoenix'), (5, 'Chris', 'Davis', '202 Maple St', 'CA', 'San Diego'), (6, 'Pat', 'Johnson', '303 Cedar St', 'AZ', 'Tucson'), (7, 'Taylor', 'Wilson', '404 Birch St', 'CA', 'Sacramento'), (8, 'Jordan', 'Lee', '505 Spruce St', 'AZ', 'Flagstaff'), (9, 'Alex', 'White', '606 Palm St', 'CA', 'Fresno'), (10, 'Morgan', 'Miller', '707 Willow St', 'AZ', 'Mesa');"
        table_name, columns, values_shard_0, values_shard_1 = parse_insert_query(
            query)
        assert table_name == "Persons"
        assert columns == "(PersonID, LastName, FirstName, Address, State, City)"
        assert values_shard_0 == "(2, 'Jane', 'Smith', '456 Elm St', 'AZ', 'Los Angeles'), (4, 'Michael', 'Brown', '101 Pine St', 'AZ', 'Phoenix'), (6, 'Pat', 'Johnson', '303 Cedar St', 'AZ', 'Tucson'), (8, 'Jordan', 'Lee', '505 Spruce St', 'AZ', 'Flagstaff'), (10, 'Morgan', 'Miller', '707 Willow St', 'AZ', 'Mesa')"
        assert values_shard_1 == "(1, 'John', 'Doe', '123 Main St', 'CA', 'New York'), (3, 'Emily', 'Jones', '789 Oak St', 'CA', 'San Francisco'), (5, 'Chris', 'Davis', '202 Maple St', 'CA', 'San Diego'), (7, 'Taylor', 'Wilson', '404 Birch St', 'CA', 'Sacramento'), (9, 'Alex', 'White', '606 Palm St', 'CA', 'Fresno')"


# def test_parse_insert_query_test_case_can_raise_exception_1(self):
#     with pytest.raises(Exception):
#         query = "INSERT INTO Persons (PersonID, LastName, FirstName, Address, City) VALUES (1, 'John', 'Doe', '123 Main St', 'New York');"
#         table_name, columns, values, sharding_id = parse_insert_query(query)
#         assert table_name == "Persons"
#         assert columns == "(PersonID, LastName, FirstName, Address, City)"
#         assert values == "(1, 'John', 'Doe', '123 Main St', 'New York')"
#         assert sharding_id == 0


class TestSelectQueryParser:

    def test_parse_select_query_test_case_1(self):
        query = "SELECT PersonID, LastName, FirstName, Address, City FROM Persons;"
        table_name, columns, sharding_id, where_condition = parse_select_query(
            query)
        assert table_name == "Persons"
        assert columns == "PersonID, LastName, FirstName, Address, City"
        assert sharding_id == None
        assert where_condition == None

    def test_parse_select_query_test_case_1(self):
        query = "SELECT PersonID, LastName, City FROM Persons;"
        table_name, columns, sharding_id, where_condition = parse_select_query(
            query)
        assert table_name == "Persons"
        assert columns == "PersonID, LastName, City"
        assert sharding_id == None
        assert where_condition == None

    def test_parse_select_query_test_case_2(self):
        query = "SELECT * FROM Persons;"
        table_name, columns, sharding_id, where_condition = parse_select_query(
            query)
        assert table_name == "Persons"
        assert columns == "*"
        assert sharding_id == None
        assert where_condition == None

    def test_parse_select_query_test_case_with_where_CA(self):
        query = "SELECT * FROM Persons WHERE State = 'CA';"
        table_name, columns, sharding_id, where_condition = parse_select_query(
            query)
        assert table_name == "Persons"
        assert columns == "*"
        assert sharding_id == 1
        assert where_condition == "WHERE State = 'CA';"

    def test_parse_select_query_test_case_with_where_NY(self):
        query = "SELECT * FROM Persons WHERE State = 'NY';"
        table_name, columns, sharding_id, where_condition = parse_select_query(
            query)
        assert table_name == "Persons"
        assert columns == "*"
        assert sharding_id == 0
        assert where_condition == "WHERE State = 'NY';"
