import sys
import pytest
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from master_server import parse_create_query, parse_insert_query, parse_select_query


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
        query = "INSERT INTO Persons (PersonID, LastName, FirstName, Address, State, City) VALUES (1, 'John', 'Doe', '123 Main St', 'CA', 'New York');"
        table_name, columns, values, sharding_id = parse_insert_query(query)
        assert table_name == "Persons"
        assert columns == "(PersonID, LastName, FirstName, Address, State, City)"
        assert values == "(1, 'John', 'Doe', '123 Main St', 'CA', 'New York')"
        assert sharding_id == 1

    def test_parse_insert_query_test_case_NY(self):
        query = "INSERT INTO Persons (PersonID, LastName, FirstName, Address, State, City) VALUES (1, 'John', 'Doe', '123 Main St', 'NY', 'New York');"
        table_name, columns, values, sharding_id = parse_insert_query(query)
        assert table_name == "Persons"
        assert columns == "(PersonID, LastName, FirstName, Address, State, City)"
        assert values == "(1, 'John', 'Doe', '123 Main St', 'NY', 'New York')"
        assert sharding_id == 0

    def test_parse_insert_query_test_case_can_raise_exception_1(self):
        with pytest.raises(Exception):
            query = "INSERT INTO Persons (PersonID, LastName, FirstName, Address, City) VALUES (1, 'John', 'Doe', '123 Main St', 'New York');"
            table_name, columns, values, sharding_id = parse_insert_query(query)
            assert table_name == "Persons"
            assert columns == "(PersonID, LastName, FirstName, Address, City)"
            assert values == "(1, 'John', 'Doe', '123 Main St', 'New York')"
            assert sharding_id == 0


class TestSelectQueryParser:

    def test_parse_select_query_test_case_1(self):
        query = "SELECT PersonID, LastName, FirstName, Address, City FROM Persons;"
        table_name, columns, sharding_id = parse_select_query(query)
        assert table_name == "Persons"
        assert columns == "PersonID, LastName, FirstName, Address, City"
        assert sharding_id == None

    def test_parse_select_query_test_case_1(self):
        query = "SELECT PersonID, LastName, City FROM Persons;"
        table_name, columns, sharding_id = parse_select_query(query)
        assert table_name == "Persons"
        assert columns == "PersonID, LastName, City"
        assert sharding_id == None

    def test_parse_select_query_test_case_2(self):
        query = "SELECT * FROM Persons;"
        table_name, columns, sharding_id = parse_select_query(query)
        assert table_name == "Persons"
        assert columns == "*"
        assert sharding_id == None

    def test_parse_select_query_test_case_with_where_CA(self):
        query = "SELECT * FROM Persons WHERE State = 'CA';"
        table_name, columns, sharding_id = parse_select_query(query)
        assert table_name == "Persons"
        assert columns == "*"
        assert sharding_id == 1

    def test_parse_select_query_test_case_with_where_NY(self):
        query = "SELECT * FROM Persons WHERE State = 'NY';"
        table_name, columns, sharding_id = parse_select_query(query)
        assert table_name == "Persons"
        assert columns == "*"
        assert sharding_id == 0
