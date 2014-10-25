import time
import pytest
import uuid

from schema.schema import Schema, Index
from schema.dynamodb import IndexDynamo
from schema.dynamodb import TableDynamo
from schema.dynamodb import IndexDynamo
from schema.fields import Field
from tests.fixtures import dynamodb


@pytest.fixture
def table_name():
    return str(uuid.uuid1())[:8]


def test_create_dynamo_schema(table_name):
    table = TableDynamo(table_name, dynamodb.connection)
    primary_index = IndexDynamo(Index.PRIMARY, 'id')
    global_index = IndexDynamo(Index.GLOBAL, 'foo', 'bar')
    tb = Schema(table, primary_index, global_index)

    assert tb.table == table
    assert tb.indexes[0] == primary_index
    assert tb.indexes[1] == global_index


def test_dynamo_table_creation(table_name):
    tb = Schema(
        TableDynamo(table_name, dynamodb.connection),
        IndexDynamo(
            Index.PRIMARY, 'id', read_capacity=8, write_capacity=4),
        id=Field())

    tb.table.create_table()
    assert table_name in dynamodb.connection.list_tables().get('TableNames')


def test_dynamo_table_creation_collision(table_name):
    tb = Schema(
        TableDynamo(table_name, dynamodb.connection),
        IndexDynamo(
            Index.PRIMARY, 'id', read_capacity=8, write_capacity=2),
        id=Field())
    tb.table.create_table()

    with pytest.raises(Exception):
        tb.table.create_table()


def test_create_empty_table(table_name):
    """Test the creation of an empty table.
    """
    tb = Schema(TableDynamo(table_name, dynamodb.connection))

    with pytest.raises(Exception):
        tb.table.create_table()


def test_create_table_without_index(table_name):
    tb = Schema(TableDynamo(table_name, dynamodb.connection), id=Field())
    with pytest.raises(Exception):
        tb.table.create_table()


def test_create_table_without_fields(table_name):
    tb = Schema(
        TableDynamo(table_name, dynamodb.connection),
        IndexDynamo(Index.PRIMARY, 'id', read_capacity=1, write_capacity=1))

    with pytest.raises(Exception):
        tb.table.create_table()
    
