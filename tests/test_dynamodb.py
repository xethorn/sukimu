import time
import pytest
import uuid

from schema import exceptions
from schema import response
from schema.dynamodb import IndexDynamo
from schema.dynamodb import IndexDynamo
from schema.dynamodb import TableDynamo
from schema.fields import Field
from schema.operations import Equal
from schema.operations import In
from schema.schema import Schema, Index
from tests.fixtures import dynamodb


@pytest.fixture
def table_name():
    return str(uuid.uuid1())[:8]


@pytest.fixture
def user_schema():
    schema = Schema(
        TableDynamo(table_name(), dynamodb.connection),

        IndexDynamo(
            Index.PRIMARY, 'id', read_capacity=8, write_capacity=2),
        IndexDynamo(
            Index.GLOBAL, 'username', read_capacity=8, write_capacity=2,
            name='username-index'),

        id=Field(),
        username=Field(),
        password=Field(),
        randomfield=Field(basetype=int))
    schema.table.create_table()
    return schema


@pytest.fixture
def thread_schema():
    schema = Schema(
        TableDynamo(table_name(), dynamodb.connection),
        IndexDynamo(Index.PRIMARY, 'forum_name', 'thread_title',
                    read_capacity=8, write_capacity=2),
        IndexDynamo(Index.LOCAL, 'forum_name', 'thread_author',
                    name='local-index'),
        IndexDynamo(Index.GLOBAL, 'thread_title', 'thread_author',
                    name='global-index', read_capacity=8, write_capacity=2),
        forum_name=Field(),
        thread_title=Field(),
        thread_author=Field(),
        thread_content=Field())
    schema.table.create_table()
    return schema


def test_can_create_fixtures(user_schema, thread_schema):
    pass


def test_create_an_entry_with_wrong_field(user_schema):
    resp = user_schema.create(id='30', username='michael', randomfield='test')
    assert not resp.success
    assert isinstance(
        resp.errors.get('randomfield'), exceptions.FieldException)

    resp = user_schema.fetch_one(id=Equal('30'))
    assert not resp.success
    assert resp.status is response.Status.NOT_FOUND


def test_create_an_entry_for_user(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp.success


def test_update_an_entry_for_user(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp.success

    resp = user_schema.update(dict(id='30'), username='joe')
    assert resp.success


def test_delete_an_entry_for_user(user_schema):
    user_schema.create(id='30', username='michael')
    user_schema.create(id='40', username='michael2')

    resp = user_schema.delete(id=Equal('30'))
    assert resp.success

    resp = user_schema.fetch_one(id=30)
    assert not resp.success


def test_update_an_entry_on_existing_key(user_schema):
    user_schema.create(id='40', username='michael')
    user_schema.create(id='30', username='joe')

    resp = user_schema.update(dict(id='30'), username='michael')
    assert not resp.success
    assert isinstance(resp.errors.get('username'), exceptions.FieldException)

    resp = user_schema.fetch_one(id=Equal('30'))
    assert resp.username == 'joe'


def test_create_an_entry_on_existing_user_id(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp.success

    resp = user_schema.create(id='30', username='otherusername')
    assert not resp.success
    assert not resp.errors.get('username')
    assert resp.status is response.Status.FIELD_VALUE_ALREADY_USED
    assert isinstance(resp.errors.get('id'), exceptions.FieldException)


def test_create_an_entry_on_existing_user_username(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp.success

    resp = user_schema.create(id='20', username='michael')
    assert not resp.success
    assert not resp.errors.get('id')
    assert resp.status is response.Status.FIELD_VALUE_ALREADY_USED
    assert isinstance(resp.errors.get('username'), exceptions.FieldException)


def test_create_an_entry_on_existing_user_username_and_id(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp.success

    resp = user_schema.create(id='20', username='michael')
    assert not resp.success
    assert not resp.errors.get('id')
    assert resp.status is response.Status.FIELD_VALUE_ALREADY_USED
    assert isinstance(resp.errors.get('username'), exceptions.FieldException)


def test_thread_creation(thread_schema):
    resp = thread_schema.create(
        forum_name='News', thread_title='title', thread_author='user',
        thread_content='content')
    assert resp.success

    resp = thread_schema.fetch_one(
        forum_name=Equal('News'), thread_title=Equal('title'))
    assert resp.success
    assert resp.thread_author == 'user'

    resp = thread_schema.fetch_one(
        forum_name=Equal('News'), thread_author=Equal('user'))
    assert resp.success
    assert resp.thread_title == 'title'

    resp = thread_schema.fetch_one(
        thread_title=Equal('title'), thread_author=Equal('user'))
    assert resp.success
    assert resp.forum_name == 'News'

    resp = thread_schema.create(
        forum_name='Updates', thread_title='Title2', thread_author='user',
        thread_content='content')
    assert resp.success

    resp = thread_schema.create(
        forum_name='Updates', thread_title='Title3', thread_author='user2',
        thread_content='content')
    assert resp.success

    resp = thread_schema.create(
        forum_name='Others', thread_title='Title', thread_author='user4',
        thread_content='foobar')
    assert resp.success


def test_thread_creation_on_duplicate_indexes(thread_schema):
    # Indexes:
    # - Forum Name and Thread Title
    # - Forum Name - Author
    # - Forum Title - Author
    resp = thread_schema.create(
        forum_name='News', thread_title='title', thread_author='user',
        thread_content='content')
    assert resp.success

    resp = thread_schema.create(
        forum_name='News', thread_title='title', thread_author='user2',
        thread_content='content')
    assert not resp.success
    assert resp.errors.get('forum_name')
    assert resp.errors.get('thread_title')

    resp = thread_schema.create(
        forum_name='News', thread_title='title2', thread_author='user',
        thread_content='content')
    assert not resp.success
    assert resp.errors.get('thread_author')
    assert resp.errors.get('forum_name')

    resp = thread_schema.create(
        forum_name='Other', thread_title='title', thread_author='user',
        thread_content='content')
    assert not resp.success
    assert resp.errors.get('thread_title')
    assert resp.errors.get('thread_author')


def test_create_dynamo_schema(table_name):
    table = TableDynamo(table_name, dynamodb.connection)
    primary_index = IndexDynamo(Index.PRIMARY, 'id')
    global_index = IndexDynamo(Index.GLOBAL, 'foo', 'bar')
    tb = Schema(table, primary_index, global_index)

    assert tb.table == table
    assert tb.indexes[0] == primary_index
    assert tb.indexes[1] == global_index


def test_fetch_on_index(thread_schema):
    resp = thread_schema.create(
        forum_name='News', thread_title='title', thread_author='user',
        thread_content='content')
    assert resp.success

    resp = thread_schema.fetch(
        forum_name=Equal('News'), thread_title=Equal('title'))
    assert resp.success
    assert resp.message[0].get('thread_author') == 'user'

    resp = thread_schema.fetch(
        thread_title=Equal('title'), thread_author=Equal('user'))
    assert resp.success
    assert resp.message[0].get('forum_name') == 'News'


def test_fetch_many(user_schema):
    user_schema.create(id='30', username='michael1')
    user_schema.create(id='40', username='michael2')
    resp = user_schema.fetch(username=In('michael1', 'michael2'))
    assert resp.success
    assert len(resp.message) == 2


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
