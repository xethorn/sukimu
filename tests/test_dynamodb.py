import time
import pytest
import uuid
from random import random
from random import shuffle

from oto import response
from oto import status

from sukimu import consts
from sukimu import exceptions
from sukimu.dynamodb import IndexDynamo
from sukimu.dynamodb import IndexDynamo
from sukimu.dynamodb import TableDynamo
from sukimu.fields import Field
from sukimu.operations import Between
from sukimu.operations import Equal
from sukimu.operations import In
from sukimu.schema import Schema, Index
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
        map_field=Field(basetype=dict),
        random_field=Field(basetype=int))
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


@pytest.fixture
def stats_schema():
    schema = Schema(
        TableDynamo(table_name(), dynamodb.connection),
        IndexDynamo(Index.PRIMARY, 'user_id', 'day_id',
                    read_capacity=8, write_capacity=2),
        user_id=Field(basetype=int),
        day_id=Field(basetype=int),
        metrics=Field(basetype=int))
    schema.table.create_table()
    return schema


@pytest.fixture
def stats_reverse_schema():
    schema = Schema(
        TableDynamo(table_name(), dynamodb.connection),
        IndexDynamo(Index.PRIMARY, 'user_id', 'day_id',
                    read_capacity=8, write_capacity=2),
        IndexDynamo(Index.GLOBAL, 'day_id', 'user_id',
                    name='day-id-user-id', read_capacity=8, write_capacity=2),
        user_id=Field(basetype=int),
        day_id=Field(basetype=int))
    schema.table.create_table()
    return schema


def test_can_create_fixtures(user_schema, thread_schema):
    pass


def test_create_an_entry_with_wrong_field(user_schema):
    resp = user_schema.create(id='30', username='michael', random_field='test')
    assert not resp
    assert isinstance(resp.errors.get('message').get('random_field'), str)

    resp = user_schema.fetch_one(id=Equal('30'))
    assert not resp
    assert resp.status is status.NOT_FOUND


def test_extension(user_schema):
    new_schema = user_schema.extends(
        new_field=Field())
    assert isinstance(new_schema, Schema)
    assert new_schema.table.name == user_schema.table.name
    assert len(new_schema.indexes) == len(user_schema.indexes)
    assert len(new_schema.fields) - 1 == len(user_schema.fields)
    assert not new_schema.table.schema == user_schema


def test_create_an_entry_for_user(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp


def test_update_an_entry_for_user(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp

    resp = user_schema.update(dict(id='30'), username='joe')
    assert resp


def test_delete_an_entry_for_user(user_schema):
    user_schema.create(id='30', username='michael')
    user_schema.create(id='40', username='michael2')

    resp = user_schema.delete(id=Equal('30'))
    assert resp

    resp = user_schema.fetch_one(id=Equal(30))
    assert not resp


def test_update_an_entry_on_existing_key(user_schema):
    user_schema.create(id='.40', username='michael')
    user_schema.create(id='30', username='joe')

    resp = user_schema.update(dict(id='30'), username='michael')
    assert not resp
    assert isinstance(
        resp.errors.get('message').get('username'), exceptions.FieldException)

    resp = user_schema.fetch_one(id=Equal('30'))
    assert resp.message.get('username') == 'joe'


def test_create_an_entry_on_existing_user_id(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp

    resp = user_schema.create(id='30', username='otherusername')
    assert not resp
    assert not resp.errors.get('message').get('username')
    assert resp.errors.get('code') is consts.ERROR_CODE_DUPLICATE_KEY
    assert isinstance(
        resp.errors.get('message').get('id'), exceptions.FieldException)


def test_create_an_entry_with_map_data(user_schema):
    resp = user_schema.create(
        id='190',
        username='emichael90',
        map_field=dict(
            key1='value',
            key2=dict(
                key1='value')))
    assert resp

    resp = user_schema.fetch_one(id=Equal('190'))
    assert resp
    assert isinstance(resp.message.get('map_field'), dict)


def test_create_an_entry_on_existing_user_username(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp

    resp = user_schema.create(id='20', username='michael')
    assert not resp
    assert not resp.errors.get('message').get('id')
    assert resp.errors.get('code') is consts.ERROR_CODE_DUPLICATE_KEY
    assert isinstance(
        resp.errors.get('message').get('username'), exceptions.FieldException)


def test_create_an_entry_on_existing_user_username_and_id(user_schema):
    resp = user_schema.create(id='30', username='michael')
    assert resp

    resp = user_schema.create(id='20', username='michael')
    assert not resp
    assert not resp.errors.get('message').get('id')
    assert resp.errors.get('code') is consts.ERROR_CODE_DUPLICATE_KEY
    assert isinstance(
        resp.errors.get('message').get('username'), exceptions.FieldException)


def test_thread_creation(thread_schema):
    resp = thread_schema.create(
        forum_name='News', thread_title='title', thread_author='user',
        thread_content='content')
    assert resp

    resp = thread_schema.fetch_one(
        forum_name=Equal('News'), thread_title=Equal('title'))
    assert resp
    assert resp.message.get('thread_author') == 'user'

    resp = thread_schema.fetch_one(
        forum_name=Equal('News'), thread_author=Equal('user'))
    assert resp
    assert resp.message.get('thread_title') == 'title'

    resp = thread_schema.fetch_one(
        thread_title=Equal('title'), thread_author=Equal('user'))
    assert resp
    assert resp.message.get('forum_name') == 'News'

    resp = thread_schema.create(
        forum_name='Updates', thread_title='Title2', thread_author='user',
        thread_content='content')
    assert resp

    resp = thread_schema.create(
        forum_name='Updates', thread_title='Title3', thread_author='user2',
        thread_content='content')
    assert resp

    resp = thread_schema.create(
        forum_name='Others', thread_title='Title', thread_author='user4',
        thread_content='foobar')
    assert resp


def test_thread_creation_on_duplicate_indexes(thread_schema):
    # Indexes:
    # - Forum Name and Thread Title
    # - Forum Name - Author
    # - Forum Title - Author
    resp = thread_schema.create(
        forum_name='News', thread_title='title', thread_author='user',
        thread_content='content')
    assert resp

    resp = thread_schema.create(
        forum_name='News', thread_title='title', thread_author='user2',
        thread_content='content')
    assert not resp
    assert resp.errors.get('message').get('forum_name')
    assert resp.errors.get('message').get('thread_title')

    resp = thread_schema.create(
        forum_name='News', thread_title='title2', thread_author='user',
        thread_content='content')
    assert not resp
    assert resp.errors.get('message').get('thread_author')
    assert resp.errors.get('message').get('forum_name')

    resp = thread_schema.create(
        forum_name='Other', thread_title='title', thread_author='user',
        thread_content='content')
    assert not resp
    assert resp.errors.get('message').get('thread_title')
    assert resp.errors.get('message').get('thread_author')


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
    assert resp

    resp = thread_schema.fetch(
        forum_name=Equal('News'), thread_title=Equal('title'))
    assert resp
    assert resp.message[0].get('thread_author') == 'user'

    resp = thread_schema.fetch(
        thread_title=Equal('title'), thread_author=Equal('user'))
    assert resp
    assert resp.message[0].get('forum_name') == 'News'


def test_fetch_many(user_schema):
    user_schema.create(id='30', username='michael1')
    user_schema.create(id='40', username='michael2')
    resp = user_schema.fetch(username=In('michael1', 'michael2'))
    assert resp
    assert len(resp.message) == 2


def test_between_request(stats_schema):
    stats_schema.create(user_id=301, day_id=35, metrics=937)

    for day in range(50):
        metrics = int(random() * 400)
        resp = stats_schema.create(user_id=300, day_id=day, metrics=metrics)

    resp = stats_schema.fetch(user_id=Equal(300), day_id=Between(30, 40))
    assert len(resp.message) == 11  # 40 is included


def test_sorting(stats_schema):
    days = list(range(50))
    shuffle(days)
    for day in days:
        metrics = int(random() * 400)
        resp = stats_schema.create(user_id=300, day_id=day, metrics=metrics)

    resp = stats_schema.fetch(user_id=Equal(300), sort=consts.SORT_DESCENDING)
    start = 49
    for i in range(50):
        assert resp.message[i].get('day_id') == start
        start = start - 1

    resp = stats_schema.fetch(user_id=Equal(300), sort=consts.SORT_ASCENDING)
    for i in range(50):
        assert resp.message[i].get('day_id') == i


def test_reverse_schema(stats_reverse_schema):
    days = list(range(50))
    shuffle(days)
    total_reverse = 0
    for day in days:
        metrics = int(random() * 400)
        stats_reverse_schema.create(
            user_id=300, day_id=day, metrics=metrics)

        if not day % 2:
            total_reverse += 1
            stats_reverse_schema.create(
                user_id=200, day_id=day, metrics=metrics)

    resp = stats_reverse_schema.fetch(user_id=Equal(300))
    assert len(resp.message) == 50

    resp = stats_reverse_schema.fetch(user_id=Equal(200))
    assert len(resp.message) == total_reverse

    resp = stats_reverse_schema.fetch(day_id=Equal(18))
    assert len(resp.message) == 2


def test_dynamo_table_creation(table_name):
    schema = Schema(
        TableDynamo(table_name, dynamodb.connection),
        IndexDynamo(
            Index.PRIMARY, 'id', read_capacity=8, write_capacity=4),
        id=Field())

    table = schema.table
    table.create_table()
    table.table.meta.client.get_waiter('table_exists').wait(
        TableName=table_name)
    assert dynamodb.connection.Table(table_name).item_count is 0


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


def test_extension_usage(user_schema):
    @user_schema.extension('stats')
    def stats(item, fields):
        return {'days': 10, 'fields': fields}

    @user_schema.extension('history')
    def history(item, fields):
        return {'length': 20}

    response = user_schema.create(id='testextension', username='michael')
    assert response

    response = user_schema.fetch_one(
        username=Equal('michael'), fields=['stats.foobar', 'stats.tests.bar'])
    assert response.message.get('stats').get('days') == 10
    assert 'foobar' in response.message.get('stats').get('fields')
    assert 'tests.bar' in response.message.get('stats').get('fields')

    response = user_schema.fetch_one(
        username=Equal('michael'),
        fields=['history', 'stats.foobar', 'stats.tests.bar'])
    assert response.message.get('stats').get('days') == 10
    assert 'foobar' in response.message.get('stats').get('fields')
    assert 'tests.bar' in response.message.get('stats').get('fields')
    assert response.message.get('history').get('length') == 20
