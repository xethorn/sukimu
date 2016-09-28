from unittest import mock
import pytest

from oto import response
from oto import status

from sukimu import fields
from sukimu import operations
from sukimu import schema


def create_schema(fields=None, indexes=None):
    table = schema.Table('TableName')
    indexes = indexes or set()
    fields = fields or dict()
    return schema.Schema(table, *indexes, **fields)


def test_create_schema(monkeypatch):
    table = schema.Table('TableName')
    s = schema.Schema(table)
    assert isinstance(s, schema.Schema)
    assert s.table is table
    assert table.schema == s


def test_create_schema_with_fields():
    table = schema.Table('TableName')
    s = schema.Schema(table, id=fields.Field())
    assert isinstance(s.fields_dependencies.get('id'), list)
    assert len(s.indexes) == 0


def test_create_schema_with_indexes(monkeypatch):
    table = schema.Table('TableName')
    index = schema.Index('id')
    monkeypatch.setattr(table, 'add_index', mock.MagicMock())
    s = schema.Schema(table, index)

    assert len(s.indexes) == 1
    assert table.add_index.called


def test_field_validation_on_create():
    s = create_schema(fields=dict(
        id=fields.Field(),
        username=fields.Field(required=True)))

    # operation is mandatory
    with pytest.raises(Exception):
        s.validate({})

    resp = s.validate({'id': 'test'}, operations.CREATE)
    assert resp.status is status.BAD_REQUEST
    assert resp.errors.get('message').get('username')
    assert not resp.errors.get('message').get('id')

    resp = s.validate({'username': 'test'}, operations.CREATE)
    assert resp.status is status.OK

    resp = s.validate({}, operations.READ)
    assert resp.status is status.OK


def test_field_validation_on_read():
    s = create_schema(fields=dict(
        id=fields.Field(),
        username=fields.Field(required=True)))

    resp = s.validate(
        {'username': 'foo', 'unknownfield': 'value'}, operations.READ)
    assert resp.status is status.OK
    assert not resp.message.get('unknownfield')

    # Fields to validate should be a dictionary of format:
    # <field name, value>
    with pytest.raises(Exception):
        s.validate([], operations.READ)


@pytest.fixture
def full_schema():
    return create_schema(
        indexes=[
            schema.Index('id'),
            schema.Index('id', 'username')],
        fields=dict(
            id=fields.Field(),
            username=fields.Field(required=True)))


def test_ensure_index(monkeypatch, full_schema):
    # If the validation_response is not the rigt object, throws an exception.
    with pytest.raises(Exception):
        full_schema.ensure_indexes(object())

    error_response = response.Response(status=status.BAD_REQUEST)
    assert full_schema.ensure_indexes(error_response) is error_response

    data = dict(id='id-value', username='username-value')
    fetch_one = mock.MagicMock(return_value=error_response)
    success_response = response.Response(data)
    monkeypatch.setattr(full_schema, 'fetch_one', fetch_one)

    resp = full_schema.ensure_indexes(success_response)
    assert resp

    fetch_one.return_value = success_response
    resp = full_schema.ensure_indexes(success_response)
    assert not resp
    assert 'id' in resp.errors.get('message')
    assert 'username' in resp.errors.get('message')


def test_extensions(full_schema):
    @full_schema.extension('stats')
    def stats(item, fields):
        return

    assert full_schema.extensions.get('stats')


def test_decorating_with_extension(full_schema):
    """Test decorating with an extension.
    """

    spy = mock.MagicMock()
    item = {'id': 'foo'}
    context = {'value': 'context_value'}
    fields = {'extension_name': ['foo']}
    extension_name = 'extension_name'

    @full_schema.extension(extension_name)
    def stats(item, fields, context=None):
        spy(item, fields, context)
        return context.get('value')

    response = full_schema.decorate(item, fields=fields, context=context)
    spy.assert_called_with(item, ['foo'], context)
    assert spy.called
    assert response.get(extension_name) == context.get('value')
