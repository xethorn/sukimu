from unittest import mock
import pytest

from schema import fields
from schema import operations
from schema import response
from schema import schema


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
    assert resp.status is response.Status.INVALID_FIELDS
    assert resp.errors.get('username')
    assert not resp.errors.get('id')

    resp = s.validate({'username': 'test'}, operations.CREATE)
    assert resp.status is response.Status.OK

    resp = s.validate({}, operations.READ)
    assert resp.status is response.Status.OK


def test_field_validation_on_read():
    s = create_schema(fields=dict(
        id=fields.Field(),
        username=fields.Field(required=True)))

    resp = s.validate(
        {'username': 'foo', 'unknownfield': 'value'}, operations.READ)
    assert resp.status is response.Status.OK
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

    error_response = response.create_error_response()
    assert full_schema.ensure_indexes(error_response) is error_response

    data = dict(id='id-value', username='username-value')
    fetch_one = mock.MagicMock(return_value=error_response)
    success_response = response.create_success_response(data)
    monkeypatch.setattr(full_schema, 'fetch_one', fetch_one)

    resp = full_schema.ensure_indexes(success_response)
    assert resp.success

    fetch_one.return_value = success_response
    resp = full_schema.ensure_indexes(success_response)
    assert not resp.success
    assert 'id' in resp.errors
    assert 'username' in resp.errors


def test_extensions(full_schema):
    @full_schema.extension('stats')
    def stats(item, fields):
        return

    assert full_schema.extensions.get('stats')
