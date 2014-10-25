from unittest import mock

from schema import fields
from schema import schema


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
