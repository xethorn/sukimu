from unittest import mock
import pytest

from sukimu import exceptions
from sukimu import fields


def test_field_creation():
    f = fields.Field()
    assert f
    assert f.basetype == str
    assert f.required is False
    assert len(f.validators) == 0


def test_required_field_creation():
    f = fields.Field(required=True)
    assert f
    assert f.basetype == str
    assert f.required is True
    assert len(f.validators) == 0


def test_field_validators(monkeypatch):
    validator = mock.MagicMock()
    f = fields.Field(validator, basetype=str)
    assert len(f.validators) == 1
    assert f.validators[0] == validator


def test_validation():
    f = fields.Field(required=True)
    wrong_values = (list(), set(), dict())
    assert f.validate('Hello') == 'Hello'
    for wrong_value in wrong_values:
        with pytest.raises(exceptions.FieldException):
            f.validate(wrong_value)


def test_validation_on_required_field():
    f = fields.Field(required=True)
    values = (None, '')
    for value in values:
        with pytest.raises(exceptions.FieldException):
            f.validate(value)


def test_validation_on_wrong_field_type():
    f = fields.Field(basetype=int)
    with pytest.raises(exceptions.FieldException):
        f.validate('value')


def test_validation_with_validators():
    def validator(value):
        return value.replace('hello', 'hi')

    def world_validator(value):
        if 'world' not in value:
            raise exceptions.FieldException(
                'The word "world" has not been found')
        return value.replace('world', 'earth')

    f = fields.Field(validator, world_validator)
    assert f.validate('hello world') == 'hi earth'
    with pytest.raises(exceptions.FieldException):
        f.validate('hello monde')
