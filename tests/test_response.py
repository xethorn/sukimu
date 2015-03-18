import pytest

from sukimu import response
from sukimu import exceptions


def test_response_creation():
    message = 'Hello World'
    resp = response.Response(response.Status.OK, message)
    assert resp.status is response.Status.OK
    assert resp.message is message
    assert not resp.errors


def test_response_errors():
    """Response errors should always be a dictionary.
    """

    message = ''
    resp = response.Response(response.Status.OK, message, errors=None)
    assert isinstance(resp.errors, dict)


def test_get_quick_access_to_message():
    """Only works if the message is a dictionary.
    """

    message = dict(attribute='value')
    resp = response.create_success_response(message=message)
    assert resp.attribute == 'value'


def test_access_to_non_dict_message():
    """Only works if the message is a dictionary.
    """

    message = ('attribute', 'value')
    resp = response.create_success_response(message=message)
    with pytest.raises(exceptions.FieldException):
        assert resp.attribute == 'value'


def test_response_success():
    resp = response.Response(response.Status.OK, '')
    assert resp.success

    resp = response.Response(response.Status.NOT_FOUND, '')
    assert not resp.success


def test_create_success_response():
    resp = response.create_success_response()
    assert resp.success
    assert resp.status is response.Status.OK


def test_create_error_response():
    resp = response.create_error_response()
    assert not resp.success
    assert resp.status is response.Status.ERROR
