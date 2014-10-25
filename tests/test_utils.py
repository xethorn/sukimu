from schema import utils


def test_key_dict():
    dictionary = {'key1': 'value1', 'key2': 'value2'}
    resp = utils.key_dict(dictionary, True)
    assert resp.get('key1') is True
