from sukimu import utils


def test_key_dict():
    dictionary = {'key1': 'value1', 'key2': 'value2'}
    resp = utils.key_dict(dictionary, True)
    assert resp.get('key1') is True


def test_dict_from_strings():
    array = ['user.stats.daily', 'user.username']
    response = utils.dict_from_strings(array)
    assert len(response) == 1
    assert len(response.get('user')) == 2
    assert 'username' in response.get('user')
    assert 'stats.daily' in response.get('user')
