def key_dict(dictionary, default=None):
    """Create a dictionary with default keys.

    This methods duplicate the keys of the entered dictionary and recreate a
    new dictionary with the same keys and for default the default value
    provided.

    Example:

        keys = {'user': Field(), 'password': Field()}
        key_dict(keys, 'hello')
        ↳ {'user': 'hello', 'password': 'hello'}

    Args:
        dictionary (dict): The dictionary to treat.
        default: The default value for each of the keys in the new dictionary.
    """

    return {key: default for key in dictionary.keys()}


def key_exclude(dictionary, keys):
    """Exclude all the keys of a dictionary.

    This method removes all the keys from a dictionary that have been passed
    as a second parameter.

    Args:
        dictionary (dict): The used dictionary.
        keys (list): The keys of the dictionary.

    Return:
        Dictionary: The dictionary without the keys.
    """

    return {key: val for key, val in dictionary.items() if key not in keys}


def dict_from_strings(array, separator='.'):
    """Recreate a dictionary from strings that are in an array.

    If you have strings such as "user.id", "user.name", those ideally can be
    turned into a dictionary where user is the principal key and name, id two
    values of this dictionary.

    Args:
        array (array): The array to turn into a dict.
    Return:
        dict: The dictionary.
    """

    dictionary = {}

    for string in array:
        parts = string.split(separator)
        key = parts[0]
        last_parts = separator.join(parts[1:])

        if last_parts:
            dictionary.setdefault(key, []).append(last_parts)
        else:
            dictionary.update({key: []})

    return dictionary


def dict_merge(*dicts):
    """Merge all dictionnary into one.

    In the event of a collision, the last dictionary is the one that will
    override the initial value.

    Args:
        dicts (list): List of all the dictionary to merge.
    Return:
        dict: The dictionary that contains all the other ones.
    """

    dictionaries = list()
    for d in dicts:
        dictionaries = dictionaries + list(d.items())

    return dict(dictionaries)


def list_merge(*lists):
    """Merge all the lists into one.

    Args:
        lists (list): List of all the lists to merge.
    Return:
        list: the result of the merged lists.
    """

    final_list = []
    for l in lists:
        final_list = final_list + l
    return final_list
