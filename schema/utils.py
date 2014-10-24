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
