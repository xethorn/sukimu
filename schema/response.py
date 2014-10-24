class Status():
    ERROR = 500
    INVALID_FIELDS = 501
    FIELD_VALUE_ALREADY_USED = 502
    KEY_VALUE_ALREADY_USED = 503
    NOT_FOUND = 400
    OK = 200


class Response():
    """Response

    The Response class provides an envelop for the messages that are going from
    the Schema and the Table. They ensure the data is in good standing (and if
    not, they provide context around the errors.)

    """

    def __init__(self, status, message, errors=None):
        """Response constructor.

        See:
            `create_error_response` and `create_success_response` for
            shortcuts.

        Args:
            status (int): The status of the Response.
            message: The message of the response. The message can contain any
                datastructures (dictionary, set, list)
            errors (list): The errors (if some have occured.)
        """

        self.status = status
        self.errors = errors or {}
        self.message = message or {}

    def __getattr__(self, key, default_value=None):
        """Shortcut to access properties on the response message.

        If the response message (and only if the response message is a
        dictionary), this method provides a shortcut to access its properties.

        Raises:
            FieldException: If the response format is not a dictionary.

        Args:
            key (string): The key to get.
            default_value: The default value.

        Return:
            The value for this specific key (and if it does not exist the
            default value.)
        """

        if isinstance(self.message, dict):
            return self.message.get(key, default_value)
        raise exceptions.RESPONSE_FORMAT_GETATTR_NOT_AVAILABLE

    @property
    def success(self):
        return self.status == Status.OK

    @staticmethod
    def create_error_response(errors=None, message=None):
        """Create an error response.
        """

        return ModelResponse(
            status=Status.ERROR,
            errors=errors,
            message=message)

    @staticmethod
    def create_success_response(message=None):
        """Create a success response.
        """

        return ModelResponse(
            status=Status.OK,
            message=message)
