class Status():
    ERROR = 500
    INVALID_FIELDS = 501
    FIELD_VALUE_ALREADY_USED = 502
    KEY_VALUE_ALREADY_USED = 503
    NOT_FOUND = 400
    OK = 200


class Response():
    def __init__(self, status, message, errors=None):
        self.status = status
        self.errors = errors or {}
        self.message = message or {}

    def __getattr__(self, key, default_value=None):
        return self.message.get(key, default_value)

    @property
    def success(self):
        return self.status == Status.OK

    @staticmethod
    def create_fail_response(errors=None):
        return ModelResponse(
            status=Status.ERROR,
            errors=errors,
            data=None)

    @staticmethod
    def create_success_response(data=None):
        return ModelResponse(
            status=Status.OK,
            data=data or [])
