from schema import exceptions


class Field():
    def __init__(
            self, basetype=str, required=False, check=None,
            index=None, **extra):
        self.required = required
        self.check = check or []
        self.extra = extra
        self.basetype = basetype

    def validate(self, value):
        if self.required and not value:
            raise exceptions.FieldException('This field is required.')
        return value
