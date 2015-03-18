"""
Field
=====

The field handle the base validation of the values being passed. It checks
first if the value is required, if the type is correct then it invokes any
additional validators.
"""

from sukimu import exceptions


class Field():
    def __init__(
            self, *validators, basetype=str, required=False):
        """Field

        All fields should have a simple basetype (number, str, map) that
        matches a database format.

        Arg:
            validators (set): All the additional validators you will want to
                run against the value.
            basetype (type): A type (str, dict, set) that explains what this
                field is about. The validation will verify if this value is
                working as expected.
            required (boolean): If this field is required. If absent, it will
                throw an exception.
        """

        self.basetype = basetype
        self.required = required
        self.validators = validators or []

    def validate(self, value):
        """Validator

        Perform controls on the value provided.

        Arg:
            value (str): The value to run the validators against.

        Exception:
            FieldException: If an error has occurred with the field value, an
                exception is immediately raised (either by this method, or by
                one of the validator).

        Return:
            The value.
        """

        if not value:
            if self.required:
                raise exceptions.FIELD_REQUIRED
            return value

        if not isinstance(value, self.basetype):
            raise exceptions.FIELD_WRONG_FORMAT

        for validator in self.validators:
            value = validator(value)

        return value
