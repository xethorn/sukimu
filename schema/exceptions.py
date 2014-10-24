class FieldException(Exception):
    pass

FIELD_ALREADY_USED = FieldException(
    'This is already used.')
FIELD_WRONG_FORMAT = FieldException(
    'This field is not following the right format.')
FIELD_REQUIRED = FieldException(
    'This field is required.')
