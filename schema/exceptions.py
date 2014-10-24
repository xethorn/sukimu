class FieldException(Exception):
    pass

FIELD_ALREADY_USED = FieldException('This is already used.')
