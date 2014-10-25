"""
Schemas
=======

Schemas defines the structure of the fields of your table. The schema handles
fields validations, and ensure index unicity. There are 5 main concepts around
the schemas:

**Indexes**

An index defines which key (or set of keys) should be unique within your table.
The schema will perform checks on those indexes whenever a row is being created
or updated.

Some examples:

1. If you have a user table, and need usernames and emails to be unique, you
   will have then 2 indexes.
2. If you have a session token table with a user id and a token number, you can
   have one index composed of two keys: user id (hash) and token number (range)

**Validators**

Validators are health checks on the provided data. For example: if you have a
field `age`, the field is most likely going to have a defined range (minimum
and maximum). If a value provided is not valid, the field validator throws an
exception, caught by the schema, and returned as part of the response (so if
more than one field is invalid, the user can be informed.)

**Extensions**

Extensions are only available for `fetch` and `fetch_one` method. They are
populating more fields on the returned object.

For instance: if you have a user table, and a profile table, you probably want
the user to be able to get the profile as part of the same response. So
`profile` can be an extension of user.

**Generated fields**

Generated fields are created after field validation. For instance: on a blog
post, you want to capture the number of words, it could be a generated field.
Those fields are saved into the database.

**Operations**

Field operations are used for two things: the first is to validate all the
possible value this operation contains (by using the field itself) and the
second is to write the correct query.

For example: ``fetch_one(dict(username=Equal('michael')))``, will convert the
key into ```username__eq``` in dynamodb.

**Some examples**

Create a basic schema::

    user = models.Schema(table, **fields)

Create a schema that has generated fields:

    user = models.Schema(table, **fields)

    @user.generated('username')
    def add_username_to_hisory(model):
        # do something.
        return model

Create extensions::

    user = models.Schema(collection, **fields)

    @user.extension('messages')
    def profile(obj):
        # get the profile for the user.
        return profile
"""

from collections import namedtuple

from schema import exceptions
from schema import operations
from schema import response
from schema import utils


class Schema():

    def __init__(self, table, *indexes, **fields):
        """Initialize the Schema.

        Args:
            table (string): Name of the table or string.
            options (dict): All the schema options.
        """

        self.table = table
        self.indexes = indexes
        self.fields = fields

        self.fields_dependencies = utils.key_dict(fields, default=[])
        self.extensions = dict()

        # Register the schema with the table.
        self.table.set_schema(self)

    def validate(self, values, operation):
        """Validate the model.

        Args:
            values (dict): The values to validate.
            operation (int): The different operations
                (correspond to schema operations).
        Return:
            Response.
        """

        data = dict()
        errors = dict()
        success = False
        items = set(values.keys())

        if operation is operation.READ and not values:
            return response.Response.create_success_response()

        if operation is operation.CREATE:
            items = set(self.fields.keys())

        for name in items:
            field = self.fields.get(name)
            if not field:
                continue

            try:
                data[name] = field.validate(values.get(name))

            except FieldException as e:
                errors[name] = e
                status = False

        status = response.Status.OK
        if errors:
            status = response.Status.INVALID_FIELDS

        return response.Response(
            status=status,
            message=data,
            errors=errors)

    def ensure_indexes(self, validation_response, current=None):
        """Ensure index unicity.

        One particularity of an index: it should be unique. For example in
        DynamoDb: an index has a hash and a range, combined, they represent the
        key of that specific row – and thus should be unique.

        If an index only has one key, that key on its own should also be
        unique (e.g. user_id).

        Args:
            validation_response (Response): The validation response that
                contains the validated fields.
            current (dict): Operations such as update requires to check against
                the found ancestor and the current row that needs to be
                validated.
        Return:
            Response: The response
        """

        if not validation_response.success:
            return validation_response

        data = validation_response.message
        errors = {}

        for index in self.indexes:
            keys = index.keys
            query = {key: operations.Equal(data.get(key)) for key in keys}
            ancestor = self.fetch_one(query)
            if ancestor.success:
                error.update({
                    key: exceptions.FIELD_ALREADY_USED for key in keys})

        status = response.Status.OK
        if errors:
            status = response.Status.FIELD_VALUE_ALREADY_USED

        return Response(
            status=status,
            errors=errors)

    def generated(self, **dependencies):
        """Register a generated field.

        Generated fields may have some dependencies. If a specific has been
        updated for instance, the generated field will also need to be updated.
        If the generated field needs to be updated everytime
        """

        pass

    def extension(self, name):
        """Register an extension.

        Args:
            name (string): Name of the extension.
        """

        def wrapper(method):
            self.extensions.update({
                name: method
            })
            return method
        return wrapper

    def fetch(self, limit=None, **query):
        """Query the table to find all the models that correspond to the query.
        """

        if not query:
            return

        return self.table.fetch(query, limit=limit)

    def fetch_one(self, **query):
        return self.table.fetch_one(**query)

    def create(self, **data):
        """Create a model from the data passed.
        """

        validation = self.validate(data, operation=SchemaOperation.CREATE)
        if not validation.success:
            return validation

        check = self.ensure_indexes(validation)
        if not check.success:
            return check

        return self.table.create(data)

    def update(self, source, data):
        """Update the model from the data passed.
        """

        pass

    def delete(self, source):
        """Delete the model(s) from the data passed.
        """

        self.table.delete(source)

        pass


class Table():

    def __init__(self, name):
        self.name = name
        self.indexes = {}

    def set_schema(self, schema):
        self.schema = schema
        for index in self.schema.indexes:
            self.add_index(index)

    def add_index(self, index):
        keys = index.keys
        keys.sort()
        for key in keys:
            self.indexes.setdefault(key, []).append(index)

        if len(keys) > 1:
            self.indexes.setdefault('-'.join(keys), []).append(index)

    def find_index(self, fields):
        fields.sort()
        key = '-'.join(fields)
        if key in self.indexes:
            return self.indexes.get(key)[0]
        return

    def create(self, data):
        raise Exception()

    def delete(self, source):
        raise Exception()

    def update(self, source, data):
        raise Exception()

    def fetch(self, query, limit=None):
        raise Exception()

    def fetch_one(self, **query):
        raise Exception()

    def is_entry_equal(self, entry, compare):
        raise Exception()

    def create_table(self):
        raise Exception()


class Index():
    PRIMARY = 1
    LOCAL = 2
    GLOBAL = 3

    def __init__(self, *keys, name=None):
        self.name = name
        self.keys = list(keys)
