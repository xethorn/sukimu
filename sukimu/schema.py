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
from copy import deepcopy
from threading import Thread

from oto import response
from oto import status

from sukimu import consts
from sukimu import exceptions
from sukimu import operations
from sukimu import utils


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

        if operation is operations.READ and not values:
            return response.Response()

        if operation is operations.CREATE:
            items = set(self.fields.keys())

        for name in items:
            field = self.fields.get(name)
            if not field:
                continue

            try:
                value = values.get(name)

                if isinstance(value, operations.Base):
                    value = value.validate(field)
                    data[name] = value
                    continue

                data[name] = field.validate(value)

            except exceptions.FieldException as e:
                errors[name] = e.args[0]
                status = False

        if errors:
            return response.create_error_response(
                consts.ERROR_CODE_VALIDATION, errors)

        return response.Response(message=data)

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

        if not validation_response:
            return validation_response

        data = validation_response.message
        errors = {}
        current = current or {}

        for index in self.indexes:
            # Some databases allow to have non unique indexes. In this case,
            # we ignore this index for the check.
            if not index.unique:
                continue

            keys = index.keys

            query = dict()
            for key in keys:
                key_value = data.get(key, current.get(key))
                if not key_value:
                    break
                query.update({key: operations.Equal(key_value)})

            if not query:
                continue

            ancestor = self.fetch_one(**query)
            if ancestor:
                if not current or dict(ancestor.message) != dict(current):
                    errors.update({
                        key: exceptions.FIELD_ALREADY_USED for key in keys})

        if errors:
            return response.create_error_response(
                consts.ERROR_CODE_DUPLICATE_KEY, errors)

        return response.Response()

    def generated(self, **dependencies):
        """Register a generated field.

        Generated fields may have some dependencies. If a specific has been
        updated for instance, the generated field will also need to be updated.
        If the generated field needs to be updated everytime.

        Args:
            dependencies (dict): The dependencies for this specific generated
                field.

        Return:
            Response: the response with the generated value.
        """

        return NotImplemented

    def extension(self, name):
        """Register an extension.

        Args:
            name (string): Name of the extension.
        """

        def wrapper(method):
            self.extensions.update({name: method})
            return method
        return wrapper

    def fetch(self, fields=None, limit=None, sort=None, index=None,
                context=None, **query):
        """Query the table to find all the models that correspond to the query.

        Args:
            fields (list): the list of fields to return on each of the items.
            limit (int): optional limit on how many items need to be fetched.
            sort (int): if the results should be sorted, and if so, in which
                order.
            index (str): name of the index to use.
            context (dict): additional context to provide (used by extensions)
            query (dict): fields to query on.

        Return:
            Response: the data of the request.
        """

        validation_response = self.validate(query, operation=operations.READ)
        if not validation_response:
            return validation_response

        schema_response = self.table.fetch(
            query, sort=sort, limit=limit, index=index)
        if schema_response and fields:
            self.decorate_response(schema_response, fields, context=context)

        return schema_response

    def fetch_one(self, fields=None, context=None, **query):
        """Fetch one specific item.

        Args:
            fields (list): the list of fields to return on the item.
            query (dict): the request fields to search on.
            context (dict): optional context (used by extensions).

        Return:
            Response: the data from the request.
        """

        validation_response = self.validate(query, operation=operations.READ)

        if not validation_response:
            return validation_response

        schema_response = self.table.fetch_one(**query)
        if schema_response and fields:
            self.decorate_response(schema_response, fields, context=context)

        return schema_response

    def decorate_response(self, response, fields, context=None):
        """Decorate a response.

        Args:
            item (dict): The current item.
            fields (dict): The fields that are need to be provided to the main
                item.
            context (dict): Additional context to provide to each extension.

        Return:
            Response: the decorated response.
        """

        if (isinstance(fields, list) or isinstance(fields, tuple) or
                isinstance(fields, set)):
            fields = utils.dict_from_strings(fields)

        data = response.message
        if isinstance(data, list):
            data = [
                self.decorate(dict(item), fields, context) for item in data]
        else:
            data = self.decorate(dict(data), fields, context)
        response.message = data

    def decorate(self, item, fields, context=None):
        """Decorate an item with more fields.

        Decoration means that some fields are going to be added to the initial
        item (using the extension with the same name.) The fields that are
        expected from this extension are also being passed.

        Fields are also cleaning the response object (unless unspecified.) For
        instance if you fetch one `user` with the fields `user.id`, only the id
        will be returned.

        Args:
            item (dict): The current item.
            fields (dict): The fields that are need to be provided to the main
                item.
            context (dict): Additional context to provide to each extension.

        Return:
            Response: the decorated response.
        """

        def activate_extension(field, item, context=None):
            extension = self.extensions.get(field)
            if not extension:
                return

            kwargs = {}
            if 'context' in extension.__code__.co_varnames:
                kwargs.update(context=context)
            item.update({field: extension(item, fields.get(field), **kwargs)})

        table_fields = fields.pop(self.table.name, -1)

        threads = []
        for field in fields:
            thread = Thread(
                target=activate_extension, args=(field, item, context))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        if table_fields == -1:
            return item

        keys = list(item.keys())
        for key in keys:
            if key not in table_fields:
                item.pop(key)

            if len(item) == len(table_fields):
                return item
        return item

    def create(self, **data):
        """Create a model from the data passed.
        """

        validation = self.validate(data, operation=operations.CREATE)
        if not validation:
            return validation

        check = self.ensure_indexes(validation)
        if not check:
            return check

        data = self.table.create(validation.message)
        return response.Response(message=data)

    def update(self, source, **data):
        """Update the model from the data passed.
        """

        if not source:
            return response.create_error_response(
                message='The source cannot be empty.')

        data = utils.key_exclude(data, source.keys())
        data = self.validate(data, operation=operations.READ)
        if not data:
            return data

        # Recreate the object - check ancestors.
        current = self.fetch_one(**{
            key: operations.Equal(val) for key, val in source.items()})
        if not current:
            return current

        fields = response.Response(
            message=dict(list(source.items()) + list(data.message.items())))

        ancestors = self.ensure_indexes(fields, current.message)
        if not ancestors:
            return ancestors

        return self.table.update(current, fields.message)

    def delete(self, **source):
        """Delete the model(s) from the data passed.
        """

        item = self.fetch_one(**source)
        if not item:
            return item

        return self.table.delete(item.message)

    def extends(self, **fields):
        """Extending a Schema.

        Extension of a schema allows to add new fields. If you have a table
        with users, some users might require different fields (for instance,
        if the user has a gaming console, you might want to get more details
        about this gaming console.)
        """

        fields = utils.dict_merge(self.fields, fields)
        table = self.table.copy()
        indexes = deepcopy(self.indexes)
        return Schema(table, *indexes, **fields)


class Table():

    def __init__(self, name):
        self.name = name
        self.indexes = {}

    def set_schema(self, schema):
        self.schema = schema
        for index in self.schema.indexes:
            self.add_index(index)

    def add_index(self, index):
        return NotImplemented

    def find_index(self, fields):
        for index in self.indexes.values():
            if len(fields) == 2:
                is_hash = index.hash in (fields[0], fields[1])
                is_range = index.range in (fields[0], fields[1])
                if is_hash and is_range:
                    return index
            if len(fields) == 1 and index.hash == fields[0]:
                return index

    def create(self, data):
        return NotImplemented

    def delete(self, source):
        return NotImplemented

    def update(self, source, data):
        return NotImplemented

    def fetch(self, query, sort=None, limit=None):
        return NotImplemented

    def fetch_one(self, **query):
        return NotImplemented

    def create_table(self):
        return NotImplemented

    def copy(self):
        return NotImplemented


class Index():
    PRIMARY = 1
    LOCAL = 2
    GLOBAL = 3

    def __init__(self, *keys, name=None, unique=True):
        self.name = name
        self.keys = list(keys)
        self.unique = unique
