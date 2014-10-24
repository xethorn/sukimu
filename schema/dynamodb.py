"""Adaptor for DynamoDB.
"""

from boto.dynamodb2 import table

from schema import schema
from schema import response


class TableDynamo(schema.Table):

    def __init__(self, name, connection):
        """Create a TableDynamo.

        Args:
            name (string): Name of the table.
            connection (DynamoDBConnection): The dynamodb connection.
        """

        self.name = name
        self.table = table.Table(name, connection=connection)
        self.indexes = {}

    def is_entry_equal(self, entry1, entry2):
        """Method to check if two entries are equal.

        Used when an `update` is performed: it helps checking that the found
        ancestor is not the current entry.

        Args:
            entry1 (dict): current object.
            entry2 (dict): object used for comparison.
        Return:
            Boolean: If the current entry is equal to the other entry.
        """

        return entry1.get(self.hash) == entry2.get(self.hash)

    def create(self, data):
        """Create an item.

        Args:
            data (dict): Data for this specific item (refer to the boto
                dynamodb documentation to know what values are allowed.)
        Return:
            Dictionnary: The data that was added.
        """

        self.table.put_item(data=data)
        return data

    def fetch(self, query, limit=None):
        """Fetch one or more entries.

        Fetching entries is allowed on any field. For better performance, it is
        recommended to use one of the indexes. If no index is used, a scan will
        be performed on the table (which are much slower.)

        Args:
            query (dict): The query.
            limit (int): the number of items you want to get back from the
                table.
        Return:
            List: All the fetched items.
        """

        data = dict()
        keys = list(query.keys())
        index = self.find_index(keys)

        if limit:
            data.update(limit=limit)

        for query_segment in query.items():
            key, value = query_segment

            if isinstance(value, Equal):
                data[key + '__eq'] = value.value

        if index:
            if index.name:
                data['index'] = index.name
            dynamo = list(self.table.query_2(**data))
        else:
            dynamo = list(self.table.scan(**data))

        if not len(dynamo):
            return response.Response(
                status=response.Status.NOT_FOUND,
                message=[])

        return response.Response(
            status=Status.OK,
            message=[dict(obj) for obj in dynamo])

    def fetch_one(self, **query):
        """Get one item.

        Args:
            query: (dict) The query item.
        Return:
            Response: If the item is found, it is provided in the message,
                if not found, the status is set to NOT_FOUND.
        """

        field_names = list(query.keys())
        is_hash = self.hash in field_names
        is_range = self.range in field_names
        item = None

        if len(query) < 3 and is_hash or is_range:
            data = dict()
            if is_hash:
                data[self.hash] = query.get(self.hash)
            if is_range:
                data[self.range] = query.get(self.range)
            item = dict(self.table.get_item(**data))

        if not item:
            data = self.fetch(query, limit=1).response
            if len(data) == 1:
                item = data[0]

        if item:
            return response.Response(
                status=response.Status.OK,
                message=item)

        return response.Response(
            status=response.Status.NOT_FOUND,
            message=None)

    def create_table(self):
        """Create a dynamodb table.

        The dynamodb table only needs to know about the indexes and the type
        of those indexes.
        """

        local_secondary_index = []
        global_secondary_index = []

        attributes = []
        indexes = {}
        indexes[schema.Index.PRIMARY] = None

        table_provision = None

        for index in self.schema.indexes:
            hash_field = self.schema.fields.get(index.hash)
            range_field = self.schema.fields.get(index.range)

            provision = dict(
                ReadCapacityUnits=index.read_capacity,
                WriteCapacityUnits=index.write_capacity)

            attributes.append(dict(
                AttributeName=index.hash,
                AttributeType=DynamoType.get(hash_field.basetype)))

            key_schema = [dict(
                AttributeName=index.hash,
                KeyType='HASH')]

            if range_field:
                attributes.append(dict(
                    AttributeName=index.range,
                    AttributeType=DynamoType.get(range_field.basetype)))

                key_schema.append(dict(
                    AttributeName=index.range,
                    KeyType='RANGE'))

            if index.type == schema.Index.PRIMARY:
                table_provision = provision
                indexes[index.type] = key_schema
                continue

            indexes.setdefault(index.type, []).append(dict(
                IndexName=index.name,
                KeySchema=key_schema,
                ProvisionedThroughput=provision,
                Projection={'ProjectionType': 'ALL'}))

        self.table.connection.create_table(
            attributes, self.name, indexes.get(schema.Index.PRIMARY),
            table_provision,
            local_secondary_indexes=indexes.get(schema.Index.LOCAL, None),
            global_secondary_indexes=indexes.get(schema.Index.GLOBAL, None))


class IndexDynamo(schema.Index):

    def __init__(
            self, type, *keys, name=None, read_capacity=None,
            write_capacity=None):
        self.name = name
        self.type = type
        self.hash = keys[0]
        self.range = keys[1] if len(keys) == 2 else None
        self.read_capacity = read_capacity
        self.write_capacity = write_capacity

    @property
    def keys(self):
        keys = [self.hash]
        if self.range:
            keys.append(self.range)
        return keys

DynamoType = {
    str: 'S',
}
