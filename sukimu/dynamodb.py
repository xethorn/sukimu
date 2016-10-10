"""Adaptor for DynamoDB.
"""

from boto3.dynamodb import conditions
from oto import response
from oto import status

from sukimu import consts
from sukimu import operations
from sukimu import schema


class TableDynamo(schema.Table):

    def __init__(self, name, connection):
        """Create a TableDynamo.

        Args:
            name (string): Name of the table.
            connection (DynamoDBConnection): The dynamodb connection.
        """

        self.name = name
        self.connection = connection
        self.table = connection.Table(name)
        self.indexes = {}
        self.hash = None
        self.range = None

    def copy(self):
        """Create a copy of the current object.

        Return:
            TableDynamo: Copy of the current instance.
        """

        return self.__class__(self.name, self.connection)

    def add_index(self, index):
        """Add an index into the tbale.

        Args:
            index (DynamoIndex): The dynamo index.
        """

        if index.type == schema.Index.PRIMARY:
            self.hash = index.hash
            self.range = index.range

        self.indexes.update({index.name: index})

    def create(self, data):
        """Create an item.

        Args:
            data (dict): Data for this specific item (refer to the boto
                dynamodb documentation to know what values are allowed.)
        Return:
            Dictionnary: The data that was added.
        """

        self.table.put_item(Item=data)
        return data

    def update(self, item, data):
        """Update an item.

        Args:
            item (DynamoDbItem): The dynamodb item to update.
            data (object): The validated fields.
        Return:
            Response: The response of the update.
        """

        if not item:
            return item

        item = item.message
        update_expression = []
        expression_attribute_values = {}
        for field, value in data.items():
            if field in [self.hash, self.range]:
                continue

            index = len(update_expression)
            update_expression.append('{}=:v{}'.format(field, index))
            expression_attribute_values.update({
                ':v{}'.format(index): value
            })
            item.update({field: value})

        item_key = {
            index: value for index, value in {
                self.hash: item.get(self.hash),
                self.range: item.get(self.range)
            }.items() if value}

        if not update_expression:
            return response.Response(status=status.OK, message=item)

        self.table.update_item(
            Key=item_key,
            UpdateExpression='SET {}'.format(','.join(update_expression)),
            ExpressionAttributeValues=expression_attribute_values)
        return response.Response(status=status.ACCEPTED, message=item)

    def delete(self, item):
        """Delete an item.

        Args:
            item (DynamoDbItem): The dynamodb item to update.
        Return:
            Response: the response of the update.
        """

        item_key = {
            index: value for index, value in {
                self.hash: item.get(self.hash),
                self.range: item.get(self.range)
            }.items() if value}
        try:
            self.table.delete_item(Key=item_key)
            return response.Response()
        except:
            return response.Response(status=status.BAD_REQUEST)

    def fetch(self, query, sort=None, limit=None, index=None):
        """Fetch one or more entries.

        Fetching entries is allowed on any field. For better performance, it is
        recommended to use one of the indexes. If no index is used, a scan will
        be performed on the table (which are much slower.)

        Args:
            query (dict): the query.
            sort (int): the sorting type (refer to schema.sort).
            limit (int): the number of items you want to get back from the
                table.
            index (str): the name of the index to used. If defined, looking for
                it.
        Return:
            List: All the fetched items.
        """

        data = dict()
        keys = list(query.keys())

        if index:
            index = self.indexes.get(index)
            assert index, 'The index requested was not found.'
        else:
            index = self.find_index(keys)

        if limit:
            data.update(Limit=limit)

        expressions = None
        for query_segment in query.items():
            key, value = query_segment
            expression = None

            if isinstance(value, operations.Equal):
                expression = conditions.Key(key).eq(value.value)

            elif isinstance(value, operations.Between):
                expression = conditions.Key(key).between(*value.value)

            elif isinstance(value, operations.In):
                if index:
                    return self.fetch_many(key, value.value, index=index.name)
                return self.fetch_many(key, value.value, index=index.name)

            if expression:
                if not expressions:
                    expressions = expression
                else:
                    expressions = expressions & expression

        if index:
            if index.name:
                data['IndexName'] = index.name

            data['ScanIndexForward'] = sort is not consts.SORT_DESCENDING
            dynamo = self.table.query(
                KeyConditionExpression=expressions,
                **data).get('Items')
        else:
            dynamo = self.table.scan(FilterExpression=expressions).get('Items')

        if not len(dynamo):
            return response.create_not_found_response()

        return response.Response(
            message=[dict(obj) for obj in dynamo])


    def fetch_many(self, key, values, index=None):
        """Fetch more than one item.

        Method used to fetch more than one item based on one key and many
        values.

        Args:
            key (string): Name of the key.
            values (list): All the values to fetch.
            index (str): the name of the index to used. If defined, looking for
                it.
        """

        message = []
        for value in values:
            message.append(
                self.fetch_one(
                    index=index, **{key: operations.Equal(value)}).message)

        if not message:
            return response.create_not_found_response()

        return response.Response(message)

    def fetch_one(self, index=None, **query):
        """Get one item.

        Args:
            query (dict): The query item.
            index (str): the name of the index to used. If defined, looking for
                it.

        Return:
            Response: If the item is found, it is provided in the message,
                if not found, the status is set to NOT_FOUND.
        """

        default_response = response.create_not_found_response()
        field_names = list(query.keys())

        required = 1
        is_hash = self.hash in field_names
        is_range = self.range in field_names
        if is_range:
            required = 2

        item = None

        if len(query) == required and is_hash:
            data = dict()
            data[self.hash] = query.get(self.hash).value

            if is_range:
                data[self.range] = query.get(self.range).value

            try:
                item = self.table.get_item(Key=data).get('Item', None)
            except:
                return default_response

        if not item:
            data = self.fetch(query, index=index, limit=1).message
            if data and len(data) == 1:
                item = data[0]

        if item:
            return response.Response(message=item)

        return default_response

    def create_table(self):
        """Create a dynamodb table.

        The dynamodb table only needs to know about the indexes and the type
        of those indexes.
        """

        local_secondary_index = []
        global_secondary_index = []

        attributes = []
        attribute_keys = set()
        indexes = {}
        indexes[schema.Index.PRIMARY] = None

        table_provision = None

        for index in self.schema.indexes:
            hash_field = self.schema.fields.get(index.hash)
            range_field = self.schema.fields.get(index.range)

            provision = dict(
                ReadCapacityUnits=index.read_capacity,
                WriteCapacityUnits=index.write_capacity)

            if index.hash not in attribute_keys:
                attribute_keys.add(index.hash)
                attributes.append(dict(
                    AttributeName=index.hash,
                    AttributeType=DynamoType.get(hash_field.basetype)))

            key_schema = [dict(
                AttributeName=index.hash,
                KeyType='HASH')]

            if range_field:
                if index.range not in attribute_keys:
                    attribute_keys.add(index.range)
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

            response = dict(
                IndexName=index.name,
                KeySchema=key_schema,
                Projection={'ProjectionType': 'ALL'})

            if index.type == schema.Index.GLOBAL:
                response.update(ProvisionedThroughput=provision)

            indexes.setdefault(index.type, []).append(response)

        self.connection.create_table(
            TableName=self.name,
            KeySchema=indexes.get(schema.Index.PRIMARY),
            AttributeDefinitions=attributes,
            ProvisionedThroughput=table_provision,
            **{
                index_name: index_definition
                for index_name, index_definition in {
                    'LocalSecondaryIndexes': indexes.get(schema.Index.LOCAL),
                    'GlobalSecondaryIndexes': indexes.get(schema.Index.GLOBAL)
                }.items() if index_definition})


class IndexDynamo(schema.Index):

    def __init__(
            self, type, *keys, name=None, read_capacity=None,
            write_capacity=None, unique=True):
        self.name = name
        self.type = type
        self.hash = keys[0]
        self.range = keys[1] if len(keys) == 2 else None
        self.read_capacity = read_capacity
        self.write_capacity = write_capacity
        self.unique = unique

    @property
    def keys(self):
        keys = [self.hash]
        if self.range:
            keys.append(self.range)
        return keys

DynamoType = {
    str: 'S',
    int: 'N',
}
