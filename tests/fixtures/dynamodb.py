import atexit
import time

from boto.dynamodb2.layer1 import DynamoDBConnection
from schema.dynamodb import TableDynamo


connection = DynamoDBConnection(
    host='localhost',
    port='8000',
    aws_secret_access_key='foo',
    aws_access_key_id='bar',
    is_secure=False)


def clean():
    while(True):
        tables = connection.list_tables().get('TableNames')
        if not tables:
            return

        for table in tables:
            try:
                TableDynamo(table, connection).table.delete()
            except:
                pass
        time.sleep(1)

atexit.register(clean)
