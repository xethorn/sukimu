import atexit

import boto3


connection = boto3.resource(
    'dynamodb', endpoint_url='http://localhost:8000', region_name='us-west-2',
	aws_access_key_id='foo', aws_secret_access_key='foo')


def clean():
    tables = connection.tables.all()
    if not tables:
        return

    for table in tables:
        try:
            table.delete()
        except:
            pass

atexit.register(clean)
