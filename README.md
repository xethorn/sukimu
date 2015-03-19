Sukimu
======

[![License](http://img.shields.io/:license-mit-blue.svg)](http://doge.mit-license.org)
[![Build status](https://travis-ci.org/xethorn/sukimu.svg?branch=master)](https://travis-ci.org/xethorn/sukimu/)

A pythonic interface for nosql databases (supports DynamoDb.)

Sukimu provides a standard way to write your table schema (fields, validators,
indexes) and perform CRUD operations. This framework also offers model
extensions, and field pickling for any read operations.


## Installation

Using pypi:

```bash
pip install sukimu
```

Using git:
```bash
pip install git+https://github.com/xethorn/sukimu.git#egg=sukimu
```


## Basic usage

When building a new project from scratch, you often need a user table. For this
specific table, we have the following rules:

* A unique id: id used across our codebase to identify the content that is
  owned by the user.
* Username: chain of characters that identify the user.
* Password: encrypted field.
* Full name: nice to have but not required.
* Active: if the account is active or not.

```python
# If you don't have dynamodb set, you can use a local dynamodb
from boto.dynamodb2.layer1 import DynamoDBConnection

from sukimu.dynamodb import TableDynamo, IndexDynamo
from sukimu.fields import Field
from sukimu.schema import Schema


connection = DynamoDBConnection(
    host='localhost', port='3333', aws_secret_access_key='foo',
    aws_access_key_id='bar', is_secure=False)


UserModel = Schema(
    TableDynamo('user', connection),

    IndexDynamo(
        Index.PRIMARY, 'id', read_capacity=1, write_capacity=1),

    IndexDynamo(
        Index.GLOBAL, 'username', name='username_index',
        read_capacity=1, write_capacity=1),

    id=Field(fields.id),

    # Login information
    username=Field(validator.username, required=True),
    password=Field(validator.password, required=True),

    # User personal informations
    full_name=Field(),
    active=Field(basetype=boolean))
```

If your table is not yet in DynamoDb, you can create it by running:

```python
UserModel.table.create_table()
```

## Indexes

An index defines which key (or set of keys) should be unique within your table.
The schema will perform checks on those indexes whenever an entry is being
created or updated.

Some examples:

* If you have a user table, and need usernames and emails to be unique, you
  will have then 2 indexes.
* If you have a session token table with a user id and a token number, you can
  have one index composed of two keys: user id (hash) and token number (range)

### DynamoDb indexes

DynamoDb indexes provides additional features such as the ability to set the
throughput (read and write capacity.) In addition, Global Indexes do not
require the combinaison (hash - range) to be unique, to enable this, you can
use `unique=False`.

## Operations

### Basics

The table is abstracted in a way that you can run any operations:

* `fetch`: fetch one or more entries based on the attributes.
* `fetch_one`: find one entry that matches the requirements.
* `create`: add a new entry (sukimu ensures index unicity.)
* `delete`: remove an entry.
* `update`: update an entry.

Example:

```python
from sukimu.operations import Equal

resp = UserModel.create(id='1a872nd', username='celine')
assert resp.success

resp = UserModel.fetch_one(username=Equal('celine'))
assert resp.username == 'celine'

# See Validators section for more details.
resp = UserModel.update(dict(id='1a872nd'), username='new$username')
print(resp.errors) # an error will show on the `$`

resp = UserModel.update(dict(id='1a872nd'), username='NewUsername')
assert resp.username == 'newusername'

resp = UserModel.fetch_one(id=Equal('1a872nd'))
assert resp.username == 'newusername'
```

### Response format

Sukimu provides a response envelope that aims to help understand the type of
data being returned:

* `response.message`: If the operation was successful, this attributes contains
  the data.
* `response.errors`: Instead of showing one error at a time, all the errors
  detected during the validation populate this attribute.
* `response.status`: similar to http status codes. For example: fetching data
  that does not exist returns a 404.


## Validators

Validators are health checks on the provided data.

For example: if you have a field `age`, the field is most likely going to have
a defined range (minimum and maximum). If a value provided is not valid, the
field validator throws an exception, caught by the schema, and returned as part
of the response (so if more than one field is invalid, the user can be
informed.)

```python
from schema import exceptions


USERNAME_FORMAT = re.compile('^[a-z\-\d]+$')

def username(value):
    """Username validation.

    Args:
        value (str): the username.
    Return:
        str: All usernames should be lowercase.
    """

    if not value or len(value) > 20:
        raise exceptions.FieldException(
            'Username should be less than 20 characters.')

    if not len(value) > 3:
        raise exceptions.FieldException(
            'Username should contain more than 3 characters.')

    if not USERNAME_FORMAT.match(value):
        raise exceptions.FieldException(
            'Usernames can only have letters and digits.')

    return value.lower()
```

Chaining validators is possible and it happens on the schema:

```python
UserSchema = Schema(
    ...
    username=Field(
        validator.username,
        validator.lowercase,
        required=True)
    ...
    )
```


## Extensions

Extensions are additional data that can be fetched on demand.

The use case for extension is very similar to a `join`. It allows you to fetch
from any source additional data, and this data will be appended to your object.

Fields are only available for `fetch` and `fetch_one` methods.

```python
from sukimu.operations import Equal

@UserModel.extension('stats')
def stats(item, fields):
    # You will observe here that fields is an array that contains
    # 'source.url' and 'user.id'.
    return {'days': 10, 'additional_fields': fields}


@UserModel.extension('history')
def history(item, fields):
    return {'length': 20}


UserModel.create(id='random', username='michael')
resp = UserModel.fetch(
    username=Equal('michael'),
    fields=[
        'history',
        'stats.days',
        'stats.source.url',
        'stats.user.id'])

print(resp.message)
```

## Author

* Michael Ortali ([@xethorn](http://twitter.com/xethorn))
