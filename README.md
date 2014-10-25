Schema
======

* Licence: `MIT`

Schemas define the structure of the fields of your table. The schema handles
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
second is to write the correct query. For example:
``fetch_one(dict(username=Equal('michael')))``, will convert the
key into ```username__eq``` in dynamodb.

Author
======
* Michael Ortali ([@xethorn](https://github.com/xethorn))
