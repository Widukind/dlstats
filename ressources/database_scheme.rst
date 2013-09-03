pymongo automatically adds an _id field to any document inserted into the database.
A timestamp is built-in to the default MongoDB ObjectId. It serves as a field giving us the fetching date.
A MongoDate translates transparently in a DateTime in python.

Databases can have different schemas for practical purposes. In the end, we want each object to provide the same attributes as INSSE at the application level.

INSEE
=====

A subcategory is different from a category in only one way : it contains INSEE identifiers on the INSEE website.

.. code-block::
  categories = {
    _id: int,
    name: string,
    url: string
    subcategories = {
      _id: int,
      name: string,
      url: string
    }
  }


  series = {
    _id: int,
    INSEE_id: int,
    name: string,
    POST_request: string,
    options: (string)
    values = {
      _id: int,
      values: int,
      date: MongoDate,
      release_date: MongoDate,
    }
  }
