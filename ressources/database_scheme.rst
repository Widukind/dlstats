pymongo automatically adds an _id field to any document inserted into the database.
A timestamp is built-in to the default MongoDB ObjectId. It serves as a field giving us the fetching date.
A MongoDate translates transparently in a DateTime in python.


INSEE
=====

A subcategory is different from a category in only one way : it contains INSEE identifiers on the INSEE website.

.. code-block::
  categories = {
    _id: int,
    name: string,
    url: string
  }

  subcategories = {
    _id: int,
    name: string,
    url: string
  }

  subcategories_belongs_to_category = {
    _id: int,
    _id_categories: int,
    _id_subcategories: int
  }

  series = {
    _id: int,
    INSEE_id: int,
    name: string,
    POST_request: string,
    options: (string)
  }

  values = {
    _id: int,
    values: int,
    date: MongoDate,
    release_date: MongoDate,
  }

  values_belongs_to_series = {
    _id: int,
    _id_values: int,
    _id_series
  }
