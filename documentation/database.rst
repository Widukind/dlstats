========
Database
========

Specification
=============

dlstats stores information from various statistical providers. The main goal is to keep up-to-date time series that are useful to the economist as well as their historical revisions.

Structure
=========

The database structure is described in bson[1]_.

Journal
_______
On top of MongoDB internal journaling mechanics, we keep a reference of all operations impacting the database. The method field stores the name of the method from dlstats.

.. code:: javascript

 journal : {
               _id : MongoID,
               method : str,
               arguments : []
              }


Categories
__________
Generic schema
--------------
Time series are organized in a tree of categories. Each node stores a reference to the node's children. It provides a simple and efficient solution to tree storage[2]_.

.. code:: javascript

 categories : {
               _id : MongoID,
               _id_journal : MongoID,
               name : str,
               children_id : MongoID,
               series_id : [MongoID]
              }

.. [1] http://www.bsonspec.org
.. [2] http://docs.mongodb.org/manual/tutorial/model-tree-structures/

Metadata
--------
The metadata differs across statistical providers. We add the corresponding fields when needed.

Eurostat
~~~~~~~~
For eurostat, we add a number of URLs for accessing the raw tsv, dft or sdmx files. Also, there is a field for the flowRef identifying the dataflow[3]_.

.. code:: javascript

 categories : {
               _id : MongoID,
               _id_journal : MongoID,
               name : str,
               children_id : MongoID,
               codes_id : [MongoID],
               series_id : [MongoID],
               url_tsv : str,
               url_dft : str,
               url_sdmx : str,
               flowRef : str
              }

.. [3] http://epp.eurostat.ec.europa.eu/portal/page/portal/sdmx_web_services/getting_started/rest_sdmx_2.1

Codes
_____
We name codes the nomenclature of attributes that defines atomically the time series. Those codes are only provided for exploration of the database. In the program, a time series is of course identified by its unique id. A document from the codes collection contains all the series related to this code. Consequently, it is possible to query for time series using a set of constraint on codes; at the application level, the client would differentiate all the series_id sets to only get the relevant time series.
Codes are not shared across categories. For example, it is certain that the FR code would contain a very large number of series. Nonetheless, each category containing series should have its own FR code. We don't store the huge list of series associated with FR.

.. code:: javascript

 codes : {
          _id : MongoID,
          _id_journal : MongoID,
          name : str,
          values : {
                    _id : MongoID,
                    value : str,
                    series_id : [MongoID]
                   }
         }

Time series
___________

A time series stores the codes restrictions it enforces, the categories it belongs to and the actual numerical data. The time series itself is a subcollection called data. It stores date/value pairs along with their revisions (if needed). Some data may be stored in two different places, depending on the statistical provider. For example, the frequency may also be found in the codes.

.. code:: javascript

 series : {
           _id : MongoID,
           _id_journal : MongoID,
           name : str,
           start_date : timestamp,
           end_date : timestamp,
           values : [float64],
           frequency : str
           revisions : {
                        _id : MongoID,
                        value : float64,
                        position : int
                       },
           codes_id : [MongoID],
           categories_id : [MongoID]
          }


Implementation
==============

MongoDB
_______
Pros
----
- simple (from a developer perspective)
- large number of drivers
- no ORM headache
- painless sharding
- very large user base
- decent documentation

Cons
----
- immature (mongodb 1.x was scary, 2.x is stable)
- complex configuration, lot of fine-tuning required

Impact on the structure
-----------------------
Growing documents impact performance and should be avoided. Preallocation can alleviate the issue. Alternatively, setting the padding to a higher value may help but comes with a memory cost.

Large number of keys are bad because MongoDB isn't Python. Collections aren't indexed with hash tables; if the collection has a large number of keys, mongoDB has to do a large number of comparisons to execute a query. In case of reading performance issues, normalization should improve the results.

HDF5
____
Better than all the other solutions as long as everything is loaded in RAM. Unfit for our job,

Cassandra
_________
Pros
----

- supported by the Apache Software Foundation
- excellent write performances

