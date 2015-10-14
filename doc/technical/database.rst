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
               children_id : [MongoID],
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
We name codes the nomenclature of attributes that defines atomically the time series. Those codes are only provided for exploration of the database. In the program, a time series is of course identified by its unique id. A document from the codes collection contains all the series related to this code. Consequently, it is possible to query for time series using a set of constraint on codes; at the application level, the client would differentiate all the series_id sets to only get the relevant time series.
We keep a pointer to the time series for better performances.

.. code:: javascript

 categories : {
               _id : MongoID,
               _id_journal : [MongoID],
               name : str,
               children_id : MongoID,
               url_tsv : str,
               url_dft : str,
               url_sdmx : str,
               flowRef : str,
               codes : {
                        _id_journal : MongoID,
                        name : str,
                        values : {
                                  key : str,
                                  description : str,
                                  series_id : [MongoID]
                                 }
                       }
              }

.. [3] http://epp.eurostat.ec.europa.eu/portal/page/portal/sdmx_web_services/getting_started/rest_sdmx_2.1

Time series
___________

The values are in a list. The position field in the revisions subcollection relates to the index of that list.

.. code:: javascript

 series : {
           _id : MongoID,
           _id_journal : MongoID,
           name : str,
           start_date : timestamp,
           end_date : timestamp,
           release_dates : [timestamp],
           values : [float64],
           frequency : str,
           revisions : {
                        value : float64,
                        position : int,
                        release_date : timestamp
                       },
           codes : {
                    name : str,
                    value : str
                   },
           categories_id : MongoID
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
- slow map/reduce

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

