=============
Configuration
=============

Medium
======

The configuration of dlstats is achieved through editing of an INI file named dlstats. For example, on a UNIX platform, the user-specific configuration would be found in $HOME/.dlstats and the system configuration is in /etc. If the user executing dlstats has a personal configuration file, the system-wide configuration is simply ignored.

Structure
=========

The INI file is divided in sections, enclosed in square brackets.

MongoDB
_______
Those options are passed to the MongoClient instance used by dlstats and follow the pymongo API. Please refer to the pymongo documentation[1]_ for more information.

.. [1] http://api.mongodb.org/python/
