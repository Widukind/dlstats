========
Commands
========

Environment
===========

Les variables d'environnement peuvent être utilisés pour définir la valeur 
des options de la ligne de commande.

Toutes les variables de l'application, commence par **DLSTATS_**

**Example:**

.. code:: shell

    $ DLSTATS_DEBUG=True dlstats fetchers run -v -S -f BIS
    
    # Or:
    
    $ export DLSTATS_DEBUG=True
    $ dlstats fetchers run -v -S -f BIS
    
    # Is the same as:
    
    $ dlstats fetchers run --debug -v -S -f BIS

dlstats.client
==============

.. code:: shell

    $ dlstats --help

    Usage: dlstats [OPTIONS] COMMAND [ARGS]...
    
    Options:
      --version  Show the version and exit.
      --help     Show this message and exit.
    
    Commands:
      fetchers  Fetchers commands.
      mongo     MongoDB commands.


dlstats fetchers
================

.. code:: shell

    $ dlstats fetchers --help

    Usage: dlstats fetchers [OPTIONS] COMMAND [ARGS]...
    
      Fetchers commands.
    
    Options:
      --help  Show this message and exit.
    
    Commands:
      datasets  Show datasets list
      list      Show fetchers list
      report    Fetchers report
      run       Run Fetcher - All datasets or selected...
      
fetchers list
-------------

.. code:: shell

    $ dlstats fetchers list
    
    ----------------------------------------------------
    BIS
    INSEE
    BEA
    IMF
    EUROSTAT
    WB
    ----------------------------------------------------
    
fetchers datasets
-----------------

.. code:: shell

    $ dlstats fetchers datasets
    
    Usage: dlstats fetchers datasets [OPTIONS]
    
      Show datasets list
    
    Options:
      -f, --fetcher [INSEE|BIS|BEA|IMF|WB|EUROSTAT]
                                      Fetcher choice  [required]
      --help                          Show this message and exit.
      
fetchers report
---------------

.. code:: shell

    $ dlstats fetchers report --help
    
    Usage: dlstats fetchers report [OPTIONS]
    
      Fetchers report
    
    Options:
      --mongo-url TEXT  URL for MongoDB connection.  [default:
                        mongodb://127.0.0.1:27017/widukind]
      --help            Show this message and exit.

**Example**
      
.. code:: shell

    $ dlstats fetchers report

::

    -----------------------------------------------------------------------------------------
    MongoDB: mongodb://127.0.0.1:27017/widukind :
    -----------------------------------------------------------------------------------------
    Provider             | Dataset                        | Series     | last Update
    -----------------------------------------------------------------------------------------
    WorldBank            | GEM                            |       9346 | 2015-09-15 21:38:18
    Eurostat             | demo_pjanbroad                 |        834 | 2015-04-23 00:00:00
    Eurostat             | gov_10a_taxag                  |      94512 | 2015-07-01 00:00:00
    Eurostat             | gov_10q_ggnfa                  |      19218 | 2015-07-01 00:00:00
    Eurostat             | namq_10_a10_e                  |      24265 | 2015-09-18 00:00:00
    Eurostat             | namq_gdp_p                     |      11956 | 2015-04-13 00:00:00
    INSEE                | 1427                           |         37 | 1900-01-01 00:00:00
    INSEE                | 158                            |        393 | 1900-01-01 00:00:00
    IMF                  | WEO                            |      10936 | 2015-04-01 00:00:00
    BIS                  | CNFS                           |        938 | 2015-09-16 09:34:20
    BIS                  | DSRP                           |         66 | 2015-09-16 08:47:38
    -----------------------------------------------------------------------------------------
    
fetchers run
------------

.. code:: shell

    $ dlstats fetchers run --help

    Usage: dlstats fetchers run [OPTIONS]
    
      Run Fetcher - All datasets or selected dataset
    
    Options:
      -v, --verbose                   Enables verbose mode.
      -S, --silent                    Suppress confirm
      -D, --debug
      --mongo-url TEXT                URL for MongoDB connection.  [default:
                                      mongodb://127.0.0.1:27017/widukind]
      -f, --fetcher [EUROSTAT|BEA|BIS|IMF|INSEE|WB]
                                      Fetcher choice  [required]
      -d, --dataset TEXT              Run selected dataset only
      --help                          Show this message and exit.

dlstats mongo
=============

.. code:: shell

    $ dlstats mongo --help
    
    Usage: dlstats mongo [OPTIONS] COMMAND [ARGS]...
    
      MongoDB commands.
    
    Options:
      --help  Show this message and exit.
    
    Commands:
      check          Verify connection
      check-schemas  Check datas in DB with schemas
      clean          Delete MongoDB collections
      reindex        Reindex collections    

mongo check
-----------

.. code:: shell

    $ dlstats mongo check --help

    Usage: dlstats mongo check [OPTIONS]
    
      Verify connection
    
    Options:
      -v, --verbose     Enables verbose mode.
      --pretty          Pretty display.
      --mongo-url TEXT  URL for MongoDB connection.  [default:
                        mongodb://127.0.0.1:27017/widukind]
      --help            Show this message and exit.
      
**Example:**

.. code:: shell

    $ dlstats mongo check

::

    ------------------------------------------------------
    Connection OK
    ------------------------------------------------------
    pymongo version : 3.1
    -------------------- Server Infos --------------------
    {'allocator': 'system',
     'bits': 64,
     'compilerFlags': '/TP /nologo /EHsc /W3 /wd4355 /wd4800 /wd4267 /wd4244 /Z7 '
                      '/errorReport:none /O2 /Oy- /MT /GL',
     'debug': False,
     'gitVersion': '05bebf9ab15511a71bfbded684bb226014c0a553',
     'javascriptEngine': 'V8',
     'loaderFlags': '/nologo /LTCG /DEBUG /LARGEADDRESSAWARE '
                    '/NODEFAULTLIB:MSVCPRT',
     'maxBsonObjectSize': 16777216,
     'ok': 1.0,
     'sysInfo': 'windows sys.getwindowsversion(major=6, minor=1, build=7601, '
                "platform=2, service_pack='Service Pack 1') "
                'BOOST_LIB_VERSION=1_49',
     'version': '2.4.14',
     'versionArray': [2, 4, 14, 0]}
    -------------------- Host Infos ----------------------
    {'extra': {'pageSize': 4096},
     'ok': 1.0,
     'os': {'name': 'Microsoft Windows 7',
            'type': 'Windows',
            'version': '6.1 SP1 (build 7601)'},
     'system': {'cpuAddrSize': 64,
                'cpuArch': 'x86_64',
                'currentTime': datetime.datetime(2015, 11, 5, 7, 9, 6, 766000),
                'hostname': 'admin-VAIO',
                'memSizeMB': 6125,
                'numCores': 4,
                'numaEnabled': False}}
    ------------------------------------------------------
    
mongo check-schemas
-------------------

.. code:: shell

    $ dlstats mongo check-schemas --help

    Usage: dlstats mongo check-schemas [OPTIONS]
    
      Check datas in DB with schemas
    
    Options:
      -v, --verbose             Enables verbose mode.
      -S, --silent              Suppress confirm
      -D, --debug
      --mongo-url TEXT          URL for MongoDB connection.  [default:
                                mongodb://127.0.0.1:27017/widukind]
      -M, --max-errors INTEGER  [default: 0]
      --help                    Show this message and exit.

**Example:**

.. code:: shell

    dlstats mongo check-schemas --max-errors 5 --silent

::

    Attention, opération très longue
    check series...
    Max error attempt. Skip test !
    check categories...
    Max error attempt. Skip test !
    check datasets...
    Max error attempt. Skip test !
    check providers...
    -------------------------------------------------------------------
    Collection           | Count      | Verified   | Errors     | Time
    series               |     315032 |       9826 |          5 | 10.488
    categories           |       6875 |       1200 |          5 | 0.335
    datasets             |         23 |          9 |          5 | 0.012
    providers            |          5 |          5 |          0 | 0.001
    -------------------------------------------------------------------
    time elapsed : 10.841 seconds
  
mongo clean
-----------

.. warning:: Dangerous operation !

.. code:: shell

    $ dlstats mongo clean --help
    
    Usage: dlstats mongo clean [OPTIONS]
    
      Delete MongoDB collections
    
    Options:
      -v, --verbose     Enables verbose mode.
      -S, --silent      Suppress confirm
      -D, --debug
      --mongo-url TEXT  URL for MongoDB connection.  [default:
                        mongodb://127.0.0.1:27017/widukind]
      --help            Show this message and exit.
      
mongo reindex
-------------

.. warning:: All Writes operations is blocked pending run !

.. code:: shell

    $ dlstats mongo reindex --help
      
    Usage: dlstats mongo reindex [OPTIONS]
    
      Reindex collections
    
    Options:
      -v, --verbose     Enables verbose mode.
      -S, --silent      Suppress confirm
      -D, --debug
      --mongo-url TEXT  URL for MongoDB connection.  [default:
                        mongodb://127.0.0.1:27017/widukind]
      --help            Show this message and exit.

