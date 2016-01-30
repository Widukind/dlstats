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
      export    Export File commands.
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
      calendar  Display calendar for this provider
      datasets  Show datasets list
      datatree  Create or Update fetcher Data-Tree
      list      Show fetchers list
      report    Fetchers report
      run       Run Fetcher - All datasets or selected...
      search    Search in Series
      tags      Create or Update field tags
      
fetchers calendar
-----------------

.. code:: shell


    $ dlstats fetchers calendar --help

    Usage: dlstats fetchers calendar [OPTIONS]

      Display calendar for this provider
    
    Options:
      -v, --verbose                   Enables verbose mode.
      -D, --debug
      -l, --log-level [DEBUG|WARN|ERROR|INFO|CRITICAL]
                                      Logging level  [default: ERROR]
      --log-config PATH               Logging config filepath
      --mongo-url TEXT                URL for MongoDB connection.  [default:
                                      mongodb://localhost/widukind]
      -f, --fetcher [BIS|ESRI|EUROSTAT|FED|ECB|IMF|INSEE]
                                      Fetcher choice  [required]
      --help                          Show this message and exit.

**Example**
      
.. code:: shell

    $ dlstats fetchers calendar -f ECB

::

    ---------------------------------------------------------------------------------------------------------------------------
    Provider   | Dataset      | Action          | Type   | Date (yyyy-mm-dd hh:mn)
    ---------------------------------------------------------------------------------------------------------------------------
    ECB        | TGB          | update_node     | date   | 2016-02-01 - 10:00
    ECB        | MIR          | update_node     | date   | 2016-02-04 - 10:00
    ECB        | STP          | update_node     | date   | 2016-02-05 - 14:00
    ECB        | EXR          | update_node     | date   | 2016-02-08 - 10:00
    ECB        | SEC          | update_node     | date   | 2016-02-10 - 10:00
    ---------------------------------------------------------------------------------------------------------------------------
    
fetchers datasets
-----------------

.. code:: shell

    $ dlstats fetchers datasets
    
    Usage: dlstats fetchers datasets [OPTIONS]
    
      Show datasets list
    
    Options:
      -f, --fetcher [BIS|ESRI|EUROSTAT|FED|ECB|IMF|INSEE]
                                      Fetcher choice  [required]
      --help                          Show this message and exit.

**Example**
      
.. code:: shell

    $ dlstats fetchers datasets -f FED

::

    CHGDEL CHGDEL - Charge-off and Delinquency Rates
    CP CP - Commercial Paper
    E2 E.2 - Survey of Terms of Business Lending
    FOR FOR - Household Debt Service and Financial Obligations Ratios
    G17 G.17 - Industrial Production and Capacity Utilization
    G19 G.19 - Consumer Credit
    G20 G.20 - Finance Companies
    H10 G.5 / H.10 - Foreign Exchange Rates
    H15 H.15 - Selected Interest Rates
    H3 H.3 - Aggregate Reserves of Depository Institution and the Monetary Base
    H41 H.4.1 - Factors Affecting Reserve Balances
    H6 H.6 - Money Stock Measures
    H8 H.8 - Assets and Liabilities of Commercial Banks in the U.S.
    PRATES PRATES - Policy Rates
    SLOOS SLOOS - Senior Loan Officer Opinion Survey on Bank Lending Practices
    Z1 Z.1 - Financial Accounts of the United States


fetchers datatree
-----------------

.. code:: shell

    Usage: dlstats fetchers datatree [OPTIONS]
    
      Create or Update fetcher Data-Tree
    
    Options:
      -v, --verbose                   Enables verbose mode.
      -S, --silent                    Suppress confirm
      -D, --debug
      -l, --log-level [DEBUG|WARN|ERROR|INFO|CRITICAL]
                                      Logging level  [default: ERROR]
      --log-config PATH               Logging config filepath
      --mongo-url TEXT                URL for MongoDB connection.  [default:
                                      mongodb://localhost/widukind]
      --force                         Force update
      -f, --fetcher [INSEE|IMF|EUROSTAT|BIS|ESRI|ECB|FED]
                                      Fetcher choice  [required]
      --help                          Show this message and exit.
fetchers list
-------------

.. code:: shell

    $ dlstats fetchers list
    
    ----------------------------------------------------
    INSEE
    IMF
    ECB
    EUROSTAT
    BIS
    FED
    ESRI
    ----------------------------------------------------
      
fetchers report
---------------

.. code:: shell

    $ dlstats fetchers report --help
    
    Usage: dlstats fetchers report [OPTIONS]
    
      Fetchers report
    
    Options:
      --mongo-url TEXT                URL for MongoDB connection.  [default:
                                      mongodb://localhost/widukind]
      -f, --fetcher [EUROSTAT|IMF|ESRI|INSEE|BIS|FED|ECB]
                                      Fetcher choice
      --help                          Show this message and exit.


**Example**
      
.. code:: shell

    $ dlstats fetchers report

::

    ---------------------------------------------------------------------------------------------------------------------------
    MongoDB: mongodb://localhost/widukind :
    ---------------------------------------------------------------------------------------------------------------------------
    Provider   | Ver. | Dataset                        | Series     | Last Update     | First Download       | last Download
    ---------------------------------------------------------------------------------------------------------------------------
    ECB        |    4 | EXR                            |      10675 | 2016-01-27      | 2016-01-27 - 17:21   | 2016-01-27 - 17:21
    INSEE      |    3 | IPAMPA-2010                    |        300 | 2016-01-28      | 2016-01-28 - 12:53   | 2016-01-28 - 12:53
    INSEE      |    3 | IPC-1970-1980-ALIM             |        236 | 2016-01-28      | 2016-01-28 - 12:53   | 2016-01-28 - 12:53
    INSEE      |    3 | IPC-1970-1980-MANUF            |        159 | 2016-01-28      | 2016-01-28 - 12:53   | 2016-01-28 - 12:53
    INSEE      |    3 | IPC-1970-1980-SERV             |        130 | 2016-01-28      | 2016-01-28 - 12:53   | 2016-01-28 - 12:53
    INSEE      |    3 | IPC-1980-PDET                  |         98 | 2016-01-28      | 2016-01-28 - 12:54   | 2016-01-28 - 12:54
    INSEE      |    3 | IPI-2010-A21                   |         20 | 2016-01-27      | 2016-01-27 - 16:58   | 2016-01-27 - 16:58
    ECB        |    4 | EXR                            |      10675 | 2016-01-27      | 2016-01-27 - 17:21   | 2016-01-27 - 17:21
    FED        |    2 | CHGDEL                         |        264 | 2016-01-28      | 2016-01-28 - 18:33   | 2016-01-28 - 18:35
    FED        |    2 | CP                             |         64 | 2016-01-28      | 2016-01-28 - 17:16   | 2016-01-28 - 18:35
    FED        |    2 | E2                             |       2425 | 2016-01-28      | 2016-01-28 - 17:17   | 2016-01-28 - 18:35
    FED        |    2 | FOR                            |          4 | 2016-01-28      | 2016-01-28 - 17:17   | 2016-01-28 - 18:35
    FED        |    2 | G17                            |       2498 | 2016-01-28      | 2016-01-28 - 17:19   | 2016-01-28 - 18:37
    FED        |    2 | G19                            |         81 | 2016-01-28      | 2016-01-27 - 19:08   | 2016-01-28 - 18:37
    FED        |    2 | G20                            |        128 | 2016-01-28      | 2016-01-28 - 17:21   | 2016-01-28 - 18:37
    FED        |    2 | H10                            |         52 | 2016-01-28      | 2016-01-28 - 17:21   | 2016-01-28 - 18:37
    FED        |    2 | H15                            |        133 | 2016-01-28      | 2016-01-28 - 17:23   | 2016-01-28 - 18:39
    FED        |    2 | H3                             |         41 | 2016-01-28      | 2016-01-28 - 17:23   | 2016-01-28 - 18:39
    FED        |    2 | H6                             |         63 | 2016-01-28      | 2016-01-28 - 18:41   | 2016-01-28 - 19:09
    FED        |    2 | H8                             |       1135 | 2016-01-28      | 2016-01-28 - 18:42   | 2016-01-28 - 18:42
    FED        |    2 | PRATES                         |          2 | 2016-01-28      | 2016-01-28 - 18:42   | 2016-01-28 - 18:42
    FED        |    2 | SLOOS                          |        297 | 2016-01-28      | 2016-01-28 - 18:42   | 2016-01-28 - 18:42
    FED        |    2 | Z1                             |      30267 | 2016-01-28      | 2016-01-27 - 19:31   | 2016-01-28 - 18:51
    ESRI       |    2 | kdef-cy                        |         22 | 2015-12-04      | 2016-01-27 - 19:41   | 2016-01-27 - 19:41
    ESRI       |    2 | kdef-fy                        |         22 | 2015-12-04      | 2016-01-27 - 19:40   | 2016-01-27 - 19:40
    ESRI       |    2 | kdef-q                         |         22 | 2015-12-04      | 2016-01-27 - 19:41   | 2016-01-27 - 19:41
    ESRI       |    2 | kkiyo-jcy                      |         22 | 2015-12-04      | 2016-01-27 - 19:40   | 2016-01-27 - 19:40
    ESRI       |    2 | kkiyo-jfy                      |         22 | 2015-12-04      | 2016-01-27 - 19:41   | 2016-01-27 - 19:41
    BIS        |    3 | CBS                            |      87797 | 2016-01-19      | 2016-01-27 - 19:59   | 2016-01-27 - 19:59
    BIS        |    3 | CNFS                           |        958 | 2015-12-01      | 2016-01-27 - 20:00   | 2016-01-27 - 20:00
    BIS        |    3 | DSRP                           |         66 | 2015-12-01      | 2016-01-27 - 20:00   | 2016-01-27 - 20:00    
    ---------------------------------------------------------------------------------------------------------------------------

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
      -l, --log-level [DEBUG|WARN|ERROR|INFO|CRITICAL]
                                      Logging level  [default: ERROR]
      --log-config PATH               Logging config filepath
      --log-file PATH                 log file for output
      --mongo-url TEXT                URL for MongoDB connection.  [default:
                                      mongodb://localhost/widukind]
      --data-tree                     Update data-tree before run.
      -f, --fetcher [INSEE|IMF|BIS|ESRI|ECB|EUROSTAT|FED]
                                      Fetcher choice  [required]
      -d, --dataset TEXT              Run selected dataset only
      --help                          Show this message and exit.

**Example**

Load or update DSRP dataset for BIS:
      
.. code:: shell

    $ dlstats fetchers run -f BIS -d DSRP

fetchers search
---------------

.. code:: shell

    $ dlstats fetchers search --help
    
    Usage: dlstats fetchers search [OPTIONS]
    
      Search in Series
    
    Options:
      -v, --verbose                   Enables verbose mode.
      -S, --silent                    Suppress confirm
      -D, --debug
      -l, --log-level [DEBUG|WARN|ERROR|INFO|CRITICAL]
                                      Logging level  [default: ERROR]
      --log-config PATH               Logging config filepath
      --mongo-url TEXT                URL for MongoDB connection.  [default:
                                      mongodb://localhost/widukind]
      -t, --search-type [datasets|series]
                                      Search Type  [default: datasets]
      -f, --fetcher [IMF|INSEE|ECB|FED|BIS|EUROSTAT|ESRI]
                                      Fetcher choice
      -d, --dataset TEXT              Run selected dataset only
      -F, --frequency [W|M|Q|H|D|A]   Frequency choice
      -s, --search TEXT               Search text  [required]
      -l, --limit INTEGER             Result limit  [default: 20]
      --help                          Show this message and exit.    
    
fetchers tags
-------------

.. code:: shell

    $ dlstats fetchers tags --help

    Usage: dlstats fetchers tags [OPTIONS]
    
      Create or Update field tags
    
    Options:
      -v, --verbose                   Enables verbose mode.
      -S, --silent                    Suppress confirm
      -D, --debug
      -l, --log-level [DEBUG|WARN|ERROR|INFO|CRITICAL]
                                      Logging level  [default: ERROR]
      --log-config PATH               Logging config filepath
      --mongo-url TEXT                URL for MongoDB connection.  [default:
                                      mongodb://localhost/widukind]
      -f, --fetcher [FED|EUROSTAT|BIS|ESRI|IMF|INSEE|ECB]
                                      Fetcher choice  [required]
      -d, --dataset TEXT              Run selected dataset only
      -M, --max-bulk INTEGER          Max Bulk  [default: 20]
      -c, --collection [datasets|series|ALL]
                                      Collection  [default: ALL; required]
      -g, --aggregate                 Run aggregate tags after update.
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

dlstats export
==============

.. code:: shell

    $ dlstats export --help
    
    Usage: dlstats export [OPTIONS] COMMAND [ARGS]...
    
      Export File commands.
    
    Options:
      --help  Show this message and exit.
    
    Commands:
      csvfile  Download csvfile from one dataset.    

export csvfile
--------------

.. code:: shell

    $ dlstats export csvfile --help

    Usage: dlstats export csvfile [OPTIONS]
    
      Download csvfile from one dataset.
    
      Examples:
    
        dlstats export csvfile -S -p BIS -d DSRP --create

        widukind-dataset-bis-dsrp.csv not exist. creating...
        export to widukind-dataset-bis-dsrp.csv - created[2016-01-30-05:09:57]      
    
    Options:
      -v, --verbose                   Enables verbose mode.
      -S, --silent                    Suppress confirm
      -D, --debug
      -l, --log-level [DEBUG|WARN|ERROR|INFO|CRITICAL]
                                      Logging level  [default: ERROR]
      --log-config PATH               Logging config filepath
      --mongo-url TEXT                URL for MongoDB connection.  [default:
                                      mongodb://localhost/widukind]
      -p, --provider [ECB|IMF|INSEE|EUROSTAT|FED|ESRI|BIS]
                                      Provider Name  [required]
      -d, --dataset TEXT              Run selected dataset only  [required]
      -P, --filepath PATH             Export filepath
      --create                        Create csv file if not exist.
      --help                          Show this message and exit.