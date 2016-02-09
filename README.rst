Widukind - dlstats
==================

|Build Status| |Build Doc| |Coveralls|

**This is a database of international macroeconomic data collected on public web servers of statistical offices worldwide.**

Requires
--------

- MongoDB 3+
- Python 3.4
- `Widukind Common`_
- `Widukind Web`_ for WebUI (Optional)

Installation with Python
------------------------

See Dockerfile for installation example.

Installation with Docker
------------------------

Use `Widukind Docker`_ project with docker-compose or manual installation:

**Requires:**

* Docker 1.9+
* docker-compose 1.5+
* sudo right or root access

::

    docker run -d --name mongodb mongo \
      mongod --bind_ip 0.0.0.0 --smallfiles --noauth --directoryperdb
     
    git clone https://github.com/Widukind/dlstats.git
    
    cd dlstats
    
    docker build -t widukind/cli .    
    
    docker run -it --rm --link mongodb:mongodb \
      -e WIDUKIND_MONGODB_URL=mongodb://mongodb/widukind \
      widukind/cli dlstats --help

    # Tips: run dlstats client with alias
    alias dlstats="docker run -it --rm --link mongodb:mongodb -e WIDUKIND_MONGODB_URL=mongodb://mongodb/widukind widukind/cli dlstats"
    dlstats --help


.. |Build Status| image:: https://travis-ci.org/Widukind/dlstats.svg?branch=master
   :target: https://travis-ci.org/Widukind/dlstats
   :alt: Travis Build Status
   
.. |Build Doc| image:: https://readthedocs.org/projects/widukind-dlstats/badge/?version=latest
   :target: http://widukind-dlstats.readthedocs.org/en/latest/?badge=latest
   :alt: Documentation Status   
   
.. |Coveralls| image:: https://coveralls.io/repos/Widukind/dlstats/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/Widukind/dlstats?branch=master
   :alt: Coverage   

LICENSE
-------

GNU Affero General Public License version 3


.. _`Widukind Web`: https://github.com/Widukind/widukind-web
.. _`Widukind Docker`: https://github.com/Widukind/widukind-docker
.. _`Widukind Common`: https://github.com/Widukind/widukind-common
