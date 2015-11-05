=====================
ElasticSearch - Datas
=====================

Mapping
=======

::

    $ curl 'http://127.0.0.1:9200/_mapping?pretty'

::

    {
      "widukind" : {
        "mappings" : {
          "datasets" : {
            "properties" : {
              "codeList" : {
                "properties" : {
                  "Borrowers' country" : {
                    "type" : "string"
                  },
                  "Borrowing sector" : {
                    "type" : "string"
                  },
                  "Frequency" : {
                    "type" : "string"
                  },
                  "Lending sector" : {
                    "type" : "string"
                  },
                  "Type of adjustment" : {
                    "type" : "string"
                  },
                  "Unit type" : {
                    "type" : "string"
                  },
                  "Valuation" : {
                    "type" : "string"
                  }
                }
              },
              "datasetCode" : {
                "type" : "string"
              },
              "docHref" : {
                "type" : "string"
              },
              "frequencies" : {
                "type" : "string"
              },
              "lastUpdate" : {
                "type" : "date",
                "format" : "dateOptionalTime"
              },
              "name" : {
                "type" : "string"
              },
              "provider" : {
                "type" : "string"
              }
            }
          },
          "series" : {
            "properties" : {
              "datasetCode" : {
                "type" : "string"
              },
              "dimensions" : {
                "properties" : {
                  "Borrowers' country" : {
                    "type" : "string"
                  },
                  "Borrowing sector" : {
                    "type" : "string"
                  },
                  "Frequency" : {
                    "type" : "string"
                  },
                  "Lending sector" : {
                    "type" : "string"
                  },
                  "Type of adjustment" : {
                    "type" : "string"
                  },
                  "Unit type" : {
                    "type" : "string"
                  },
                  "Valuation" : {
                    "type" : "string"
                  }
                }
              },
              "frequency" : {
                "type" : "string"
              },
              "key" : {
                "type" : "string"
              },
              "name" : {
                "type" : "string"
              },
              "provider" : {
                "type" : "string"
              }
            }
          }
        }
      },
      "widukind_test" : {
        "mappings" : { }
      }
    }
