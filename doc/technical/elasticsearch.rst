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
                  "Commodity" : {
                    "type" : "string"
                  },
                  "Commodity Prices" : {
                    "type" : "string"
                  },
                  "Correction" : {
                    "type" : "string"
                  },
                  "Country" : {
                    "type" : "string"
                  },
                  "Country Code" : {
                    "type" : "string"
                  },
                  "FREQ" : {
                    "type" : "string"
                  },
                  "GEO" : {
                    "type" : "string"
                  },
                  "Geographical area" : {
                    "type" : "string"
                  },
                  "INDIC_NA" : {
                    "type" : "string"
                  },
                  "ISO" : {
                    "type" : "string"
                  },
                  "Mode of representation" : {
                    "type" : "string"
                  },
                  "Power" : {
                    "type" : "string"
                  },
                  "Reference period" : {
                    "type" : "string"
                  },
                  "S_ADJ" : {
                    "type" : "string"
                  },
                  "Scale" : {
                    "type" : "string"
                  },
                  "Subject" : {
                    "type" : "string"
                  },
                  "Subject Code" : {
                    "type" : "string"
                  },
                  "TIME_FORMAT" : {
                    "type" : "string"
                  },
                  "Treatment of taxes" : {
                    "type" : "string"
                  },
                  "UNIT" : {
                    "type" : "string"
                  },
                  "Unit of measurement" : {
                    "type" : "string"
                  },
                  "Units" : {
                    "type" : "string"
                  },
                  "WEO Country Code" : {
                    "type" : "string"
                  },
                  "age" : {
                    "type" : "string"
                  },
                  "asset10" : {
                    "type" : "string"
                  },
                  "code1" : {
                    "type" : "string"
                  },
                  "code2" : {
                    "type" : "string"
                  },
                  "code3" : {
                    "type" : "string"
                  },
                  "code4" : {
                    "type" : "string"
                  },
                  "code5" : {
                    "type" : "string"
                  },
                  "code6" : {
                    "type" : "string"
                  },
                  "country" : {
                    "type" : "string"
                  },
                  "freq" : {
                    "type" : "string"
                  },
                  "geo" : {
                    "type" : "string"
                  },
                  "indic_em" : {
                    "type" : "string"
                  },
                  "indic_na" : {
                    "type" : "string"
                  },
                  "intrt" : {
                    "type" : "string"
                  },
                  "na_item" : {
                    "type" : "string"
                  },
                  "nace_r2" : {
                    "type" : "string"
                  },
                  "s_adj" : {
                    "type" : "string"
                  },
                  "sector" : {
                    "type" : "string"
                  },
                  "sex" : {
                    "type" : "string"
                  },
                  "unit" : {
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
              "created" : {
                "type" : "date",
                "format" : "dateOptionalTime"
              },
              "datasetCode" : {
                "type" : "string"
              },
              "dimensions" : {
                "properties" : {
                  "Commodity" : {
                    "type" : "string"
                  },
                  "Commodity Prices" : {
                    "type" : "string"
                  },
                  "Correction" : {
                    "type" : "string"
                  },
                  "Country" : {
                    "type" : "string"
                  },
                  "Country Code" : {
                    "type" : "string"
                  },
                  "FREQ" : {
                    "type" : "string"
                  },
                  "GEO" : {
                    "type" : "string"
                  },
                  "Geographical area" : {
                    "type" : "string"
                  },
                  "INDIC_NA" : {
                    "type" : "string"
                  },
                  "ISO" : {
                    "type" : "string"
                  },
                  "Mode of representation" : {
                    "type" : "string"
                  },
                  "Power" : {
                    "type" : "string"
                  },
                  "Reference period" : {
                    "type" : "string"
                  },
                  "S_ADJ" : {
                    "type" : "string"
                  },
                  "Scale" : {
                    "type" : "string"
                  },
                  "Subject" : {
                    "type" : "string"
                  },
                  "Subject Code" : {
                    "type" : "string"
                  },
                  "TIME_FORMAT" : {
                    "type" : "string"
                  },
                  "Treatment of taxes" : {
                    "type" : "string"
                  },
                  "UNIT" : {
                    "type" : "string"
                  },
                  "Unit of measurement" : {
                    "type" : "string"
                  },
                  "Units" : {
                    "type" : "string"
                  },
                  "WEO Country Code" : {
                    "type" : "string"
                  },
                  "age" : {
                    "type" : "string"
                  },
                  "asset10" : {
                    "type" : "string"
                  },
                  "code1" : {
                    "type" : "string"
                  },
                  "code2" : {
                    "type" : "string"
                  },
                  "code3" : {
                    "type" : "string"
                  },
                  "code4" : {
                    "type" : "string"
                  },
                  "code5" : {
                    "type" : "string"
                  },
                  "code6" : {
                    "type" : "string"
                  },
                  "country" : {
                    "type" : "string"
                  },
                  "freq" : {
                    "type" : "string"
                  },
                  "geo" : {
                    "type" : "string"
                  },
                  "indic_em" : {
                    "type" : "string"
                  },
                  "indic_na" : {
                    "type" : "string"
                  },
                  "intrt" : {
                    "type" : "string"
                  },
                  "na_item" : {
                    "type" : "string"
                  },
                  "nace_r2" : {
                    "type" : "string"
                  },
                  "s_adj" : {
                    "type" : "string"
                  },
                  "sector" : {
                    "type" : "string"
                  },
                  "sex" : {
                    "type" : "string"
                  },
                  "unit" : {
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
      }
