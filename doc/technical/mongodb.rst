===============
MongoDB Schemas
===============

.. contents:: **Table of Contents**
    :depth: 3
    :backlinks: none
    
categories
==========

Fields
------

Unique Constraint
:::::::::::::::::

:Fields: provider + categoryCode

id
::

:required: Yes
:unique: Yes
:type: ObjectID
:comments: Unique ID 

name
::::

:required: Yes ???
:unique: No ???
:type: String
:default value: null ???
:comments: ???

* Examples:
    * Catches by fishing area - historical data (1950-1999)
    * Soil erosion by water by NUTS 3 regions (data source: JRC)
    * Enterprises in high-tech sectors by NACE Rev.2 activity
    * Enterprises in high-tech sectors by NACE Rev.1.1 activity
    * Business statistics
    * High-technology trade
    * Data on employment at national level 

categoryCode
::::::::::::

:required: Yes ???
:unique: No
:type: String
:default value: null ???
:comments: ??? 

* Examples:
    * fish_ca_h
    * aei_pr_soiler
    * htec_eco_ent2
    * htec_eco_ent
    * htec_sti_pat
    * ipr_dfa_cres

provider
::::::::

:required: Yes
:unique: No 
:type: String
:comments: Name of Provider 

* Examples:
    * WorldBank
    * Eurostat
    * INSEE
    * IMF

children
::::::::

:required: No
:unique: No 
:type: Array of bson.objectid.ObjectId or null
:default value: [None]
:comments: ??? 

docHref
:::::::

:required: No
:unique: No 
:type: String
:default value: null
:comments: Not used 

lastUpdate
::::::::::

:required: No
:unique: No
:type: ISODate / datetime
:default value: null
:comments: ??? 

exposed
:::::::

:required: No ???
:unique: No
:type: Bool
:default value: false
:comments: ???

Examples
--------

.. code:: javascript

    {
        "_id": ObjectId('559d6f819f8f0807a98ee821'),
        "provider": "WorldBank",
        "docHref": null,
        "lastUpdate": null,
        "children": null,
        "categoryCode": "GEM",
        "exposed": false,
        "name": "GEM"
    },
    {
        "_id": ObjectId('559e40c29f8f081123ecd8f8'),
        "docHref": null,
        "categoryCode": "WEO",
        "provider": "IMF",
        "exposed": false,
        "name": "WEO",
        "lastUpdate": null,
        "children": null
    },       
    {
        "_id": ObjectId('559d6fc69f8f0807a98f0c2f'),
        "lastUpdate": null,
        "categoryCode": "ei_bcs_cs",
        "exposed": false,
        "children": [
            ObjectId('560287d79f8f0857111ce31d'),
            ObjectId('560287d79f8f0857111ce31e')
        ],
        "provider": "Eurostat",
        "docHref": null,
        "name": "Consumer surveys (source: DG ECFIN)"
    }    
    
    
providers
=========

Fields
------

Unique Constraint
:::::::::::::::::

:Fields: name

id
::

:required: Yes
:unique: Yes 
:type: ObjectID
:comments: Unique ID 

name
::::

:required: Yes
:unique: Yes
:type: String
:comments: Name of Provider

* Examples:
    * WorldBank
    * Eurostat
    * INSEE
    * IMF     

website
:::::::

:required: Yes ???
:unique: No
:type: String
:comments: URL of Provider Site

Examples
--------

.. code:: javascript

    {
        "_id": ObjectId('559d6f81bc00a4d38e44ed74'),
        "website": "http://www.worldbank.org/",
        "name": "WorldBank"
    },
    {
        "_id": ObjectId('559d6fc6bc00a4d38e44ed76'),
        "website": "http://ec.europa.eu/eurostat",
        "name": "Eurostat"
    }
    
datasets
========

Fields
------

Unique Constraint
:::::::::::::::::

:Fields: provider + datasetCode

id
::

:required: Yes
:unique: Yes 
:type: ObjectID
:comments: Unique ID 

provider
::::::::

:required: Yes
:unique: No 
:type: String
:comments: Name of Provider 

* Examples:
    * WorldBank
    * Eurostat
    * INSEE
    * IMF     

datasetCode
:::::::::::

:required: Yes ???
:unique: No ???
:type: String
:comments: ??? 

* Examples:
    * demo_pjanbroad
    * GEM
    * 158
    * 1427
    * 1430
    * WEO
    * namq_gdp_c
    * namq_gdp_k
    * namq_gdp_p
    * nama_gdp_c
    * nama_gdp_k
    * nama_gdp_p
    * namq_10_a10
    * namq_10_an6
    * lfsi_act_q
    * gov_10a_taxag
    * gov_10q_ggdebt
    * gov_10q_ggnfa
    * namq_10_a10_e
    * irt_st_q
    * namq_10_gdp

name
::::

:required: Yes ???
:unique: Yes ???
:type: String
:default value: null ???
:comments: ???

* Examples:
    * Population on 1 January by broad age group and sex
    * Global Economic Monirtor
    * Harmonised consumer price index - Base 2005 - French series by product according to the European classification
    * Producer price indices of French industry for all markets (base 2010) - Main aggregates
    * Producer price indices of French industry for the French market (base 2010) - Basic price - Main aggregates
    * World Economic Outlook
    * GDP and main components - Current prices
    * GDP and main components - volumes
    * GDP and main components - Price indices
    * Gross value added and income A*10 industry breakdowns
    * Gross fixed capital formation with AN_F6 asset breakdowns
    * Population, activity and inactivity - quarterly data
    * Main national accounts tax aggregates
    * Quarterly government debt
    * Quarterly non-financial accounts for general government
    * Employment A*10 industry breakdowns
    * Money market interest rates - quarterly data
    * GDP and main components (output, expenditure and income)

lastUpdate
::::::::::

:required: No ???
:unique: No
:type: ISODate / datetime
:default value: null
:comments: ??? 

docHref
:::::::

:required: No
:unique: No ??? 
:type: String
:default value: null
:comments: URL for Dataset ??? 

* Examples:
    * null
    * http://data.worldbank.org/data-catalog/global-economic-monitor
    * http://www.bdm.insee.fr/bdm2/documentationGroupe?codeGroupe=158
    * http://www.bdm.insee.fr/bdm2/documentationGroupe?codeGroupe=1427
    * http://www.bdm.insee.fr/bdm2/documentationGroupe?codeGroupe=1430
    * http://www.imf.org/external/ns/cs.aspx?id=28

dimensionList
:::::::::::::

:required: Yes
:unique: No
:type: dlstats.fetchers._commons.CodeDict (list of OrderedDict)
:default value: CodeDict()
:comments: ??? 

attributeList
:::::::::::::

:required: No
:unique: No 
:type: dlstats.fetchers._commons.CodeDict (list of OrderedDict)
:default value: CodeDict()
:comments: ???

notes
:::::

:required: No
:unique: No 
:type: String
:default value: empty string
:comments: ???

Examples
--------

.. code:: javascript

    {
        "_id": ObjectId('56016d84fab819e7b143892a'),
        "dimensionList": {
            "geo": [
                [
                    "EU28",
                    "European Union (28 countries)"
                ],
                [
                    "EU27",
                    "European Union (27 countries)"
                ],
                [
                    "EA19",
                    "Euro area (19 countries)"
                ],
                [
                    "EA18",
                    "Euro area (18 countries)"
                ],
                [
                    "BE",
                    "Belgium"
                ],
                [
                    "BG",
                    "Bulgaria"
                ],
                [
                    "CZ",
                    "Czech Republic"
                ],
                [
                    "DK",
                    "Denmark"
                ],
                [
                    "DE",
                    "Germany (until 1990 former territory of the FRG)"
                ],
                [
                    "DE_TOT",
                    "Germany (including former GDR)"
                ],
                [
                    "EE",
                    "Estonia"
                ],
                [
                    "IE",
                    "Ireland"
                ],
                [
                    "EL",
                    "Greece"
                ],
                [
                    "ES",
                    "Spain"
                ],
                [
                    "FR",
                    "France"
                ],
                [
                    "FX",
                    "France (metropolitan)"
                ],
                [
                    "HR",
                    "Croatia"
                ],
                [
                    "IT",
                    "Italy"
                ],
                [
                    "CY",
                    "Cyprus"
                ],
                [
                    "LV",
                    "Latvia"
                ],
                [
                    "LT",
                    "Lithuania"
                ],
                [
                    "LU",
                    "Luxembourg"
                ],
                [
                    "HU",
                    "Hungary"
                ],
                [
                    "MT",
                    "Malta"
                ],
                [
                    "NL",
                    "Netherlands"
                ],
                [
                    "AT",
                    "Austria"
                ],
                [
                    "PL",
                    "Poland"
                ],
                [
                    "PT",
                    "Portugal"
                ],
                [
                    "RO",
                    "Romania"
                ],
                [
                    "SI",
                    "Slovenia"
                ],
                [
                    "SK",
                    "Slovakia"
                ],
                [
                    "FI",
                    "Finland"
                ],
                [
                    "SE",
                    "Sweden"
                ],
                [
                    "UK",
                    "United Kingdom"
                ],
                [
                    "EEA31",
                    "European Economic Area (EU-28 plus IS, LI, NO)"
                ],
                [
                    "EEA30",
                    "European Economic Area (EU-27 plus IS, LI, NO)"
                ],
                [
                    "EFTA",
                    "European Free Trade Association"
                ],
                [
                    "IS",
                    "Iceland"
                ],
                [
                    "LI",
                    "Liechtenstein"
                ],
                [
                    "NO",
                    "Norway"
                ],
                [
                    "CH",
                    "Switzerland"
                ],
                [
                    "ME",
                    "Montenegro"
                ],
                [
                    "MK",
                    "Former Yugoslav Republic of Macedonia, the"
                ],
                [
                    "AL",
                    "Albania"
                ],
                [
                    "RS",
                    "Serbia"
                ],
                [
                    "TR",
                    "Turkey"
                ],
                [
                    "AD",
                    "Andorra"
                ],
                [
                    "BY",
                    "Belarus"
                ],
                [
                    "BA",
                    "Bosnia and Herzegovina"
                ],
                [
                    "XK",
                    "Kosovo (under United Nations Security Council Resolution 1244/99)"
                ],
                [
                    "MD",
                    "Moldova"
                ],
                [
                    "MC",
                    "Monaco"
                ],
                [
                    "RU",
                    "Russia"
                ],
                [
                    "SM",
                    "San Marino"
                ],
                [
                    "UA",
                    "Ukraine"
                ],
                [
                    "AM",
                    "Armenia"
                ],
                [
                    "AZ",
                    "Azerbaijan"
                ],
                [
                    "GE",
                    "Georgia"
                ]
            ],
            "freq": [
                [
                    "A",
                    "Annual"
                ],
                [
                    "S",
                    "Half-yearly, semester"
                ],
                [
                    "Q",
                    "Quarterly"
                ],
                [
                    "M",
                    "Monthly"
                ],
                [
                    "W",
                    "Weekly"
                ],
                [
                    "B",
                    "Business week"
                ],
                [
                    "D",
                    "Daily"
                ],
                [
                    "H",
                    "Hourly"
                ],
                [
                    "N",
                    "Minutely"
                ]
            ],
            "age": [
                [
                    "TOTAL",
                    "Total"
                ],
                [
                    "Y_LT15",
                    "Less than 15 years"
                ],
                [
                    "Y15-64",
                    "From 15 to 64 years"
                ],
                [
                    "Y_GE65",
                    "65 years or over"
                ],
                [
                    "UNK",
                    "Unknown"
                ]
            ],
            "sex": [
                [
                    "T",
                    "Total"
                ],
                [
                    "M",
                    "Males"
                ],
                [
                    "F",
                    "Females"
                ]
            ]
        },
        "lastUpdate": ISODate('2015-04-23T00:00:00.000Z'),
        "attributeList": {
            "obs_status": [
                [
                    "b",
                    "break in time series"
                ],
                [
                    "c",
                    "confidential"
                ],
                [
                    "d",
                    "definition differs (see metadata)"
                ],
                [
                    "e",
                    "estimated"
                ],
                [
                    "f",
                    "forecast"
                ],
                [
                    "i",
                    "see metadata (phased out)"
                ],
                [
                    "n",
                    "not significant"
                ],
                [
                    "p",
                    "provisional"
                ],
                [
                    "r",
                    "revised"
                ],
                [
                    "s",
                    "Eurostat estimate (phased out)"
                ],
                [
                    "u",
                    "low reliability"
                ],
                [
                    "z",
                    "not applicable"
                ]
            ],
            "time_format": [
                [
                    "P1Y",
                    "Annual"
                ],
                [
                    "P6M",
                    "Semi-annual"
                ],
                [
                    "P3M",
                    "Quarterly"
                ],
                [
                    "P1M",
                    "Monthly"
                ],
                [
                    "P7D",
                    "Weekly"
                ],
                [
                    "P1D",
                    "Daily"
                ],
                [
                    "PT1M",
                    "Minutely"
                ]
            ]
        },
        "name": "Population on 1 January by broad age group and sex",
        "provider": "Eurostat",
        "datasetCode": "demo_pjanbroad",
        "docHref": null
    }
    
series
======

Fields
------

Unique Constraint
:::::::::::::::::

:Fields: provider + datasetCode + key


id
::

:required: Yes
:unique: Yes 
:type: ObjectID
:comments: Unique ID 

provider
::::::::

:required: Yes
:unique: No 
:type: String
:comments: Name of Provider 

* Examples:
    * WorldBank
    * Eurostat
    * INSEE
    * IMF     

key
:::

:required: Yes
:unique: Yes 
:type: String
:comments: Unique key of Serie 

* Examples:
    * Q.PYP_MNAC.WDA.P3.IT
    * Q.PYP_MNAC.WDA.P3.LU
    * Q.PYP_MNAC.WDA.P3.LV
    * Q.PYP_MNAC.WDA.P31_S13.IT
    * Q.PYP_MNAC.WDA.P31_S13.LU
    * Q.PYP_MNAC.WDA.P31_S13.LV

name
::::

:required: Yes
:unique: Yes
:type: String
:comments: Unique name of Serie 

attributes
::::::::::

:required: No
:unique: No
:type: Dict
:comments: ???

datasetCode
:::::::::::

:required: Yes ???
:unique: No ???
:type: String
:comments: ??? 

* Examples:
    * GEM
    * nama_gdp_c
    * namq_gdp_c
    * 158
    * 1427
    * 1430
    * WEO
    * namq_gdp_k
    * namq_gdp_p
    * nama_gdp_k
    * nama_gdp_p
    * demo_pjanbroad
    * namq_10_a10
    * gov_10a_taxag
    * namq_10_an6
    * lfsi_act_q
    * gov_10q_ggdebt
    * gov_10q_ggnfa
    * namq_10_a10_e
    * irt_st_q
    * namq_10_gdp

dimensions
::::::::::

:required: Yes ???
:unique: No
:type: Dict
:comments: ??? 
        
startDate
:::::::::

:required: Yes ???
:unique: No
:type: Integer ???
:comments: ??? 
        
endDate
:::::::

:required: Yes ???
:unique: No
:type: Integer ???
:comments: ???

frequency
:::::::::

:required: Yes ???
:unique: No
:type: String
:comments: ???

* Examples:
    * A
    * M
    * Q
 
releaseDates
::::::::::::

:required: Yes ???
:unique: No
:type: Array
:comments: ??? 

revisions
:::::::::

:required: Yes ???
:unique: No
:type: Dict
:comments: ??? 

values
::::::

:required: Yes ???
:unique: No
:type: Array
:comments: ??? 
        
notes
:::::

:required: No ???
:unique: No
:type: String
:comments: ??? 

Example - IMF
-------------

.. code:: javascript

    {
        "_id": ObjectId('560154fe9f8f084db8e653a3'),
        "attributes": {
            "flag": [
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "e",
                "e",
                "e",
                "e",
                "e",
                "e",
                "e",
                "e"
            ]
        },
        "datasetCode": "WEO",
        "dimensions": {
            "Scale": "Billions",
            "WEO Country Code": "512",
            "Country": "AFG",
            "Units": "0",
            "Subject": "NGDP"
        },
        "endDate": 50,
        "frequency": "A",
        "key": "NGDP.AFG.0",
        "name": "Gross domestic product, current prices.Afghanistan.National currency",
        "notes": "Expressed in billions of national currency units . Expenditure-based GDP is total final expenditures at purchasers? prices (including the f.o.b. value of exports of goods and services), less the f.o.b. value of imports of goods and services. [SNA 1993]",
        "provider": "IMF",
        "releaseDates": [
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z'),
            ISODate('2015-04-01T00:00:00.000Z')
        ],
        "revisions": {
            "33": [
                {
                    "value": "1,148.113",
                    "releaseDates": ISODate('2014-10-01T00:00:00.000Z')
                }
            ],
            "34": [
                {
                    "value": "1,248.663",
                    "releaseDates": ISODate('2014-10-01T00:00:00.000Z')
                }
            ],
            "35": [
                {
                    "value": "1,378.499",
                    "releaseDates": ISODate('2014-10-01T00:00:00.000Z')
                }
            ],
            "36": [
                {
                    "value": "1,526.441",
                    "releaseDates": ISODate('2014-10-01T00:00:00.000Z')
                }
            ],
            "37": [
                {
                    "value": "1,682.614",
                    "releaseDates": ISODate('2014-10-01T00:00:00.000Z')
                }
            ],
            "38": [
                {
                    "value": "1,858.130",
                    "releaseDates": ISODate('2014-10-01T00:00:00.000Z')
                }
            ],
            "39": [
                {
                    "value": "2,057.319",
                    "releaseDates": ISODate('2014-10-01T00:00:00.000Z')
                }
            ]
        },
        "startDate": 10,
        "values": [
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "181.605",
            "220.013",
            "246.210",
            "304.926",
            "345.817",
            "427.495",
            "517.509",
            "607.227",
            "711.759",
            "836.222",
            "1,033.591",
            "1,114.649",
            "1,165.605",
            "1,250.023",
            "1,382.709",
            "1,535.283",
            "1,699.171",
            "1,884.765",
            "2,081.098"
        ]
    }
        
        
Example - WorldBank (GEM)
-------------------------

.. code:: javascript

    {
        "_id": ObjectId('55f927739f8f087fa959e3ed'),
        "values": [
            "",
            "0.736533",
            "0.68195",
            "0.714125",
            "0.666342",
            "0.840883",
            "0.881617",
            "1.02235",
            "1.041133",
            "1.085208",
            "1.222867",
            "1.304292",
            "1.345858",
            "1.480267",
            "2.010875",
            "1.582225",
            "1.327092",
            "1.581",
            "1.506467",
            "2.138275",
            "2.884017",
            "2.759967",
            "2.474358",
            "2.389625",
            "2.439892",
            "2.27315",
            "2.154142",
            "2.09215",
            "2.385942",
            "2.517075",
            "2.569058",
            "2.563233",
            "2.663358",
            "2.454575",
            "2.617542",
            "2.33285",
            "1.907025",
            "1.785242",
            "1.855433",
            "1.725658",
            "1.84335",
            "1.932108",
            "2.129333",
            "2.1048",
            "1.979733",
            "2.512563",
            "2.616948",
            "2.547255",
            "2.602967",
            "3.137989",
            "2.636496",
            "3.351451",
            "4.042094",
            "4.142297",
            "4.073449",
            "4.948254",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA"
        ],
        "key": "Commodity_Prices.Meat, beef, $/kg, nominal$.A",
        "startDate": -10,
        "releaseDates": [
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z'),
            ISODate('2015-07-08T11:24:24.000Z')
        ],
        "dimensions": {
            "Commodity": "4"
        },
        "name": "Commodity Prices; Meat, beef, $/kg, nominal$; Annual",
        "frequency": "A",
        "attributes": {},
        "endDate": 55,
        "provider": "WorldBank",
        "datasetCode": "GEM"
    }
