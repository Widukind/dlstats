# -*- coding: utf-8 -*-

import os
import logging
import zipfile

from dlstats.fetchers._commons import Fetcher, Datasets, Providers, SeriesIterator
from dlstats.utils import Downloader, clean_datetime, clean_dict, clean_key
from dlstats.xml_utils import (XMLStructure_1_0 as XMLStructure, 
                               XMLData_1_0_FED as XMLData,
                               dataset_converter)

VERSION = 3

FREQUENCIES_SUPPORTED = [
    "M", 
    "A", 
    "D", 
    "Q", 
    "W-SUN",
    "W-MON",
    "W-TUE",
    "W-WED",
    "W-THU",
    "W-FRI",
    "W-SAT",
]
FREQUENCIES_REJECTED = []

logger = logging.getLogger(__name__)

DATASETS = {
    'H15': {
        'name': 'H.15 Selected Interest Rates - Selected Interest Rates',
        'doc_href': 'http://www.federalreserve.gov/releases/H15/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&filetype=zip',
        'dsd_id': 'H15-H15',
    },
    'H15-DISCONTINUED': {
        'name': 'H.15 Selected Interest Rates - Discontinued series from the H.15',
        'doc_href': 'http://www.federalreserve.gov/releases/H15/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&filetype=zip',
        'dsd_id': 'H15-discontinued',
    },
    'CHGDEL': {
        'name': 'Charge-off and Delinquency Rates - Charge-off and delinquency rates',
        'doc_href': 'http://www.federalreserve.gov/releases/CHGDEL/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=CHGDEL&filetype=zip',
        'dsd_id': 'CHGDEL-CHGDEL',
    },
    'PRATES': {
        'name': 'Policy Rates - Policy Rates',
        'doc_href': 'http://www.federalreserve.gov/releases/PRATES/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=PRATES&filetype=zip',
        'dsd_id': 'PRATES-PRATES_POLICY_RATES',
    },
    'CP-RATES': {
        'name': 'Commercial paper - Commercial Paper Rates',
        'doc_href': 'http://www.federalreserve.gov/releases/CP/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=CP&filetype=zip',
        'dsd_id': 'CP-RATES',
    },
    'CP-VOL': {
        'name': 'Commercial paper - Volumes Statistics for Commercial Paper Issuance',
        'doc_href': 'http://www.federalreserve.gov/releases/CP/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=CP&filetype=zip',
        'dsd_id': 'CP-VOL',
    },
    'CP-OUTST': {
        'name': 'Commercial paper - Commercial Paper Outstandings',
        'doc_href': 'http://www.federalreserve.gov/releases/CP/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=CP&filetype=zip',
        'dsd_id': 'CP-OUTST',
    },
    'CP-OUTST-OLD': {
        'name': 'Commercial paper - Commercial Paper Outstandings - Old Structure',
        'doc_href': 'http://www.federalreserve.gov/releases/CP/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=CP&filetype=zip',
        'dsd_id': 'CP-OUTST_OLD',
    },
    'CP-OUTST-YREND': {
        'name': 'Commercial paper - Year-end Commercial Paper Outstandings (maturing after December 31)',
        'doc_href': 'http://www.federalreserve.gov/releases/CP/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=CP&filetype=zip',
        'dsd_id': 'CP-OUTST_YREND',
    },
    'CP-RATES-OLD': {
        'name': 'Commercial paper - Commercial Paper Rates - based on dealer survey data',
        'doc_href': 'http://www.federalreserve.gov/releases/CP/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=CP&filetype=zip',
        'dsd_id': 'CP-RATES_OLD',
    },
    'H3': {
        'name': 'Aggregate Reserves of Depository Institutions and the Monetary Base - H.3 Aggregate Reserves of Depository Institutions and the Monetary Base',
        'doc_href': 'http://www.federalreserve.gov/releases/H3/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H3&filetype=zip',
        'dsd_id': 'H3-H3',
    },
    'H3-DISCONTINUED': {
        'name': 'Aggregate Reserves of Depository Institutions and the Monetary Base - H.3 Discontinued Break Adj, Seasonally Adj Monetary Base and Related Items',
        'doc_href': 'http://www.federalreserve.gov/releases/H3/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H3&filetype=zip',
        'dsd_id': 'H3-H3_DISCONTINUED',
    },
    'Z1': {
        'name': 'Flow of Funds Z.1 - Financial Accounts of the United States - Z.1',
        'doc_href': 'http://www.federalreserve.gov/releases/Z1/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=Z1&filetype=zip',
        'dsd_id': 'Z.1-Z1',
    },
    'H8': {
        'name': 'H.8 Assets and Liabilities of Commercial Banks in the United States - Assets and Liabilities of Commercial Banks in the U.S.',
        'doc_href': 'http://www.federalreserve.gov/releases/H8/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H8&filetype=zip',
        'dsd_id': 'H8-H8',
    },
    'H41': {
        'name': 'Factors Affecting Reserve Balances (H.4.1) - H41 Factors Affecting Reserve Balances of Depository Institutions and Condition Statement of Federal Reserve Banks',
        'doc_href': 'http://www.federalreserve.gov/releases/H41/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H41&filetype=zip',
        'dsd_id': 'H41-H41',
    },
    'G19-CCOUT': {
        'name': 'G.19 - Consumer Credit - Consumer Credit Outstanding',
        'doc_href': 'http://www.federalreserve.gov/releases/G19/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G19&filetype=zip',
        'dsd_id': 'G19-CCOUT',
    },
    'G19-TERMS': {
        'name': 'G.19 - Consumer Credit - Terms of Credit Outstanding',
        'doc_href': 'http://www.federalreserve.gov/releases/G19/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G19&filetype=zip',
        'dsd_id': 'G19-TERMS',
    },
    'FOR': {
        'name': 'Household Debt Service and Financial Obligations Ratios - Financial Obligations Ratio',
        'doc_href': 'http://www.federalreserve.gov/releases/FOR/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=FOR&filetype=zip',
        'dsd_id': 'FOR-FOR',
    },
    'G20-OWNED': {
        'name': 'G.20 - Finance Companies - Owned and Managed Receivables',
        'doc_href': 'http://www.federalreserve.gov/releases/G20/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G20&filetype=zip',
        'dsd_id': 'G20-OWNED',
    },
    'G20-TERMS': {
        'name': 'G.20 - Finance Companies - Terms of Credit',
        'doc_href': 'http://www.federalreserve.gov/releases/G20/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G20&filetype=zip',
        'dsd_id': 'G20-TERMS',
    },
    'G20-HIST': {
        'name': 'G.20 - Finance Companies - Balance Sheet',
        'doc_href': 'http://www.federalreserve.gov/releases/G20/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G20&filetype=zip',
        'dsd_id': 'G20-HIST',
    },
    'H6-M1': {
        'name': 'Money Stock Measures - H6 M1 Money Stock Measure',
        'doc_href': 'http://www.federalreserve.gov/releases/H6/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H6&filetype=zip',
        'dsd_id': 'H6-H6_M1',
    },
    'H6-M2': {
        'name': 'Money Stock Measures - H6 M2 Money Stock Measure',
        'doc_href': 'http://www.federalreserve.gov/releases/H6/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H6&filetype=zip',
        'dsd_id': 'H6-H6_M2',
    },
    'H6-M3-DISCONTINUED': {
        'name': 'Money Stock Measures - H6 M3 Money Stock Measure - Discontinued (Data frozen as of March 23, 2006)',
        'doc_href': 'http://www.federalreserve.gov/releases/H6/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H6&filetype=zip',
        'dsd_id': 'H6-H6M3_DISCONTINUED',
    },
    'H6-MEMO': {
        'name': 'Money Stock Measures - H6 Memorandum Items',
        'doc_href': 'http://www.federalreserve.gov/releases/H6/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H6&filetype=zip',
        'dsd_id': 'H6-H6_MEMO',
    },
    'H10': {
        'name': 'G.5/H.10 - Foreign Exchange Rates - Foreign Exchange Rates',
        'doc_href': 'http://www.federalreserve.gov/releases/H10/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H10&filetype=zip',
        'dsd_id': 'H10-H10',
    },
    'SLOOS': {
        'name': 'Senior Loan Officer Opinion Survey on Bank Lending Practices - Standards, terms, and demand',
        'doc_href': 'http://www.federalreserve.gov/releases/SLOOS/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=SLOOS&filetype=zip',
        'dsd_id': 'SLOOS-SLOOS',
    },
    'SLOOS-REASONS': {
        'name': 'Senior Loan Officer Opinion Survey on Bank Lending Practices - Reasons for changes to loan conditions',
        'doc_href': 'http://www.federalreserve.gov/releases/SLOOS/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=SLOOS&filetype=zip',
        'dsd_id': 'SLOOS-REASONS',
    },
    'SLOOS-DISCONTINUED': {
        'name': 'Senior Loan Officer Opinion Survey on Bank Lending Practices - Discontinued standards, terms, and demand',
        'doc_href': 'http://www.federalreserve.gov/releases/SLOOS/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=SLOOS&filetype=zip',
        'dsd_id': 'SLOOS-DISCONTINUED',
    },
    'G17-IP-MAJOR-INDUSTRY-GROUPS': {
        'name': 'Industrial Production - Industrial Production: Major Industry Groups',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-IP_MAJOR_INDUSTRY_GROUPS',
    },
    'G17-IP-DURABLE-GOODS-DETAIL': {
        'name': 'Industrial Production - Industrial Production: Durable Goods Detail',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-IP_DURABLE_GOODS_DETAIL',
    },
    'G17-IP-NONDURABLE-GOODS-DETAIL': {
        'name': 'Industrial Production - Industrial Production: Nondurable Goods Detail',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-IP_NONDURABLE_GOODS_DETAIL',
    },
    'G17-IP-MINING-AND-UTILITY-DETAIL': {
        'name': 'Industrial Production - Industrial Production: Mining and Utility Detail',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-IP_MINING_AND_UTILITY_DETAIL',
    },
    'G17-IP-MARKET-GROUPS': {
        'name': 'Industrial Production - Industrial Production: Market Groups',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-IP_MARKET_GROUPS',
    },
    'G17-IP-SPECIAL-AGGREGATES': {
        'name': 'Industrial Production - Industrial Production: Special Aggregates',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-IP_SPECIAL_AGGREGATES',
    },
    'G17-IP-GROSS-VALUE-STAGE-OF-PROCESS-GROUPS': {
        'name': 'Industrial Production - Industrial Production: Gross Value Stage of Process Groups',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-IP_GROSS_VALUE_STAGE_OF_PROCESS_GROUPS',
    },
    'G17-MVA': {
        'name': 'Industrial Production - Motor Vehicle Assemblies',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-MVA',
    },
    'G17-DIFF': {
        'name': 'Industrial Production - Diffusion Indexes of Industrial Production',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-DIFF',
    },
    'G17-CAPUTL': {
        'name': 'Industrial Production - Capacity Utilization',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-CAPUTL',
    },
    'G17-CAP': {
        'name': 'Industrial Production - Industrial Capacity',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-CAP',
    },
    'G17-GVIP': {
        'name': 'Industrial Production - Industrial Production: Gross Value of Products',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-GVIP',
    },
    'G17-RIW': {
        'name': 'Industrial Production - Relative Importance Weights for Seasonally Adjusted IP',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-RIW',
    },
    'G17-KW': {
        'name': 'Industrial Production - Electric Power Use: Manufacturing and Mining',
        'doc_href': 'http://www.federalreserve.gov/releases/G17/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&filetype=zip',
        'dsd_id': 'G17-KW',
    },
    'E2-MATURITY-RISK': {
        'name': 'E.2 Survey of Terms of Business Lending - Maturity/repricing interval and risk of loans',
        'doc_href': 'http://www.federalreserve.gov/releases/E2/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=E2&filetype=zip',
        'dsd_id': 'E2-MATURITY_RISK',
    },
    'E2-SIZE': {
        'name': 'E.2 Survey of Terms of Business Lending - Size of loan ($ thousands)',
        'doc_href': 'http://www.federalreserve.gov/releases/E2/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=E2&filetype=zip',
        'dsd_id': 'E2-SIZE',
    },
    'E2-BASERATE': {
        'name': 'E.2 Survey of Terms of Business Lending - Base rate of loan',
        'doc_href': 'http://www.federalreserve.gov/releases/E2/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=E2&filetype=zip',
        'dsd_id': 'E2-BASERATE',
    },
    'E2-TERMS-SET': {
        'name': 'E.2 Survey of Terms of Business Lending - Time pricing terms were set and loan commitment status',
        'doc_href': 'http://www.federalreserve.gov/releases/E2/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=E2&filetype=zip',
        'dsd_id': 'E2-TERMS_SET',
    },
    'E2-PARTICIPATION': {
        'name': 'E.2 Survey of Terms of Business Lending - Loans made under participation',
        'doc_href': 'http://www.federalreserve.gov/releases/E2/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=E2&filetype=zip',
        'dsd_id': 'E2-PARTICIPATION',
    },
    'E2-SBA': {
        'name': 'E.2 Survey of Terms of Business Lending - Backed by SBA',
        'doc_href': 'http://www.federalreserve.gov/releases/E2/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=E2&filetype=zip',
        'dsd_id': 'E2-SBA',
    },
    'E2-SUMMARY-STATS': {
        'name': 'E.2 Survey of Terms of Business Lending - Summary statistics',
        'doc_href': 'http://www.federalreserve.gov/releases/E2/current/default.htm',
        'url': 'http://www.federalreserve.gov/datadownload/Output.aspx?rel=E2&filetype=zip',
        'dsd_id': 'E2-SUMMARY_STATS',
    },
}


CATEGORIES = [
    {
        "provider_name": "FED",
        "category_code": "PEI",
        "name": "Principal Economic Indicators",
        "position": 1,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [],
        "metadata": {}
    },
        {
            "provider_name": "FED",
            "category_code": "PEI-CC",
            "name": "Consumer Credit (G.19)",
            "position": 1,
            "parent": "PEI",
            "all_parents": ["PEI"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "G19-TERMS",
                    "name": DATASETS["G19-TERMS"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G19-TERMS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G19-CCOUT",
                    "name": DATASETS["G19-CCOUT"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G19-CCOUT"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "PEI-IPCU",
            "name": "Industrial Production and Capacity Utilization (G.17)",
            "position": 2,
            "parent": "PEI",
            "all_parents": ["PEI"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "G17-IP-MAJOR-INDUSTRY-GROUPS",
                    "name": DATASETS["G17-IP-MAJOR-INDUSTRY-GROUPS"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-MAJOR-INDUSTRY-GROUPS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-DURABLE-GOODS-DETAIL",
                    "name": DATASETS["G17-IP-DURABLE-GOODS-DETAIL"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-DURABLE-GOODS-DETAIL"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-NONDURABLE-GOODS-DETAIL",
                    "name": DATASETS["G17-IP-NONDURABLE-GOODS-DETAIL"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-NONDURABLE-GOODS-DETAIL"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-MINING-AND-UTILITY-DETAIL",
                    "name": DATASETS["G17-IP-MINING-AND-UTILITY-DETAIL"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-MINING-AND-UTILITY-DETAIL"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-MARKET-GROUPS",
                    "name": DATASETS["G17-IP-MARKET-GROUPS"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-MARKET-GROUPS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-SPECIAL-AGGREGATES",
                    "name": DATASETS["G17-IP-SPECIAL-AGGREGATES"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-SPECIAL-AGGREGATES"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-GROSS-VALUE-STAGE-OF-PROCESS-GROUPS",
                    "name": DATASETS["G17-IP-GROSS-VALUE-STAGE-OF-PROCESS-GROUPS"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-GROSS-VALUE-STAGE-OF-PROCESS-GROUPS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-MVA",
                    "name": DATASETS["G17-MVA"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-MVA"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-DIFF",
                    "name": DATASETS["G17-DIFF"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-DIFF"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-CAPUTL",
                    "name": DATASETS["G17-CAPUTL"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-CAPUTL"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-CAP",
                    "name": DATASETS["G17-CAP"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-CAP"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-GVIP",
                    "name": DATASETS["G17-GVIP"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-GVIP"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-RIW",
                    "name": DATASETS["G17-RIW"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-RIW"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-KW",
                    "name": DATASETS["G17-KW"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-KW"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
    {
        "provider_name": "FED",
        "category_code": "BAL",
        "name": "Bank Assets & Liabilities",
        "position": 2,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [],
        "metadata": {}
    },
        {
            "provider_name": "FED",
            "category_code": "BAL-CDR",
            "name": "Charge-off and Delinquency Rates",
            "position": 1,
            "parent": "BAL",
            "all_parents": ["BAL"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "CHGDEL",
                    "name": DATASETS["CHGDEL"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["CHGDEL"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "BAL-ARDIMB",
            "name": "Aggregate Reserves of Depository Institution and the Monetary Base (H.3)",
            "position": 2,
            "parent": "BAL",
            "all_parents": ["BAL"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "H3",
                    "name": DATASETS["H3"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["H3"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "H3-DISCONTINUED",
                    "name": DATASETS["H3-DISCONTINUED"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["H3-DISCONTINUED"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "BAL-ALCB",
            "name": "Assets and Liabilities of Commercial Banks in the U.S. (H.8)",
            "position": 3,
            "parent": "BAL",
            "all_parents": ["BAL"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "H8",
                    "name": DATASETS["H8"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["H8"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "BAL-SLOOSBLP",
            "name": "Senior Loan Officer Opinion Survey on Bank Lending Practices",
            "position": 4,
            "parent": "BAL",
            "all_parents": ["BAL"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "SLOOS",
                    "name": DATASETS["SLOOS"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["SLOOS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "SLOOS-REASONS",
                    "name": DATASETS["SLOOS-REASONS"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["SLOOS-REASONS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "SLOOS-DISCONTINUED",
                    "name": DATASETS["SLOOS-DISCONTINUED"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["SLOOS-DISCONTINUED"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "BAL-STBL",
            "name": "Survey of Terms of Business Lending (E.2)",
            "position": 5,
            "parent": "BAL",
            "all_parents": ["BAL"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "E2-MATURITY-RISK",
                    "name": DATASETS["E2-MATURITY-RISK"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["E2-MATURITY-RISK"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "E2-SIZE",
                    "name": DATASETS["E2-SIZE"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["E2-SIZE"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "E2-BASERATE",
                    "name": DATASETS["E2-BASERATE"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["E2-BASERATE"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "E2-TERMS-SET",
                    "name": DATASETS["E2-TERMS-SET"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["E2-TERMS-SET"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "E2-PARTICIPATION",
                    "name": DATASETS["E2-PARTICIPATION"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["E2-PARTICIPATION"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "E2-SBA",
                    "name": DATASETS["E2-SBA"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["E2-SBA"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "E2-SUMMARY-STATS",
                    "name": DATASETS["E2-SUMMARY-STATS"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["E2-SUMMARY-STATS"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
    {
        "provider_name": "FED",
        "category_code": "BF",
        "name": "Business Finance",
        "position": 3,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [],
        "metadata": {}
    },
        {
            "provider_name": "FED",
            "category_code": "BF-CP",
            "name": "Commercial paper",
            "position": 1,
            "parent": "BF",
            "all_parents": ["BF"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "CP-RATES",
                    "name": DATASETS["CP-RATES"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["CP-RATES"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "CP-VOL",
                    "name": DATASETS["CP-VOL"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["CP-VOL"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "CP-OUTST",
                    "name": DATASETS["CP-OUTST"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["CP-OUTST"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "CP-OUTST-OLD",
                    "name": DATASETS["CP-OUTST-OLD"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["CP-OUTST-OLD"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "CP-OUTST-YREND",
                    "name": DATASETS["CP-OUTST-YREND"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["CP-OUTST-YREND"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "CP-RATES-OLD",
                    "name": DATASETS["CP-RATES-OLD"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["CP-RATES-OLD"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "BF-FC",
            "name": "Finance Companies (G.20)",
            "position": 2,
            "parent": "BF",
            "all_parents": ["BF"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "G20-OWNED",
                    "name": DATASETS["G20-OWNED"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["G20-OWNED"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G20-TERMS",
                    "name": DATASETS["G20-TERMS"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["G20-TERMS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G20-HIST",
                    "name": DATASETS["G20-HIST"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["G20-HIST"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
    {
        "provider_name": "FED",
        "category_code": "ERID",
        "name": "Exchange Rates and International Data",
        "position": 4,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [],
        "metadata": {}
    },
        {
            "provider_name": "FED",
            "category_code": "ERID-FER",
            "name": "Foreign Exchange Rates (G.5 / H.10)",
            "position": 1,
            "parent": "ERID",
            "all_parents": ["ERID"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "H10",
                    "name": DATASETS["H10"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["H10"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
    {
        "provider_name": "FED",
        "category_code": "FA",
        "name": "Financial Accounts",
        "position": 5,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [],
        "metadata": {}
    },
        {
            "provider_name": "FED",
            "category_code": "FA-FAUS",
            "name": "Financial Accounts of the United States (Z.1)",
            "position": 1,
            "parent": "FA",
            "all_parents": ["FA"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "Z1",
                    "name": DATASETS["Z1"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["Z1"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
    {
        "provider_name": "FED",
        "category_code": "HF",
        "name": "Household Finance",
        "position": 6,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [],
        "metadata": {}
    },
        {
            "provider_name": "FED",
            "category_code": "HF-CC",
            "name": "Consumer Credit (G.19)",
            "position": 1,
            "parent": "HF",
            "all_parents": ["HF"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "G19-TERMS",
                    "name": DATASETS["G19-TERMS"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["G19-TERMS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G19-CCOUT",
                    "name": DATASETS["G19-CCOUT"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["G19-CCOUT"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "HF-FC",
            "name": "Finance Companies (G.20)",
            "position": 2,
            "parent": "HF",
            "all_parents": ["HF"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "G20-TERMS",
                    "name": DATASETS["G20-TERMS"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["G20-TERMS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G20-OWNED",
                    "name": DATASETS["G20-OWNED"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["G20-OWNED"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G20-HIST",
                    "name": DATASETS["G20-HIST"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["G20-HIST"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "HF-HDSFOR",
            "name": "Household Debt Service and Financial Obligations Ratios (FOR)",
            "position": 3,
            "parent": "HF",
            "all_parents": ["HF"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "FOR",
                    "name": DATASETS["FOR"]["name"], 
                    "last_update": None,      
                    "metadata": {
                        "doc_href": DATASETS["FOR"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
    {
        "provider_name": "FED",
        "category_code": "IA",
        "name": "Industrial Activity",
        "position": 7,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [],
        "metadata": {}
    },
        {
            "provider_name": "FED",
            "category_code": "IA-IPCU",
            "name": "Industrial Production and Capacity Utilization (G.17)",
            "position": 1,
            "parent": "IA",
            "all_parents": ["IA"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "G17-IP-MAJOR-INDUSTRY-GROUPS",
                    "name": DATASETS["G17-IP-MAJOR-INDUSTRY-GROUPS"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-MAJOR-INDUSTRY-GROUPS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-DURABLE-GOODS-DETAIL",
                    "name": DATASETS["G17-IP-DURABLE-GOODS-DETAIL"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-DURABLE-GOODS-DETAIL"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-NONDURABLE-GOODS-DETAIL",
                    "name": DATASETS["G17-IP-NONDURABLE-GOODS-DETAIL"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-NONDURABLE-GOODS-DETAIL"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-MINING-AND-UTILITY-DETAIL",
                    "name": DATASETS["G17-IP-MINING-AND-UTILITY-DETAIL"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-MINING-AND-UTILITY-DETAIL"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-MARKET-GROUPS",
                    "name": DATASETS["G17-IP-MARKET-GROUPS"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-MARKET-GROUPS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-SPECIAL-AGGREGATES",
                    "name": DATASETS["G17-IP-SPECIAL-AGGREGATES"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-SPECIAL-AGGREGATES"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-IP-GROSS-VALUE-STAGE-OF-PROCESS-GROUPS",
                    "name": DATASETS["G17-IP-GROSS-VALUE-STAGE-OF-PROCESS-GROUPS"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-IP-GROSS-VALUE-STAGE-OF-PROCESS-GROUPS"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-MVA",
                    "name": DATASETS["G17-MVA"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-MVA"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-DIFF",
                    "name": DATASETS["G17-DIFF"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-DIFF"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-CAPUTL",
                    "name": DATASETS["G17-CAPUTL"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-CAPUTL"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-CAP",
                    "name": DATASETS["G17-CAP"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-CAP"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-GVIP",
                    "name": DATASETS["G17-GVIP"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-GVIP"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-RIW",
                    "name": DATASETS["G17-RIW"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-RIW"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "G17-KW",
                    "name": DATASETS["G17-KW"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["G17-KW"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
    {
        "provider_name": "FED",
        "category_code": "IR",
        "name": "Interest Rates",
        "position": 8,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [],
        "metadata": {}
    },
        {
            "provider_name": "FED",
            "category_code": "IR-SIR",
            "name": "Selected Interest Rates (H.15)",
            "position": 1,
            "parent": "IR",
            "all_parents": ["IR"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "H15",
                    "name": DATASETS["H15"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["H15"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "H15-DISCONTINUED",
                    "name": DATASETS["H15-DISCONTINUED"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["H15-DISCONTINUED"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "IR-PR",
            "name": "Policy Rates",
            "position": 2,
            "parent": "IR",
            "all_parents": ["IR"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "PRATES",
                    "name": DATASETS["PRATES"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["PRATES"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
    {
        "provider_name": "FED",
        "category_code": "MSRB",
        "name": "Money Stock and Reserve Balances",
        "position": 9,
        "parent": None,
        "all_parents": [],
        "doc_href": None,
        "datasets": [],
        "metadata": {}
    },
        {
            "provider_name": "FED",
            "category_code": "MSRB-ARDIMB",
            "name": "Aggregate Reserves of Depository Institution and the Monetary Base (H.3)",
            "position": 1,
            "parent": "MSRB",
            "all_parents": ["MSRB"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "H3",
                    "name": DATASETS["H3"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["H3"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "H3-DISCONTINUED",
                    "name": DATASETS["H3-DISCONTINUED"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["H3-DISCONTINUED"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "MSRB-FARB",
            "name": "Factors Affecting Reserve Balances (H.4.1)",
            "position": 2,
            "parent": "MSRB",
            "all_parents": ["MSRB"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "H41",
                    "name": DATASETS["H41"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["H41"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },
        {
            "provider_name": "FED",
            "category_code": "MSRB-MSM",
            "name": "Money Stock Measures (H.6)",
            "position": 3,
            "parent": "MSRB",
            "all_parents": ["MSRB"],
            "doc_href": None,
            "datasets": [
                {
                    "dataset_code": "H6-M1",
                    "name": DATASETS["H6-M1"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["H6-M1"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "H6-M2",
                    "name": DATASETS["H6-M2"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["H6-M2"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "H6-MEMO",
                    "name": DATASETS["H6-MEMO"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["H6-MEMO"]["doc_href"]
                    }
                },
                {
                    "dataset_code": "H6-M3-DISCONTINUED",
                    "name": DATASETS["H6-M3-DISCONTINUED"]["name"], 
                    "last_update": None,          
                    "metadata": {
                        "doc_href": DATASETS["H6-M3-DISCONTINUED"]["doc_href"]
                    }
                },
            ],
            "metadata": {}
        },             
]

def extract_zip_file(zipfilepath):
    zfile = zipfile.ZipFile(zipfilepath)
    filepaths = {}
    for filename in zfile.namelist():
        if filename.endswith("struct.xml"):
            key = "struct.xml"
        elif filename.endswith("data.xml"):
            key = "data.xml"
        else:
            key = filename

        filepath = zfile.extract(filename, os.path.dirname(zipfilepath))
        filepaths[key] = os.path.abspath(filepath)

    return filepaths

class FED(Fetcher):
    
    def __init__(self, **kwargs):        
        super().__init__(provider_name='FED', version=VERSION, **kwargs)
        
        self.provider = Providers(name=self.provider_name,
                                  long_name='Federal Reserve',
                                  version=VERSION,
                                  region='US',
                                  website='http://www.federalreserve.gov',
                                  terms_of_use='http://www.federalreserve.gov/accessibility.htm',
                                  fetcher=self)

    def build_data_tree(self):
        
        return CATEGORIES
        
    def upsert_dataset(self, dataset_code):
        
        dataset = Datasets(provider_name=self.provider_name, 
                           dataset_code=dataset_code,
                           name=DATASETS[dataset_code]['name'],
                           doc_href=DATASETS[dataset_code]['doc_href'],                           
                           fetcher=self)
        dataset.last_update = clean_datetime()
        
        dataset.series.data_iterator = FED_Data(dataset, 
                                                url=DATASETS[dataset_code]['url'])
        
        return dataset.update_database()

class FED_Data(SeriesIterator):
    
    def __init__(self, dataset, url=None):
        super().__init__(dataset)
        
        self.url = url
        self.store_path = self.get_store_path()
        self.xml_dsd = XMLStructure(provider_name=self.provider_name) 

        self.dsd_id = self.dataset_code
        if "dsd_id" in DATASETS[self.dataset_code]:
            self.dsd_id = DATASETS[self.dataset_code]["dsd_id"]
        
        self._load()
        
    def _load(self):

        download = Downloader(url=self.url, 
                              store_filepath=self.store_path,
                              filename="data-%s.zip" % self.dataset_code,
                              use_existing_file=self.fetcher.use_existing_file)
        zip_filepath = download.get_filepath()
        self.fetcher.for_delete.append(zip_filepath)
        
        filepaths = (extract_zip_file(zip_filepath))
        dsd_fp = filepaths['struct.xml']
        data_fp = filepaths['data.xml']
        
        for filepath in filepaths.values():
            self.fetcher.for_delete.append(filepath)
        
        self.xml_dsd.process(dsd_fp)
        self._set_dataset()

        self.xml_data = XMLData(provider_name=self.provider_name,
                                dataset_code=self.dataset_code,
                                xml_dsd=self.xml_dsd,
                                dsd_id=self.dsd_id,          
                                frequencies_supported=FREQUENCIES_SUPPORTED)
        
        self.rows = self.xml_data.process(data_fp)

    def _set_dataset(self):
        
        dataset = dataset_converter(self.xml_dsd, self.dataset_code, self.dsd_id)

        self.dataset.dimension_keys = dataset["dimension_keys"] 
        self.dataset.attribute_keys = dataset["attribute_keys"]
        
        for key in self.dataset.dimension_keys:
            if key in dataset["concepts"]:
                self.dataset.concepts[key] = dataset["concepts"][key]
            if key in dataset["codelists"]:
                self.dataset.codelists[key] = dataset["codelists"][key]
        
        for key in self.dataset.attribute_keys:
            if key in dataset["concepts"]:
                self.dataset.concepts[key] = dataset["concepts"][key]
            if key in dataset["codelists"]:
                self.dataset.codelists[key] = dataset["codelists"][key]

    def clean_field(self, bson):
        bson["attributes"].pop("SERIES_NAME", None)
        bson = super().clean_field(bson)
        return bson
        
    def build_series(self, bson):
        self.dataset.add_frequency(bson["frequency"])
        bson["last_update"] = self.dataset.last_update
        return bson

