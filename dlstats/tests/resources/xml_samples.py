
import os

BASE_RESOURCES_DIR = os.path.abspath(os.path.dirname(__file__))
RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "xmlutils"))

def filepath(provider, filename):
    return os.path.abspath(os.path.join(RESOURCES_DIR, provider, filename))    

DSD_FED_TERMS = {
    "provider": "FED",
    "filepaths": {
        "datastructure": filepath("fed", "fed-structure-1.0.xml"),
    },
    "dataset_code": "G19-TERMS",
    "dataset_name": "G.19 - Consumer Credit - Terms of Credit Outstanding",
    "dsd_id": "G19-TERMS",
    "dsd_ids": ['G19-CCOUT', 'G19-TERMS'],
    "dataflow_keys": ['G19-CCOUT', 'G19-TERMS'],
    "is_completed": True,
    "categories_key": "PEI",
    "categories_parents": None,
    "categories_root": ['BAL', 'BF', 'ERID', 'FA', 'HF', 'IA', 'IR', 'MSRB', 'PEI'],
    "concept_keys": ['CREDTYP', 'CURRENCY', 'DATAREP', 'FREQ', 'HOLDER', 'ISSUE', 'OBS_STATUS', 'OBS_VALUE', 'SA', 'SERIES_NAME', 'TERMS', 'TIME_PERIOD', 'UNIT', 'UNIT_MULT'],
    "codelist_keys": ['CL_CCOUT_CREDTYP', 'CL_CCOUT_DATAREP', 'CL_CCOUT_HOLDER', 'CL_CURRENCY', 'CL_FREQ', 'CL_ISSUE', 'CL_OBS_STATUS', 'CL_SA', 'CL_TERMS', 'CL_UNIT', 'CL_UNIT_MULT'],
    "codelist_count": {
        "CL_CCOUT_CREDTYP": 5,
        "CL_CCOUT_DATAREP": 3,
        "CL_CCOUT_HOLDER": 10,
        "CL_CURRENCY": 200,
        "CL_FREQ": 50,
        "CL_ISSUE": 2,
        "CL_OBS_STATUS": 4,
        "CL_SA": 2,
        "CL_TERMS": 8,
        "CL_UNIT": 413,
        "CL_UNIT_MULT": 9,
    },
    "dimension_keys": ['ISSUE', 'TERMS', 'FREQ'],
    "dimension_count": {
        "ISSUE": 2,
        "TERMS": 8,
        "FREQ": 50,
    },
    "attribute_keys": ['OBS_STATUS', 'CURRENCY', 'UNIT', 'UNIT_MULT', 'SERIES_NAME'],
    "attribute_count": {
        "OBS_STATUS": 4,
        "CURRENCY": 200,
        "UNIT": 413,
        "UNIT_MULT": 9,
        "SERIES_NAME": 0,
    }
}

DSD_EUROSTAT = {
    "provider": "EUROSTAT",
    "filepaths": {
        "datastructure": filepath("eurostat", "eurostat-datastructure-2.0.xml"),
    },
    "dataset_code": "nama_10_fcs",
    "dataset_name": "nama_10_fcs",
    "dsd_id": "nama_10_fcs",
    "dsd_ids": ["nama_10_fcs"],
    "dataflow_keys": ['nama_10_fcs'],
    "is_completed": True,
    "categories_key": "nama_10_ma",
    "categories_parents": ["data", "economy", "na10", "nama_10"],
    "categories_root": ["data"],    
    "concept_keys": ['FREQ', 'OBS_STATUS', 'OBS_VALUE', 'TIME_FORMAT', 'TIME_PERIOD', 'geo', 'na_item', 'unit'],
    "codelist_keys": ['CL_FREQ', 'CL_GEO', 'CL_NA_ITEM', 'CL_OBS_STATUS', 'CL_TIME_FORMAT', 'CL_UNIT'],
    "codelist_count": {
        "CL_FREQ": 9,
        "CL_GEO": 33,
        "CL_NA_ITEM": 10,
        "CL_OBS_STATUS": 12,
        "CL_TIME_FORMAT": 7,
        "CL_UNIT": 12,        
    },
    "dimension_keys": ['FREQ', 'unit', 'na_item', 'geo'],
    "dimension_count": {
        "FREQ": 9,
        "unit": 12,
        "na_item": 10,
        "geo": 33,
    },
    "attribute_keys": ["TIME_FORMAT", "OBS_STATUS"],
    "attribute_count": {
        "TIME_FORMAT": 7,
        "OBS_STATUS": 12,
    }, 
}


DSD_DESTATIS =  {
    "provider": "DESTATIS",
    "filepaths": {},
    "dataset_code": "DCS",
    "dataset_name": "",
    "dsd_id": "DCS",
    "dsd_ids": ["DCS"],
    "dataflow_keys": ["DCS"],
    "is_completed": False,
    "concept_keys": [],
    "codelist_keys": [],
    "codelist_count": None,
    "dimension_keys": [],
    "dimension_count": None,
    "attribute_keys": [],
    "attribute_count": None,
}                   

DSD_OECD_MEI = {
    "provider": "OECD",
    "filepaths": {
        "datastructure": filepath("oecd", "oecd-mei-datastructure-2.0.xml"),
    },
    "dataset_code": "MEI",
    "dataset_name": "Main Economic Indicators Publication",
    "dsd_id": "MEI",
    "dsd_ids": ["MEI"],
    "dataflow_keys": ['MEI'],
    "is_completed": True,
    "categories_key": "MEI",
    "categories_parents": ["MEI"],
    "categories_root": ["MEI"],    
    "concept_keys": ['FREQUENCY', 'LOCATION', 'MEASURE', 'OBS_STATUS', 'OBS_VALUE', 'POWERCODE', 'REFERENCEPERIOD', 'SUBJECT', 'TIME', 'TIME_FORMAT', 'UNIT'],
    "codelist_keys": ['CL_MEI_FREQUENCY', 'CL_MEI_LOCATION', 'CL_MEI_MEASURE', 'CL_MEI_OBS_STATUS', 'CL_MEI_POWERCODE', 'CL_MEI_REFERENCEPERIOD', 'CL_MEI_SUBJECT', 'CL_MEI_TIME', 'CL_MEI_TIME_FORMAT','CL_MEI_UNIT'],
    "codelist_count": {
        "CL_MEI_FREQUENCY": 3,
        "CL_MEI_LOCATION": 64,
        "CL_MEI_MEASURE": 24,
        "CL_MEI_OBS_STATUS": 14,
        "CL_MEI_POWERCODE": 32,
        "CL_MEI_REFERENCEPERIOD": 68,
        "CL_MEI_SUBJECT": 1099,
        "CL_MEI_TIME": 1172,
        "CL_MEI_TIME_FORMAT": 5,
        "CL_MEI_UNIT": 296,    
    },
    "dimension_keys": ['LOCATION', 'SUBJECT', 'MEASURE', 'FREQUENCY'],
    "dimension_count": {
        "LOCATION": 64,
        "SUBJECT": 1099,
        "MEASURE": 24,
        "FREQUENCY": 3,        
    },
    "attribute_keys": ['OBS_STATUS', 'TIME_FORMAT', 'UNIT', 'REFERENCEPERIOD', 'POWERCODE'],
    "attribute_count": {
        "OBS_STATUS": 14,
        "TIME_FORMAT": 5,
        "UNIT": 296,
        "REFERENCEPERIOD": 68,
        "POWERCODE": 32,
    }, 
}

DSD_OECD_EO = {
    "provider": "OECD",
    "filepaths": {
        "datastructure": filepath("oecd", "oecd-eo-datastructure-2.0.xml"),
    },
    "dataset_code": "EO",
    "dataset_name": "Economic Outlook No 98 - November 2015",
    "dsd_id": "EO",
    "dsd_ids": ["EO"],
    "dataflow_keys": ['EO'],
    "is_completed": True,
    "categories_key": "EO",
    "categories_parents": ["EO"],
    "categories_root": ["EO"],
    "concept_keys": ['FREQUENCY', 'LOCATION', 'OBS_STATUS', 'OBS_VALUE', 'POWERCODE', 'REFERENCEPERIOD', 'TIME', 'TIME_FORMAT', 'UNIT', 'VARIABLE'],
    "codelist_keys": ['CL_EO_FREQUENCY', 'CL_EO_LOCATION', 'CL_EO_OBS_STATUS', 'CL_EO_POWERCODE', 'CL_EO_REFERENCEPERIOD', 'CL_EO_TIME', 'CL_EO_TIME_FORMAT', 'CL_EO_UNIT', 'CL_EO_VARIABLE'],
    "codelist_count": {
        "CL_EO_FREQUENCY": 2,
        "CL_EO_LOCATION": 59,
        "CL_EO_OBS_STATUS": 14,
        "CL_EO_POWERCODE": 32,
        "CL_EO_REFERENCEPERIOD": 68,
        "CL_EO_TIME": 406,
        "CL_EO_TIME_FORMAT": 5,
        "CL_EO_UNIT": 296,
        "CL_EO_VARIABLE": 297,
    },
    "dimension_keys": ['LOCATION', 'VARIABLE', 'FREQUENCY'],
    "dimension_count": {
        "LOCATION": 59,
        "VARIABLE": 297,
        "FREQUENCY": 2,
    },
    "attribute_keys": ['OBS_STATUS', 'TIME_FORMAT', 'UNIT', 'REFERENCEPERIOD', 'POWERCODE'],
    "attribute_count": {
        "OBS_STATUS": 14,
        "TIME_FORMAT": 5,
        "UNIT": 296,
        "REFERENCEPERIOD": 68,
        "POWERCODE": 32,
    }, 
}

DSD_ECB = {
    "provider": "ECB",
    "filepaths": {
        "dataflow": filepath("ecb", "ecb-dataflow-2.1.xml"),
        "categorisation": filepath("ecb", "ecb-categorisation-2.1.xml"),
        "categoryscheme": filepath("ecb", "ecb-categoryscheme-2.1.xml"),
        "conceptscheme": filepath("ecb", "ecb-conceptscheme-2.1.xml"),
        "datastructure": filepath("ecb", "ecb-datastructure-2.1.xml"),
    },
    "dataset_code": "EXR",
    "dataset_name": "Exchange Rates",
    "dsd_id": "ECB_EXR1",    
    "dsd_ids": ["ECB_EXR1"],    
    "dataflow_keys": ['EXR'],
    "is_completed": True,
    "categorisations_key": "53A341E8-D48B-767E-D5FF-E2E3E0E2BB19",
    "categories_key": "07",
    "categories_parents": None,
    "categories_root": ['01', '02', '03', 
                        #'04', EUROSTAT / ESTAT 
                        '05', '06', '07', '08', 
                        '09', '10', '11'],
    "concept_keys": ['ACCOUNT_ENTRY', 'ADJUSTMENT', 'ADJUST_DETAIL', 
                     'ADJU_DETAIL', 'AGG_EQUN', 'AME_AGG_METHOD', 'AME_ITEM', 
                     'AME_REFERENCE', 'AME_REF_AREA', 'AME_TRANSFORMATION', 
                     'AME_UNIT', 'AMOUNT_CAT', 'AREA_DEFINITION', 
                     'AVAILABILITY', 'BANKING_IND', 'BANKING_ITEM', 
                     'BANKING_REF', 'BANKING_SUFFIX', 'BANK_SELECTION', 
                     'BDS_ITEM', 'BIS_BLOCK', 'BIS_SUFFIX', 'BIS_TOPIC', 
                     'BKN_DENOM', 'BKN_ITEM', 'BKN_SERIES', 'BKN_TYPE', 
                     'BLS_AGG_METHOD', 'BLS_COUNT', 'BLS_COUNT_DETAIL', 
                     'BLS_ITEM', 'BOP_BASIS', 'BOP_ITEM', 'BREAKS', 
                     'BS_COUNT_SECTOR', 'BS_ITEM', 'BS_NFC_ACTIVITY', 
                     'BS_REP_SECTOR', 'BS_SUFFIX', 'CB_EXP_TYPE', 'CB_ITEM', 
                     'CB_PORTFOLIO', 'CB_REP_FRAMEWRK', 'CB_REP_SECTOR', 
                     'CB_SECTOR_SIZE', 'CB_VAL_METHOD', 'CCP_SYSTEM', 
                     'CIBL_CATEGORY', 'CIBL_TABLE', 'CIBL_TYPE', 'COLLATERAL', 
                     'COLLECTION', 'COLLECTION_DETAIL', 'COMMENT_OBS', 
                     'COMMENT_TS', 'COMPILATION', 'COMPILING_ORG', 
                     'COMP_APPROACH', 'COMP_METHOD', 'CONF_STATUS', 
                     'COUNTERPART_AREA', 'COUNTERPART_SECTOR', 'COUNT_AREA', 
                     'COUNT_AREA_IFS', 'COUNT_SECTOR', 'COVERAGE', 'CPP_METHOD', 
                     'CREDIT_RATING', 'CURRENCY', 'CURRENCY_DENOM', 
                     'CURRENCY_P_H', 'CURRENCY_S', 'CURRENCY_TRANS', 
                     'CURR_BRKDWN', 'DATA_COMP', 'DATA_TYPE', 'DATA_TYPE_BKN', 
                     'DATA_TYPE_BOP', 'DATA_TYPE_DBI', 'DATA_TYPE_FM', 
                     'DATA_TYPE_FXS', 'DATA_TYPE_IFI', 'DATA_TYPE_LIG', 
                     'DATA_TYPE_MIR', 'DATA_TYPE_MM', 'DATA_TYPE_MUFA', 
                     'DATA_TYPE_PDF', 'DATA_TYPE_PSS', 'DATA_TYPE_SEC', 
                     'DD_ECON_CONCEPT', 'DD_SUFFIX', 'DD_TRANSF', 'DEBT_TYPE', 
                     'DECIMALS', 'DISS_ORG', 'DOM_SER_IDS', 'EAPLUS_FLAG', 
                     'EFFECT_DOMAIN', 'EMBARGO', 'EMBARGO_DETAIL', 'EONIA_BANK', 
                     'EONIA_ITEM', 'ESA95TP_ASSET', 'ESA95TP_BRKDWN', 
                     'ESA95TP_COM','ESA95TP_CONS', 'ESA95TP_CPAREA', 
                     'ESA95TP_CPSECTOR', 'ESA95TP_DC_AL', 'ESA95TP_DENOM', 
                     'ESA95TP_PRICE', 'ESA95TP_REGION', 'ESA95TP_SECTOR', 
                     'ESA95TP_SUFFIX', 'ESA95TP_TRANS', 'ESA95_ACCOUNT', 
                     'ESA95_BREAKDOWN', 'ESA95_SUFFIX', 'ESA95_UNIT', 
                     'ESCB_FLAG', 'EXR_SUFFIX', 'EXR_TYPE', 'EXT_REF_AREA', 
                     'EXT_TITLE', 'EXT_UNIT', 'EXT_UNIT_MULT', 'FCT_BREAKDOWN', 
                     'FCT_HORIZON', 'FCT_SOURCE', 'FCT_TOPIC', 'FIRM_AGE', 
                     'FIRM_OWNERSHIP', 'FIRM_SECTOR', 'FIRM_SIZE', 
                     'FIRM_TURNOVER', 'FLOATING_RATE_BASE', 'FLOW_STOCK_ENTRY', 
                     'FM_CONTRACT_TIME', 'FM_COUPON_RATE', 'FM_IDENTIFIER', 
                     'FM_LOT_SIZE', 'FM_MATURITY', 'FM_OUTS_AMOUNT', 
                     'FM_PUT_CALL', 'FM_STRIKE_PRICE', 'FREQ', 'FUNCTIONAL_CAT', 
                     'FVC_ITEM', 'FVC_ORI_SECTOR', 'FVC_REP_SECTOR', 
                     'FXS_OP_TYPE', 'GOVNT_COUNT_SECTOR', 'GOVNT_ITEM_ESA', 
                     'GOVNT_REF_SECTOR', 'GOVNT_ST_SUFFIX', 'GOVNT_VALUATION', 
                     'GROUP_TYPE', 'HOLDER_AREA', 'HOLDER_SECTOR', 'ICO_PAY', 
                     'ICO_UNIT', 'ICPF_ITEM', 'ICP_ITEM', 'ICP_SUFFIX', 
                     'IFS_CODE', 'INSTRUMENT_FM', 'INSTR_ASSET', 'INS_BUS_TYPE', 
                     'INT_ACC_ITEM', 'IN_OUT_DATA_IFI', 'IR_BUS_COV', 
                     'IR_FV_TYPE', 'IR_TYPE', 'ISSUER_AREA', 'ISSUER_IN', 
                     'ISSUER_RBG', 'ISSUER_SECTOR', 'IS_IN_EADB', 'IVF_ITEM', 
                     'IVF_REP_SECTOR', 'LIG_ITEM', 'MARKET_ROLE', 'MARKET_TRANS', 
                     'MATURITY', 'MATURITY_CAT', 'MATURITY_NOT_IRATE', 
                     'MATURITY_ORIG', 'MATURITY_RES', 'MATURITY_TYPE', 
                     'MA_FLAG', 'MEASURE_MEI', 'MEASURE_QNA', 'MEASURE_SNA', 
                     'METHOD_AGENCY', 'METHOD_DETAIL', 'METHOD_PUBL', 'METHOD_REF', 'MFI_LIST', 'MFI_LIST_IND', 'MFI_STATUS', 'MM_BANK', 'MM_SEGMENT', 'MUFA_CRED_AREA', 'MUFA_CRED_SECTOR', 'MUFA_DEBT_AREA', 'MUFA_DEBT_SECTOR', 'MUFA_ITEM', 'MUFA_SOURCE', 'MUFA_VALUATION', 'NAT_TITLE', 'NA_PRICE', 'NOM_CURR', 'NON_RESID_ECON_ACT', 'OBS_COM', 'OBS_CONF', 'OBS_PRE_BREAK', 'OBS_STATUS', 'OBS_VALUE', 'OECD_A16_CODE', 'OEO_CODE', 'OFI_ITEM', 'OFI_REP_SECTOR', 'OIL_PRODUCT', 'OLV_INDICATOR', 'OPTION_TYPE_PDF', 'ORGANISATION', 'OTHER_METH_EXPL', 'PD_ITEM', 'PD_ORIGIN', 'PD_SEAS_EX', 'PORTFOLIO_CAT', 'PRE_BREAK_VALUE','PRICE_BASE', 'PRICE_TYPE', 'PROPERTY_CPP', 'PROPERTY_IND', 'PROPERTY_SUFFIX', 'PROVIDER_FM', 'PROVIDER_FM_ID', 'PSS_INFO_TYPE', 'PSS_INSTRUMENT', 'PSS_SYSTEM', 'PUBLICATION', 'PUBL_ECB', 'PUBL_MU', 'PUBL_PUBLIC', 'RBG_ID', 'REF_AREA', 'REF_AREA_MEI', 'REF_PERIOD_DETAIL', 'REF_SECTOR', 'REPORTING_SECTOR', 'REPO_CPARTY', 'REP_CTY', 'REP_DELAY', 'RESID_ECON_ACT', 'RIR_SUFFIX', 'RPP_DWELLING', 'RPP_GEO_COV', 'RPP_SOURCE', 'RPP_SUFFIX', 'RT_DENOM', 'RT_ECON_CONCEPT', 'R_L_SECTOR', 'SAFE_ANSWER', 'SAFE_DENOM', 'SAFE_FILTER', 'SAFE_ITEM', 'SAFE_QUESTION', 'SECURITISATION_TYP', 'SECURITY_TYPE', 'SEC_ISSUING_SECTOR', 'SEC_ITEM', 'SEC_SUFFIX', 'SEC_VALUATION', 'SEE_SYSTEM', 'SERIES_DENOM', 'SHI_INDICATOR', 'SHI_SUFFIX', 'SOURCE_AGENCY', 'SOURCE_AGENCY_2', 'SOURCE_DATA', 'SOURCE_DETAIL', 'SOURCE_DETAIL_2', 'SOURCE_PUB', 'SOURCE_PUBL_2', 'SSI_INDICATOR', 'SSS_INFO_TYPE', 'SSS_INSTRUMENT', 'SSS_SYSTEM', 'STO', 'STOCK_FLOW', 'STS_CLASS', 'STS_CONCEPT', 'STS_INSTITUTION', 'STS_SUFFIX', 'SUBJECT_MEI', 'SUBJECT_OEO', 'SUBJECT_QNA', 'SUBJECT_SNA', 'SUFFIX_QNA', 'SURVEY_FREQ', 'TIME_FORMAT', 'TIME_HORIZON', 'TIME_PERIOD', 'TIME_PER_COLLECT', 'TITLE', 'TITLE_COMPL', 'TPH', 'TRADE_WEIGHT', 'TRD_FLOW', 'TRD_PRODUCT', 'TRD_SUFFIX', 'TRG_ACCOUNT', 'TRG_ANCILLARY', 'TRG_BAND', 'TRG_CATEGORY', 'TR_TYPE', 'UNIT', 'UNIT_DETAIL', 'UNIT_INDEX_BASE', 'UNIT_MEASURE', 'UNIT_MULT', 'UNIT_PRICE_BASE', 'U_A_SECTOR', 'VALUATION', 'VAL_COLUMN', 'VAL_ITEM', 'VAL_REPORT', 'VAL_ROW', 'VIS_CTY', 'WEO_ITEM', 'WEO_REF_AREA', 'WOB_CONCEPT', 'WOB_TAXATION'],
    "codelist_keys": ['CL_COLLECTION', 'CL_CURRENCY', 'CL_DECIMALS', 'CL_EXR_SUFFIX', 'CL_EXR_TYPE', 'CL_FREQ', 'CL_OBS_CONF', 'CL_OBS_STATUS', 'CL_ORGANISATION', 'CL_UNIT', 'CL_UNIT_MULT'],
    "codelist_count": {
        "CL_COLLECTION": 10,
        "CL_CURRENCY": 349,
        "CL_DECIMALS": 16,
        "CL_EXR_SUFFIX": 6,
        "CL_EXR_TYPE": 36,
        "CL_FREQ": 10,
        "CL_OBS_CONF": 4,
        "CL_OBS_STATUS": 17,
        "CL_ORGANISATION": 893,
        "CL_UNIT": 330,
        "CL_UNIT_MULT": 11,
    },
    "dimension_keys": ['FREQ', 'CURRENCY', 'CURRENCY_DENOM', 'EXR_TYPE', 'EXR_SUFFIX'],
    "dimension_count": {
        'CURRENCY': 349,
        'CURRENCY_DENOM': 349,
        'EXR_SUFFIX': 6,
        'EXR_TYPE': 36,
        'FREQ': 10
    },
    "attribute_keys": ['TIME_FORMAT', 'OBS_STATUS', 'OBS_CONF', 'OBS_PRE_BREAK', 'OBS_COM', 'BREAKS', 'COLLECTION', 'DOM_SER_IDS', 'PUBL_ECB', 'PUBL_MU', 'PUBL_PUBLIC', 'UNIT_INDEX_BASE', 'COMPILATION', 'COVERAGE', 'DECIMALS', 'NAT_TITLE', 'SOURCE_AGENCY', 'SOURCE_PUB', 'TITLE', 'TITLE_COMPL', 'UNIT', 'UNIT_MULT'],
    "attribute_count": {
        "TIME_FORMAT": 0,
        "OBS_STATUS": 17,
        "OBS_CONF": 4,
        "OBS_PRE_BREAK": 0,
        "OBS_COM": 0,
        "BREAKS": 0,
        "COLLECTION": 10,
        "DOM_SER_IDS": 0,
        "PUBL_ECB": 0,
        "PUBL_MU": 0,
        "PUBL_PUBLIC": 0,
        "UNIT_INDEX_BASE": 0,
        "COMPILATION": 0,
        "COVERAGE": 0,
        "DECIMALS": 16,
        "NAT_TITLE": 0,
        "SOURCE_AGENCY": 893,
        "SOURCE_PUB": 0,
        "TITLE": 0,
        "TITLE_COMPL": 0,
        "UNIT": 330,
        "UNIT_MULT": 11
    }, 
}

DSD_INSEE = {
    "provider": "INSEE",
    "filepaths": {
        "dataflow": filepath("insee", "insee-dataflow-2.1.xml"),
        "categorisation": filepath("insee", "insee-categorisation-2.1.xml"),
        "categoryscheme": filepath("insee", "insee-categoryscheme-2.1.xml"),
        "conceptscheme": filepath("insee", "insee-conceptscheme-2.1.xml"),
        "datastructure": filepath("insee", "insee-datastructure-2.1.xml"),
        "CL_UNIT": filepath("insee", "insee-codelist-cl_unit.xml"),
        "CL_UNIT_MULT": filepath("insee", "insee-codelist-cl_unit_mult.xml"),
        "CL_AREA": filepath("insee", "insee-codelist-cl_area.xml"),
        "CL_TIME_COLLECT": filepath("insee", "insee-codelist-cl_time_collect.xml"),
        "CL_OBS_STATUS": filepath("insee", "insee-codelist-cl_obs_status.xml"),
        "CL_FREQ": filepath("insee", "insee-codelist-cl_freq.xml"),
    },
    "dataset_code": "IPI-2010-A21",
    "dataset_name": "Industrial production index (base 2010) - NAF level A21",
    "dsd_id": "IPI-2010-A21",
    "dsd_ids": ["IPI-2010-A21"],
    "dataflow_keys": ['IPI-2010-A21'],
    "is_completed": True,
    "categorisations_key": "CAT_IPI-2010_IPI-2010-A21",
    "categories_key": "IPI-2010",
    "categories_parents": ['PRODUCTION-ENT', 'INDUSTRIE-CONST', 'PRODUCTION-IND'],    
    "categories_root": ['COMPTA-NAT', 'ENQ-CONJ', 'PRIX', 'PRODUCTION-ENT', 
                        'DEMO-ENT', 'POPULATION', 'MARCHE-TRAVAIL', 
                        'SALAIRES-REVENUS', 'ECHANGES-EXT', 'CONDITIONS-VIE-SOCIETE', 
                        'SERVICES-TOURISME-TRANSPORT', 'SRGDP'],    
    "concept_keys": ['ACCUEIL-PERS-AGEES', 'ACTIVITE', 'AGE', 'ANCIENNETE', 
                     'BASE_PER', 'BASIND', 'BRANCHE', 'CARAC-LOG', 'CARBURANT', 
                     'CAT-DE', 'CAT-FP', 'CAUSE-DECES', 'CHAMP-GEO', 'CHEPTEL', 'CLIENTELE', 'COMPTE', 'CORRECTION', 'COTISATION', 'DATE-DEF-ENT', 'DECIMALS', 'DEMOGRAPHIE', 'DEPARTEMENT', 'DEST-INV', 'DEVISE', 'DIPLOME', 'EFFOPE', 'EMBARGO_TIME', 'ETAB-SCOL', 'ETAT-CONSTRUCTION', 'EXPAGRI', 'FACTEUR-INV', 'FEDERATION', 'FINANCEMENT', 'FONCTION', 'FORMATION', 'FORME-EMP', 'FORME-VENTE', 'FREQ', 'FREQUENCE', 'GEOGRAPHIE', 'HALO', 'IDBANK', 'INDEX', 'INDICATEUR', 'INSTRUMENT', 'IPC-CALC01', 'LAST_UPDATE', 'LOCAL', 'LOCALISATION', 'LOGEMENT', 'MARCHANDISE', 'METIER', 'MIN-FPE', 'MONNAIE', 'NATURE', 'NATURE-FLUX', 'OBS_STATUS', 'OBS_VALUE', 'OCC-SOL', 'OPERATION', 'PERIODE', 'POPULATION', 'PRATIQUE', 'PRIX', 'PROD-VEG', 'PRODUIT', 'QUESTION', 'QUOTITE-TRAV', 'REF_AREA', 'REGION', 'REPARTITION', 'RESIDENCE', 'REVENU', 'SECT-INST', 'SEXE', 'SPECIALITE-SANTE', 'TAILLE-ENT', 'TAILLE-MENAGE', 'TIME_PERIOD', 'TIME_PER_COLLECT', 'TITLE', 'TOURISME-INDIC', 'TYPE-CESS-ENT', 'TYPE-COTIS', 'TYPE-CREAT-ENT', 'TYPE-EMP', 'TYPE-ENT', 'TYPE-ETAB', 'TYPE-EVO', 'TYPE-FAMILLE', 'TYPE-FLUX', 'TYPE-MENAGE', 'TYPE-OE', 'TYPE-PRIX', 'TYPE-RESEAU', 'TYPE-SAL', 'TYPE-SURF-ALIM', 'TYPE-TX-CHANGE', 'TYPE-VEHICULE', 'UNITE', 'UNITE-URBAINE', 'UNIT_MEASURE', 'UNIT_MULT', 'ZONE-GEO'],
    "codelist_keys": ['CL_AREA', 'CL_FREQ', 'CL_NAF2_A21', 'CL_NATURE', 'CL_OBS_STATUS', 'CL_TIME_COLLECT', 'CL_UNIT'],
    "codelist_count": {
        "CL_AREA": 11,
        "CL_FREQ": 7,
        "CL_NAF2_A21": 30,
        "CL_NATURE": 25,
        "CL_OBS_STATUS": 10,
        "CL_TIME_COLLECT": 7,
        "CL_UNIT": 125,    
    },
    "dimension_keys": ['FREQ', 'PRODUIT', 'NATURE'],
    "dimension_count": {
        'FREQ': 7, 
        'NATURE': 25,
        'PRODUIT': 30
    },
    "attribute_keys": ['IDBANK', 'TITLE', 'LAST_UPDATE', 'UNIT_MEASURE', 
                       'UNIT_MULT', 'REF_AREA', 'DECIMALS', 'BASE_PER', 
                       'TIME_PER_COLLECT', 'OBS_STATUS', 'EMBARGO_TIME'],      
    "attribute_count": {
        'IDBANK': 0, 
        'TITLE': 0, 
        'LAST_UPDATE': 0,
        'UNIT_MEASURE': 125, 
        'UNIT_MULT': 0, 
        'REF_AREA': 11, 
        'DECIMALS': 0,
        'BASE_PER': 0, 
        'TIME_PER_COLLECT': 7, 
        'OBS_STATUS': 10, 
        'EMBARGO_TIME': 0, 
    },
}

DATA_FED_TERMS = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "fed", "fed-data-1.0.xml")),
    "klass": "XMLData_1_0_FED",
    "DSD": DSD_FED_TERMS,
    "kwargs": {
        "provider_name": "FED",
        "dataset_code": "G19-TERMS",
        "dsd_filepath": DSD_FED_TERMS["filepaths"]["datastructure"],
        "dsd_id": DSD_FED_TERMS["dsd_id"]
    },
    "series_accept": 11,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 3714,
    "series_key_first": "RIFLPBCIANM48_N.M",
    "series_key_last": "DTCTLVENA_N.M",
    "series_sample": {
        "provider_name": "FED",
        "dataset_code": "G19-TERMS",
        'key': 'RIFLPBCIANM48_N.M',
        'name': "Finance rate on consumer installment loans at commercial banks, new autos 48 month loan; not seasonally adjusted",
        'frequency': 'M',
        'last_update': None,
        'first_value': {
            'value': '10.20',
            'period': '1972-02-29',
            'attributes': {
                'OBS_STATUS': 'A',
            },
        },
        'last_value': {
            'value': '4.11',
            'period': '2015-08-31',
            'attributes': {
                'OBS_STATUS': 'A',
            },
        },
        'dimensions': {
            'FREQ': '129',
            'ISSUE': 'COMBANK',
            'TERMS': 'NEWCAR',
        },
        'attributes': {
            'CURRENCY': 'USD',
            'UNIT': 'Percent',
            'SERIES_NAME': 'RIFLPBCIANM48_N.M',
            'UNIT_MULT': '1',
        },
    }
}

DATA_EUROSTAT = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "eurostat", "eurostat-data-compact-2.0.xml")),
    "klass": "XMLCompactData_2_0_EUROSTAT",
    "DSD": DSD_EUROSTAT,
    "kwargs": {
        "provider_name": "EUROSTAT",
        "dataset_code": "nama_10_fcs",
        #"field_frequency": "FREQ",
        #"dimension_keys": DSD_EUROSTAT["dimension_keys"],
        "dsd_filepath": DSD_EUROSTAT["filepaths"]["datastructure"],
        "dsd_id": DSD_EUROSTAT["dsd_id"]        
        #"frequencies_supported": [] #TODO: specific: P1Y (A), P3M (Q), P1M (M), P1D (D)
    },
    "series_accept": 3303,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 70534,
    "series_key_first": 'A.CLV05_MEUR.P311_S14.AT',
    "series_key_last": 'A.PYP_MNAC.P34.UK',
    "series_sample": {
        "provider_name": "EUROSTAT",
        "dataset_code": "nama_10_fcs",
        'key': 'A.CLV05_MEUR.P311_S14.AT',
        'name': 'Annual - Chain linked volumes (2005), million euro - Final consumption expenditure of households, durable goods - Austria',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '10452.9',
            'period': '1995',
            'attributes': {},
        },
        'last_value': {
            'value': '17517.5',
            'period': '2014',
            'attributes': {},
        },
        'dimensions': {
            'FREQ': 'A',
            'unit': 'CLV05_MEUR',
            'na_item': 'P311_S14',
            'geo': 'AT',
        },
        'attributes': {
            'TIME_FORMAT': 'P1Y',
        },
    }
}

DATA_DESTATIS = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "destatis", "destatis-data-compact-2.0.xml")),
    "klass": "XMLCompactData_2_0_DESTATIS",
    "DSD": DSD_DESTATIS,
    "kwargs": {
        "provider_name": "DESTATIS",
        "dataset_code": "DCS",
        "field_frequency": "FREQ",
        "dimension_keys": DSD_DESTATIS["dimension_keys"],
        "dsd_id": DSD_DESTATIS["dsd_id"],        
        #"dsd_filepath": DSD_DESTATIS["filepaths"]["datastructure"],
        "frequencies_supported": ["A", "D", "M", "Q", "W"]
    },
    "series_accept": 7,
    "series_reject_frequency": 0,
    "series_reject_empty": 7,
    "series_all_values": 1197,
    "series_key_first": 'M.DCS.DE.FM1_EUR.U2',
    "series_key_last": 'M.DCS.DE.FDSLF_EUR.U2',
    "series_sample": {
        "provider_name": "DESTATIS",
        "dataset_code": "DCS",
        'key': 'M.DCS.DE.FM1_EUR.U2',
        'name': 'M-DCS-DE-FM1_EUR-U2',
        'frequency': 'M',
        'last_update': None,
        'first_value': {
            'value': '593935',
            'period': '1972-02-29',
            'attributes': {
                'OBS_STATUS': 'A',
            },
        },
        'last_value': {
            'value': '1789542',
            'period': '2015-08-31',
            'attributes': {
                'OBS_STATUS': 'A',
            },
        },
        'dimensions': {
            'FREQ': 'M',
            'DATA_DOMAIN': 'DCS',
            'REF_AREA': 'DE',
            'INDICATOR': 'FM1_EUR',
            'COUNTERPART_AREA': 'U2'
        },
        'attributes': {},
    }
}

DATA_OECD_MEI = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "oecd", "oecd-mei-data-generic.xml")),
    "klass": "XMLGenericData_2_0_OECD",
    "DSD": DSD_OECD_MEI,
    "kwargs": {
        "provider_name": "OECD",
        "dataset_code": "MEI",
        "dsd_filepath": DSD_OECD_MEI["filepaths"]["datastructure"],
        "dsd_id": DSD_OECD_MEI["dsd_id"],        
    },
    "series_accept": 4,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 1569,
    "series_key_first": 'AUT.PRMNTO01.IXOBSA.A',
    "series_key_last": 'FRA.PRMNTO01.IXOBSA.M',
    "series_sample": {
        "provider_name": "OECD",
        "dataset_code": "MEI",
        'key': 'AUT.PRMNTO01.IXOBSA.A',
        'name': 'Austria - Production > Manufacturing > Total manufacturing > Total manufacturing - Index 2010=100, s.a. - Annual',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '12.8770244930154',
            'period': '1956',
            'attributes': {},
        },
        'last_value': {
            'value': '108.920383493122',
            'period': '2014',
            'attributes': {},
        },
        'dimensions': {
            'LOCATION': 'AUT',
            'SUBJECT': 'PRMNTO01',
            'MEASURE': 'IXOBSA',
            'FREQUENCY': 'A',
        },
        'attributes': {
            'POWERCODE': '0',
            'REFERENCEPERIOD': '2010_100',
            'TIME_FORMAT': 'P1Y',
            'UNIT': 'IDX'
        },
    }
}

DATA_OECD_EO = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "oecd", "oecd-eo-data-generic.xml")),
    "klass": "XMLGenericData_2_0_OECD",
    "DSD": DSD_OECD_EO,
    "kwargs": {
        "provider_name": "OECD",
        "dataset_code": "EO",
        "dsd_filepath": DSD_OECD_EO["filepaths"]["datastructure"],
        "dsd_id": DSD_OECD_EO["dsd_id"],
    },
    "series_accept": 4,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 465,
    "series_key_first": 'FRA.CB.Q',
    "series_key_last": 'FRA.CB.A',
    "series_sample": {
        "provider_name": "OECD",
        "dataset_code": "EO",
        'key': 'FRA.CB.Q',
        'name': 'France - Current account balance, value - Quarterly',
        'frequency': 'Q',
        'last_update': None,
        'first_value': {
            'value': '4295445496.82796',
            'period': '1973-Q1',
            'attributes': {},
        },
        'last_value': {
            'value': '9811246401.83801',
            'period': '2017-Q4',
            'attributes': {},
        },
        'dimensions': {
            'LOCATION': 'FRA',
            'VARIABLE': 'CB',
            'FREQUENCY': 'Q',
        },
        'attributes': {
            'TIME_FORMAT': 'P3M',
            'POWERCODE': '0',
            'UNIT': 'EUR'
        },
    }
}

_DATA_ECB = {
    "filepath": None,
    "klass": None,
    "DSD": DSD_ECB,
    "kwargs": {
        "provider_name": "ECB",
        "dataset_code": "EXR",
        #"field_frequency": "FREQ",
        #"dimension_keys": DSD_ECB["dimension_keys"],
        "dsd_filepath": DSD_ECB["filepaths"]["datastructure"],
        "dsd_id": DSD_ECB["dsd_id"],
        #"frequencies_supported": ["A", "D", "M", "Q", "W"]
    },
    "series_accept": 10,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 9195,
    "series_key_first": 'A.ARS.EUR.SP00.A',
    "series_key_last": 'Q.AUD.EUR.SP00.A',
    "series_sample": {
        "provider_name": "ECB",
        "dataset_code": "EXR",
        'key': 'A.ARS.EUR.SP00.A',
        'name': 'Indicative exchange rate, Argentine peso/Euro, 2:15 pm (C.E.T.)',
        'frequency': 'A',
        'last_update': None,
        'first_value': {
            'value': '0.895263095238095',
            'period': '2001',
            'attributes': {
                'OBS_STATUS': 'A',
                'OBS_COM': "Indicative rate"
            },
        },
        'last_value': {
            'value': '10.252814453125001',
            'period': '2015',
            'attributes': {
                'OBS_STATUS': 'A',
                'OBS_COM': "Indicative rate"
            },
        },
        'dimensions': {
            'FREQ': 'A',
            'CURRENCY': 'ARS',
            'CURRENCY_DENOM': 'EUR',
            'EXR_TYPE': 'SP00',
            'EXR_SUFFIX': 'A',
        },
        'attributes': {
            'COLLECTION': 'A',
            'DECIMALS': '5',
            'SOURCE_AGENCY': '4F0',
            'TITLE': 'Argentine peso/Euro',
            'TITLE_COMPL': 'Indicative exchange rate, Argentine peso/Euro, 2:15 pm (C.E.T.)',
            'UNIT': 'ARS',
            'UNIT_MULT': '0'
        }
    }      
}

_DATA_INSEE = {
    "filepath": None,
    "klass": None,
    "DSD": DSD_INSEE,
    "kwargs": {
        "provider_name": "INSEE",
        "dataset_code": "IPI-2010-A21",
        "dsd_filepath": DSD_INSEE["filepaths"]["datastructure"],
        "dsd_id": DSD_INSEE["dsd_id"],
    },
    "series_accept": 20,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 3240,
    "series_key_first": '001654489',
    "series_key_last": '001655704',
    "series_sample": {
        "provider_name": "INSEE",
        "dataset_code": "IPI-2010-A21",
        'key': '001654489',
        'name': 'Monthly - B - Mining and quarrying - Raw index',
        'frequency': 'M',
        'last_update': None,
        'first_value': {
            'value': '139.22',
            'period': '1990-01',
            'attributes': {
                "OBS_STATUS": "A"
            },
        },
        'last_value': {
            'value': '96.98',
            'period': '2015-11',
            'attributes': {
                "OBS_STATUS": "A"
            },
        },
        'dimensions': {
           'FREQ': 'M',
           'PRODUIT': 'B',
           'NATURE': 'BRUT',
        },
        'attributes': {
           'BASE_PER': '2010',
           'DECIMALS': '2',
           'IDBANK': '001654489',
           'LAST_UPDATE': '2016-01-08',
           'REF_AREA': 'FM',
           'TIME_PER_COLLECT': 'PERIODE',
           'TITLE': 'Indice brut de la production industrielle (base 100 en 2010) - Industries extractives (NAF rév. 2, niveau section, poste B)',
           'UNIT_MEASURE': 'SO',
           'UNIT_MULT': '0'
        }
    }
}

DATA_ECB_GENERIC = _DATA_ECB.copy()
DATA_ECB_GENERIC["filepath"] = filepath("ecb", "ecb-data-generic-2.1.xml")
DATA_ECB_GENERIC["klass"] = "XMLGenericData_2_1_ECB"

DATA_ECB_SPECIFIC = _DATA_ECB.copy()
DATA_ECB_SPECIFIC["filepath"] = filepath("ecb", "ecb-data-specific-2.1.xml")
DATA_ECB_SPECIFIC["klass"] = "XMLSpecificData_2_1_ECB"
DATA_ECB_SPECIFIC["series_all_values"] = 9205

DATA_INSEE_GENERIC = _DATA_INSEE.copy()
DATA_INSEE_GENERIC["filepath"] = filepath("insee", "insee-data-generic-2.1.xml")
DATA_INSEE_GENERIC["klass"] = "XMLGenericData_2_1_INSEE"

DATA_INSEE_SPECIFIC = _DATA_INSEE.copy()
DATA_INSEE_SPECIFIC["filepath"] = filepath("insee", "insee-data-specific-2.1.xml")
DATA_INSEE_SPECIFIC["klass"] = "XMLSpecificData_2_1_INSEE"

