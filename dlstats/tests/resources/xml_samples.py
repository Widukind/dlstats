
import os

BASE_RESOURCES_DIR = os.path.abspath(os.path.dirname(__file__))
RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "xmlutils"))

def filepath(provider, filename):
    return os.path.abspath(os.path.join(RESOURCES_DIR, provider, filename))    

DSD_FED = {
    "provider": "FED",
    "filepaths": {
        "datastructure": filepath("fed", "fed-structure-1.0.xml"),
    },
    "dataset_code": "G19",
    "dsd_id": "G19",
    "is_completed": True,
    "categories_key": "PEI",
    "categories_parents": None,
    "categories_root": ["PEI", "FA"],    
    "concept_keys": ['TIME_PERIOD', 'FREQ', 'OBS_STATUS', 'OBS_VALUE', 'UNIT', 'UNIT_MULT', 'CURRENCY', 'SERIES_NAME', 'CREDTYP', 'HOLDER', 'DATAREP', 'TERMS', 'ISSUE', 'SA'],
    "codelist_keys": ['CL_OBS_STATUS', 'CL_FREQ', 'CL_UNIT_MULT', 'CL_UNIT', 'CL_CURRENCY', 'CL_CCOUT_CREDTYP', 'CL_CCOUT_HOLDER', 'CL_CCOUT_DATAREP', 'CL_ISSUE', 'CL_TERMS', 'CL_SA'],
    "codelist_count": {
        "CL_OBS_STATUS": 4,
        "CL_FREQ": 50,
        "CL_UNIT_MULT": 9,
        "CL_UNIT": 413,
        "CL_CURRENCY": 200,
        "CL_CCOUT_CREDTYP": 5,
        "CL_CCOUT_HOLDER": 10,
        "CL_CCOUT_DATAREP": 3,
        "CL_ISSUE": 2,
        "CL_TERMS": 8,
        "CL_SA": 2,                           
    },
    "dimension_keys": ['CREDTYP', 'HOLDER', 'DATAREP', 'SA', 'FREQ', 'ISSUE', 'TERMS'],
    "dimension_count": {
        "CREDTYP": 5,
        "HOLDER": 10,
        "DATAREP": 3,
        "SA": 2,
        "FREQ": 50,
        "ISSUE": 2,
        "TERMS": 8,
    },
    "attribute_keys": ['UNIT', 'UNIT_MULT', 'CURRENCY', 'SERIES_NAME', 'OBS_STATUS'],
    "attribute_count": {
        "UNIT": 413,
        "UNIT_MULT": 9,
        "CURRENCY": 200,
        "SERIES_NAME": 0,
        "OBS_STATUS": 4,
    }
}

DSD_EUROSTAT = {
    "provider": "EUROSTAT",
    "filepaths": {
        "datastructure": filepath("eurostat", "eurostat-datastructure-2.0.xml"),
    },
    "dataset_code": "nama_10_fcs",
    "dsd_id": "nama_10_fcs_DSD",
    "is_completed": True,
    "categories_key": "nama_10_ma",
    "categories_parents": ["data", "economy", "na10", "nama_10"],
    "categories_root": ["data"],    
    "concept_keys": ['FREQ', 'unit', 'na_item', 'geo', 'TIME_PERIOD', 'OBS_VALUE', 'OBS_STATUS', 'TIME_FORMAT'],
    "codelist_keys": ['CL_UNIT', 'CL_NA_ITEM', 'CL_GEO', 'CL_TIME_FORMAT', 'CL_FREQ', 'CL_OBS_STATUS'],
    "codelist_count": {
        "CL_UNIT": 12,
        "CL_NA_ITEM": 10,
        "CL_GEO": 33,
        "CL_TIME_FORMAT": 7,
        "CL_FREQ": 9,
        "CL_OBS_STATUS": 12,    
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
    "dsd_id": "DCS",
    "is_completed": False,
    "concept_keys": [],
    "codelist_keys": [],
    "codelist_count": None,
    "dimension_keys": [],
    "dimension_count": None,
    "attribute_keys": [],
    "attribute_count": None,
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
    "is_completed": True,
    "dataflow_key": 'EXR',
    "categorisations_key": "53A341E8-D48B-767E-D5FF-E2E3E0E2BB19",
    "categories_key": "07",
    "categories_parents": None,
    "categories_root": ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11'],
    "concept_keys": [
        'COUNT_AREA', 'CURRENCY', 'DATA_TYPE_PDF', 'SUFFIX_QNA', 'ESA95TP_TRANS', 
        'TRD_PRODUCT', 'IR_TYPE', 'BDS_ITEM', 'DD_SUFFIX', 'SAFE_QUESTION', 
        'BANKING_IND', 'SEC_VALUATION', 'BKN_TYPE', 'STS_CONCEPT', 'FM_OUTS_AMOUNT', 
        'DD_ECON_CONCEPT', 'MATURITY_CAT', 'OBS_STATUS', 'STS_INSTITUTION', 
        'CB_VAL_METHOD', 'DATA_TYPE_PSS', 'PROVIDER_FM', 'SSS_INFO_TYPE', 
        'PD_ITEM', 'FM_COUPON_RATE', 'NON_RESID_ECON_ACT', 'REP_CTY', 
        'OFI_REP_SECTOR', 'ESA95TP_PRICE', 'BS_COUNT_SECTOR', 'EFFECT_DOMAIN', 
        'ESA95TP_CPAREA', 'EMBARGO', 'METHOD_PUBL', 'REP_DELAY', 
        'GOVNT_REF_SECTOR', 'MUFA_SOURCE', 'AME_REFERENCE', 'DD_TRANSF', 
        'BKN_SERIES', 'WEO_ITEM', 'BLS_AGG_METHOD', 'ESA95TP_CONS', 'SOURCE_DETAIL', 
        'BOP_ITEM', 'SHI_SUFFIX', 'SOURCE_PUBL_2', 'FIRM_TURNOVER', 
        'IVF_REP_SECTOR', 'TRG_ACCOUNT', 'COMP_METHOD', 'FVC_ITEM', 'OBS_VALUE', 
        'STS_SUFFIX', 'CB_EXP_TYPE', 'AREA_DEFINITION', 'MATURITY_NOT_IRATE', 
        'FM_MATURITY', 'PORTFOLIO_CAT', 'AME_REF_AREA', 'COLLECTION_DETAIL', 'SOURCE_DETAIL_2', 'ISSUER_RBG', 'REPO_CPARTY', 'COMPILATION', 'METHOD_DETAIL', 'MUFA_DEBT_AREA', 'ESA95TP_REGION', 'FCT_TOPIC', 'SUBJECT_MEI', 'DATA_TYPE_MM', 'REPORTING_SECTOR', 'SAFE_ANSWER', 'PD_SEAS_EX', 'DATA_TYPE_MUFA', 'BLS_COUNT_DETAIL', 'SEC_SUFFIX', 'VIS_CTY', 'SAFE_ITEM', 'RT_DENOM', 'CONF_STATUS', 'DATA_TYPE_BKN', 'DATA_TYPE_LIG', 'OFI_ITEM', 'REF_SECTOR', 'UNIT_INDEX_BASE', 'ESA95_SUFFIX', 'COUNTERPART_SECTOR', 'MUFA_ITEM', 'TRG_BAND', 'BANKING_ITEM', 'IN_OUT_DATA_IFI', 'OBS_COM', 'PSS_SYSTEM', 'STOCK_FLOW', 'COUNT_AREA_IFS', 'OPTION_TYPE_PDF', 'PD_ORIGIN', 'SAFE_DENOM', 'UNIT_MEASURE', 'ICP_SUFFIX', 'FM_STRIKE_PRICE', 'STO', 'ADJUSTMENT','INT_ACC_ITEM', 'ADJU_DETAIL', 'UNIT_MULT', 'SSS_INSTRUMENT', 'MATURITY_RES', 'ESA95_ACCOUNT', 'PRICE_BASE', 'BANKING_SUFFIX', 'DATA_TYPE_IFI', 'EXT_TITLE', 'VAL_ROW', 'ESA95TP_BRKDWN', 'BS_REP_SECTOR', 'NOM_CURR', 'FM_LOT_SIZE', 'VAL_REPORT', 'CIBL_CATEGORY', 'HOLDER_SECTOR', 'REF_PERIOD_DETAIL', 'CIBL_TYPE', 'ESA95_UNIT', 'PRICE_TYPE', 'TPH', 'ORGANISATION', 'ESA95_BREAKDOWN', 'FIRM_SIZE', 'COMPILING_ORG', 'RPP_SOURCE', 'FVC_REP_SECTOR', 'UNIT', 'MEASURE_MEI', 'REF_AREA_MEI', 'BANKING_REF', 'PUBL_MU', 'ISSUER_AREA', 'PROVIDER_FM_ID', 'LIG_ITEM', 'TRD_FLOW', 'MARKET_ROLE', 'ESA95TP_SECTOR', 'COMMENT_TS', 'TIME_PER_COLLECT', 'CURRENCY_P_H', 'MATURITY_TYPE', 'PSS_INSTRUMENT', 'MUFA_CRED_AREA', 'UNIT_DETAIL', 'DATA_TYPE_DBI', 'VAL_ITEM', 'EXT_UNIT_MULT', 'AME_AGG_METHOD', 'MARKET_TRANS', 'SOURCE_PUB', 'PROPERTY_CPP', 'PUBL_PUBLIC', 'ICP_ITEM', 'SERIES_DENOM', 'AME_ITEM', 'EAPLUS_FLAG', 'SOURCE_DATA', 'SHI_INDICATOR', 'ISSUER_SECTOR', 'IR_FV_TYPE', 'OECD_A16_CODE', 'CB_REP_FRAMEWRK', 'WOB_TAXATION', 'BIS_BLOCK', 'COUNT_SECTOR', 'FLOW_STOCK_ENTRY', 'DATA_TYPE_SEC', 'BIS_TOPIC', 'EONIA_BANK', 'MEASURE_QNA', 'MUFA_CRED_SECTOR', 'OEO_CODE', 'PRE_BREAK_VALUE', 'DEBT_TYPE', 'RT_ECON_CONCEPT', 'ESCB_FLAG', 'REF_AREA', 'CB_REP_SECTOR', 'ACCOUNT_ENTRY', 'R_L_SECTOR', 'RBG_ID', 'TITLE_COMPL', 'CURR_BRKDWN', 'MA_FLAG', 'FM_PUT_CALL', 'FM_IDENTIFIER', 'SAFE_FILTER', 'TRG_CATEGORY', 'ICPF_ITEM', 'RESID_ECON_ACT', 'ADJUST_DETAIL', 'VALUATION', 'COMP_APPROACH', 'FIRM_OWNERSHIP', 'FM_CONTRACT_TIME', 'SUBJECT_QNA', 'MEASURE_SNA', 'ESA95TP_DC_AL', 'AGG_EQUN', 'COLLATERAL', 'AME_TRANSFORMATION', 'BIS_SUFFIX', 'COUNTERPART_AREA', 'GROUP_TYPE', 'DATA_TYPE_BOP', 'ESA95TP_SUFFIX', 'ICO_PAY', 'DATA_TYPE_FM', 'FREQ', 'BS_NFC_ACTIVITY', 'CB_SECTOR_SIZE', 'CURRENCY_DENOM', 'FCT_SOURCE', 'STS_CLASS','PROPERTY_SUFFIX', 'DISS_ORG', 'FIRM_SECTOR', 'CCP_SYSTEM', 'EMBARGO_DETAIL', 'BS_ITEM', 'EONIA_ITEM', 'TIME_HORIZON', 'BREAKS', 'GOVNT_COUNT_SECTOR', 'SOURCE_AGENCY_2', 'FVC_ORI_SECTOR', 'OLV_INDICATOR', 'CURRENCY_TRANS', 'MFI_LIST', 'EXR_SUFFIX', 'HOLDER_AREA', 'BKN_DENOM', 'MATURITY', 'MM_SEGMENT', 'INSTRUMENT_FM', 'COMMENT_OBS', 'DOM_SER_IDS', 'GOVNT_ST_SUFFIX', 'SEC_ITEM', 'GOVNT_VALUATION', 'FCT_BREAKDOWN', 'FLOATING_RATE_BASE', 'WOB_CONCEPT', 'DATA_TYPE', 'SEC_ISSUING_SECTOR', 'CURRENCY_S', 'IVF_ITEM', 'NAT_TITLE', 'BLS_ITEM',
        'CIBL_TABLE', 'PSS_INFO_TYPE', 'EXT_REF_AREA', 'OBS_PRE_BREAK', 'BOP_BASIS', 
        'DATA_TYPE_MIR', 'TRD_SUFFIX', 'INS_BUS_TYPE', 'MUFA_VALUATION', 
        'CB_PORTFOLIO', 'OIL_PRODUCT', 'SSS_SYSTEM', 'METHOD_REF', 'SOURCE_AGENCY', 
        'FCT_HORIZON', 'WEO_REF_AREA', 'DATA_TYPE_FXS', 'NA_PRICE', 'IFS_CODE', 
        'SSI_INDICATOR', 'PUBLICATION', 'BKN_ITEM', 'OTHER_METH_EXPL', 'IS_IN_EADB', 
        'BS_SUFFIX', 'TR_TYPE', 'ESA95TP_CPSECTOR', 'VAL_COLUMN', 'PROPERTY_IND', 
        'MM_BANK', 'MUFA_DEBT_SECTOR', 'FIRM_AGE', 'DATA_COMP', 'METHOD_AGENCY', 
        'ESA95TP_DENOM', 'PUBL_ECB', 'RPP_GEO_COV', 'RPP_SUFFIX', 'AVAILABILITY', 
        'CB_ITEM', 'ESA95TP_ASSET', 'MFI_STATUS', 'CREDIT_RATING', 'BLS_COUNT', 
        'RIR_SUFFIX', 'IR_BUS_COV', 'OBS_CONF', 'SECURITY_TYPE', 'INSTR_ASSET', 
        'EXR_TYPE', 'SURVEY_FREQ', 'COVERAGE', 'UNIT_PRICE_BASE', 'U_A_SECTOR', 
        'GOVNT_ITEM_ESA', 'DECIMALS', 'MATURITY_ORIG', 'MFI_LIST_IND', 'TRG_ANCILLARY', 
        'CPP_METHOD', 'EXT_UNIT', 'ESA95TP_COM', 'COLLECTION', 'RPP_DWELLING', 
        'AMOUNT_CAT', 'BANK_SELECTION', 'ICO_UNIT', 'ISSUER_IN', 'TITLE', 
        'SUBJECT_SNA', 'FXS_OP_TYPE', 'SEE_SYSTEM', 'SECURITISATION_TYP', 
        'TIME_FORMAT', 'FUNCTIONAL_CAT', 'TRADE_WEIGHT', 'SUBJECT_OEO', 'AME_UNIT', 
        'TIME_PERIOD'                     
    ],
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
        "CL_AREA": filepath("insee", "insee-codelist-cl_area.xml"),
        "CL_TIME_COLLECT": filepath("insee", "insee-codelist-cl_time_collect.xml"),
        "CL_OBS_STATUS": filepath("insee", "insee-codelist-cl_obs_status.xml"),
    },
    "dataset_code": "IPI-2010-A21",
    "dataset_name": "Industrial production index (base 2010) - NAF level A21",
    "dsd_id": "IPI-2010-A21",
    "is_completed": True,
    "dataflow_key": 'IPI-2010-A21',
    "categorisations_key": "CAT_IPI-2010_IPI-2010-A21",
    "categories_key": "IPI-2010",
    "categories_parents": ['PRODUCTION-ENT', 'INDUSTRIE-CONST', 'PRODUCTION-IND'],    
    "categories_root": ['COMPTA-NAT', 'ENQ-CONJ', 'PRIX', 'PRODUCTION-ENT', 
                        'DEMO-ENT', 'POPULATION', 'MARCHE-TRAVAIL', 
                        'SALAIRES-REVENUS', 'ECHANGES-EXT', 'CONDITIONS-VIE-SOCIETE', 
                        'SERVICES-TOURISME-TRANSPORT', 'SRGDP'],    
    "concept_keys": ['FREQ', 'ACTIVITE', 'FINANCEMENT', 'SECT-INST', 'COMPTE', 
                     'OPERATION', 'NATURE-FLUX', 'INDEX', 'FORME-VENTE', 
                     'MARCHANDISE', 'INSTRUMENT', 'QUESTION', 'PRODUIT', 
                     'BRANCHE', 'PRATIQUE', 'METIER', 'CAT-DE', 'CAUSE-DECES', 
                     'TYPE-ENT', 'TYPE-ETAB', 'FONCTION', 'FACTEUR-INV', 
                     'DEST-INV', 'ETAT-CONSTRUCTION', 'TYPE-RESEAU', 
                     'DEMOGRAPHIE', 'INDICATEUR', 'TOURISME-INDIC', 'TYPE-SAL', 
                     'COTISATION', 'CLIENTELE', 'TYPE-CREAT-ENT', 'FORME-EMP', 
                     'TYPE-EMP', 'TYPE-OE', 'TYPE-CESS-ENT', 'LOCAL', 
                     'CARAC-LOG', 'LOGEMENT', 'POPULATION', 'TYPE-FAMILLE', 
                     'TYPE-MENAGE', 'CARBURANT', 'TYPE-VEHICULE', 'FORMATION', 
                     'EFFOPE', 'UNITE-URBAINE', 'SPECIALITE-SANTE', 
                     'ACCUEIL-PERS-AGEES', 'DIPLOME', 'ETAB-SCOL', 'CHAMP-GEO', 
                     'GEOGRAPHIE','LOCALISATION', 'RESIDENCE', 'ZONE-GEO', 
                     'HALO', 'TYPE-EVO', 'SEXE', 'TYPE-FLUX', 'CAT-FP', 
                     'TYPE-COTIS', 'PERIODE', 'AGE', 'TAILLE-MENAGE', 
                     'TAILLE-ENT', 'ANCIENNETE', 'QUOTITE-TRAV', 'PRIX', 
                     'UNITE', 'CORRECTION', 'NATURE', 'TYPE-SURF-ALIM', 
                     'DATE-DEF-ENT', 'DEVISE', 'MONNAIE', 'TYPE-TX-CHANGE', 
                     'TYPE-PRIX', 'BASIND', 'FREQUENCE', 'REPARTITION', 
                     'REVENU','MIN-FPE', 'EXPAGRI', 'OCC-SOL', 'CHEPTEL', 
                     'PROD-VEG', 'FEDERATION', 'DEPARTEMENT', 'REGION', 
                     'IPC-CALC01', 'IDBANK', 'TITLE', 'LAST_UPDATE', 
                     'UNIT_MEASURE', 'UNIT_MULT', 'REF_AREA', 'DECIMALS', 
                     'BASE_PER', 'TIME_PER_COLLECT', 'TIME_PERIOD', 'OBS_VALUE',
                     'OBS_STATUS', 'EMBARGO_TIME'],
    "codelist_keys": ['CL_FREQ', 'CL_NAF2_A21', 'CL_NATURE', 'CL_UNIT', 'CL_AREA', 'CL_TIME_COLLECT', 'CL_OBS_STATUS'],
    "codelist_count": {
        'CL_FREQ': 7, 
        'CL_NAF2_A21': 30, 
        'CL_NATURE': 25,
        'CL_UNIT': 123,
        'CL_AREA': 11,
        'CL_TIME_COLLECT': 7,
        'CL_OBS_STATUS': 10,
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
        'UNIT_MEASURE': 123, 
        'UNIT_MULT': 0, 
        'REF_AREA': 11, 
        'DECIMALS': 0,
        'BASE_PER': 0, 
        'TIME_PER_COLLECT': 7, 
        'OBS_STATUS': 10, 
        'EMBARGO_TIME': 0, 
    },
}


DATA_FED = {
    "filepath": os.path.abspath(os.path.join(RESOURCES_DIR, "fed", "fed-data-1.0.xml")),
    "klass": "XMLData_1_0_FED",
    "DSD": DSD_FED,
    "kwargs": {
        "provider_name": "FED",
        "dataset_code": "G19",
        "field_frequency": "FREQ",
        "dimension_keys": DSD_FED["dimension_keys"],
        #"frequencies_supported": ["M", "A", "D", "Q"],
    },
    "series_accept": 80,
    "series_reject_frequency": 0,
    "series_reject_empty": 0,
    "series_all_values": 47448,
    "series_key_first": "RIFLPBCIANM48_N.M",
    "series_key_last": "DTCTLNV_XDF_BA_N.M",
    "series_sample": {
        "provider_name": "FED",
        "dataset_code": "G19",
        'key': 'RIFLPBCIANM48_N.M',
        'name': 'RIFLPBCIANM48_N.M',
        'frequency': 'M',
        'last_update': None,
        'first_value': {
            'value': '10.20',
            'ordinal': 25,
            'period': '1972-02-29',
            'period_o': '1972-02-29',
            'attributes': {
                'OBS_STATUS': 'A',
            },
        },
        'last_value': {
            'value': '4.11',
            'ordinal': 547,
            'period': '2015-08-31',
            'period_o': '2015-08-31',
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
        "field_frequency": "FREQ",
        "dimension_keys": DSD_EUROSTAT["dimension_keys"],
        "dsd_filepath": DSD_EUROSTAT["filepaths"]["datastructure"],
        "frequencies_supported": [] #TODO: specific: P1Y (A), P3M (Q), P1M (M), P1D (D)
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
            'ordinal': 25,
            'period': '1995',
            'period_o': '1995',
            'attributes': {},
        },
        'last_value': {
            'value': '17517.5',
            'ordinal': 44,
            'period': '2014',
            'period_o': '2014',
            'attributes': {},
        },
        'dimensions': {
            'FREQ': 'A',
            'unit': 'CLV05_MEUR',
            'na_item': 'P311_S14',
            'geo': 'AT',
        },
        'attributes': {
            'TIME_FORMAT': 'P3M',
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
            'ordinal': 380,
            'period': '1972-02-29',
            'period_o': '1972-02-29',
            'attributes': {
                'OBS_STATUS': 'A',
            },
        },
        'last_value': {
            'value': '1789542',
            'ordinal': 550,
            'period': '2015-08-31',
            'period_o': '2015-08-31',
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

_DATA_ECB = {
    "filepath": None,
    "klass": None,
    "DSD": DSD_ECB,
    "kwargs": {
        "provider_name": "ECB",
        "dataset_code": "EXR",
        "field_frequency": "FREQ",
        "dimension_keys": DSD_ECB["dimension_keys"],
        "frequencies_supported": ["A", "D", "M", "Q", "W"]
    },
    "series_accept": 8,
    "series_reject_frequency": 2,
    "series_reject_empty": 0,
    "series_all_values": 9130,
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
            'ordinal': 31,
            'period': '2001',
            'period_o': '2001',
            'attributes': {
                'OBS_STATUS': 'A',
                'OBS_COM': "Indicative rate"
            },
        },
        'last_value': {
            'value': '10.252814453125001',
            'ordinal': 45,
            'period': '2015',
            'period_o': '2015',
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
        "field_frequency": "FREQ",
        "dimension_keys": DSD_INSEE["dimension_keys"],
        "frequencies_rejected": ["S", "B", "I"]
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
        'name': 'Indice brut de la production industrielle (base 100 en 2010) - Industries extractives (NAF rév. 2, niveau section, poste B)',
        'frequency': 'M',
        'last_update': None,
        'first_value': {
            'value': '139.22',
            'ordinal': 240,
            'period': '1990-01',
            'period_o': '1990-01',
            'attributes': {
                "OBS_STATUS": "A"
            },
        },
        'last_value': {
            'value': '96.98',
            'ordinal': 550,
            'period': '2015-11',
            'period_o': '2015-11',
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
DATA_ECB_SPECIFIC["series_all_values"] = 9140

DATA_INSEE_GENERIC = _DATA_INSEE.copy()
DATA_INSEE_GENERIC["filepath"] = filepath("insee", "insee-data-generic-2.1.xml")
DATA_INSEE_GENERIC["klass"] = "XMLGenericData_2_1_INSEE"

DATA_INSEE_SPECIFIC = _DATA_INSEE.copy()
DATA_INSEE_SPECIFIC["filepath"] = filepath("insee", "insee-data-specific-2.1.xml")
DATA_INSEE_SPECIFIC["klass"] = "XMLSpecificData_2_1_INSEE"

