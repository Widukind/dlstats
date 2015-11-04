# -*- coding: utf-8 -*-

__all__ = ['FETCHERS', 'FETCHERS_DATASETS']

FETCHERS = {}
FETCHERS_DATASETS = {}

try:
    from dlstats.fetchers.bis import BIS, DATASETS as DATASETS_BIS
    FETCHERS['BIS'] = BIS
    FETCHERS_DATASETS['BIS'] = DATASETS_BIS.copy()
    __all__.append('BIS')
except ImportError:
    pass

try:
    from dlstats.fetchers.oecd import OECD, DATASETS as DATASETS_OECD
    FETCHERS['OECD'] = OECD
    FETCHERS_DATASETS['OECD'] = DATASETS_OECD.copy()
    __all__.append('OECD')
except ImportError:
    pass

try:
    from dlstats.fetchers.eurostat import Eurostat
    FETCHERS['EUROSTAT'] = Eurostat
    #TODO: FETCHERS_DATASETS['EUROSTAT'] = None
    __all__.append('Eurostat')
except ImportError:
    pass

try:
    from dlstats.fetchers.insee import Insee
    FETCHERS['INSEE'] = Insee
    #TODO: FETCHERS_DATASETS['INSEE'] = None
    __all__.append('Insee')
except ImportError:
    pass

try:
    from dlstats.fetchers.world_bank import WorldBank
    FETCHERS['WB'] = WorldBank
    #TODO: FETCHERS_DATASETS['WB'] = None
    __all__.append('WorldBank')
except ImportError:
    pass

try:
    from dlstats.fetchers.IMF import IMF
    FETCHERS['IMF'] = IMF
    #TODO: FETCHERS_DATASETS['IMF'] = None
    __all__.append('IMF')
except ImportError:
    pass

try:
    from dlstats.fetchers.BEA import BEA
    FETCHERS['BEA'] = BEA
    #TODO: FETCHERS_DATASETS['BEA'] = None
    __all__.append('BEA')
except ImportError:
    pass

try:
    from dlstats.fetchers.ecb import ECB
    FETCHERS['ECB'] = ECB
    #TODO: FETCHERS_DATASETS['ECB'] = None
    __all__.append('ECB')
except ImportError:
    pass

try:
    from dlstats.fetchers.esri import Esri
    FETCHERS['ESRI'] = Esri
    #TODO: FETCHERS_DATASETS['ESRI'] = None
    __all__.append('Esri')
except ImportError:
    pass
