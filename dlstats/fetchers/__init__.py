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
    FETCHERS['Eurostat'] = Eurostat
    #TODO: FETCHERS_DATASETS['EUROSTAT'] = None
    __all__.append('Eurostat')
except ImportError:
    pass

try:
    from dlstats.fetchers.world_bank import WorldBank
    FETCHERS['WorldBank'] = WorldBank
    #TODO: FETCHERS_DATASETS['WB'] = None
    __all__.append('WorldBank')
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
    from dlstats.fetchers.insee import INSEE
    FETCHERS['INSEE'] = INSEE
    __all__.append('INSEE')
except ImportError:
    pass
