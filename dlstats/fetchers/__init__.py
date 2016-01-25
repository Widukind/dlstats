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
    from dlstats.fetchers.IMF import IMF, DATASETS as DATASETS_IMF
    FETCHERS['IMF'] = IMF
    FETCHERS_DATASETS['IMF'] = DATASETS_IMF
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

try:
    from dlstats.fetchers.insee import INSEE
    FETCHERS['INSEE'] = INSEE
    __all__.append('INSEE')
except ImportError:
    pass

try:
    from dlstats.fetchers.destatis import DESTATIS
    FETCHERS['DESTATIS'] = DESTATIS
    __all__.append('DESTATIS')
except ImportError:
    pass

try:
    from dlstats.fetchers.fed import FED
    FETCHERS['FED'] = FED
    __all__.append('FED')
except ImportError:
    pass
