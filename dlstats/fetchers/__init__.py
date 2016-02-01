# -*- coding: utf-8 -*-

__all__ = ['FETCHERS']

FETCHERS = {}

try:
    from dlstats.fetchers.bis import BIS
    FETCHERS['BIS'] = BIS
    __all__.append('BIS')
except ImportError:
    pass

#try:
#    from dlstats.fetchers.oecd import OECD
#    FETCHERS['OECD'] = OECD
#    __all__.append('OECD')
#except ImportError:
#    pass

try:
    from dlstats.fetchers.eurostat import Eurostat
    FETCHERS['EUROSTAT'] = Eurostat
    __all__.append('Eurostat')
except ImportError:
    pass

#try:
#    from dlstats.fetchers.world_bank import WorldBank
#    FETCHERS['WORLDBANK'] = WorldBank
#    __all__.append('WorldBank')
#except ImportError:
#    pass

try:
    from dlstats.fetchers.imf import IMF
    FETCHERS['IMF'] = IMF
    __all__.append('IMF')
except ImportError:
    pass

#try:
#    from dlstats.fetchers.bea import BEA
#    FETCHERS['BEA'] = BEA
#    __all__.append('BEA')
#except ImportError:
#    pass

try:
    from dlstats.fetchers.ecb import ECB
    FETCHERS['ECB'] = ECB
    __all__.append('ECB')
except ImportError:
    pass

try:
    from dlstats.fetchers.esri import Esri
    FETCHERS['ESRI'] = Esri
    __all__.append('Esri')
except ImportError:
    pass

try:
    from dlstats.fetchers.insee import INSEE
    FETCHERS['INSEE'] = INSEE
    __all__.append('INSEE')
except ImportError:
    pass

#try:
#    from dlstats.fetchers.destatis import DESTATIS
#    FETCHERS['DESTATIS'] = DESTATIS
#    __all__.append('DESTATIS')
#except ImportError:
#    pass

try:
    from dlstats.fetchers.fed import FED
    FETCHERS['FED'] = FED
    __all__.append('FED')
except ImportError:
    pass
