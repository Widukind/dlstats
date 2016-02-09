# -*- coding: utf-8 -*-

import os
from widukind_common.constants import *

CACHE_URL = os.environ.get('WIDUKIND_CACHE_URL', 'simple') #redis://localhost:6379/0

SCHEMAS_VALIDATION_DISABLE = os.environ.get('WIDUKIND_SCHEMAS_VALIDATION_DISABLE', 'false')