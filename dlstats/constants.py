# -*- coding: utf-8 -*-

import os

ES_INDEX = os.environ.get("WIDUKIND_ES_INDEX", "widukind")

MONGODB_NAME = os.environ.get("WIDUKIND_MONGODB_NAME", "widukind")

COL_CATEGORIES = "categories"

COL_PROVIDERS = "providers"

COL_DATASETS = "datasets"

COL_SERIES = "series"

COL_ALL = [
    COL_CATEGORIES,
    COL_PROVIDERS,
    COL_DATASETS,
    COL_SERIES
]
