# -*- coding: utf-8 -*-

from datetime import datetime

from voluptuous import All, Length, Schema, Invalid, Optional, Any, Extra, Range

def date_validator(value):
    """Custom validator (only a few types are natively implemented in voluptuous)
    """
    if isinstance(value, datetime):
        return value
    else:
        raise Invalid('Input date was not of type datetime')

def typecheck(_type, msg=None):
    """Coerce a value to a type.

    If the type constructor throws a ValueError, the value will be marked as
    Invalid.
    """
    def validator(value):
        if not isinstance(value, _type):
            raise Invalid(msg or ('expected %s' % _type.__name__))
        else:
            return value
    return validator

codedict_schema = Schema({Extra: dict})

provider_schema = Schema({
    'enable': typecheck(bool),
    'lock': typecheck(bool),
    'name': All(str, Length(min=1)),
    'long_name': All(str, Length(min=1)),
    'version': All(int, Range(min=1)),
    'slug': All(str, Length(min=1)),
    'region': All(str, Length(min=1)),
    'website': All(str, Length(min=9)),
    'metadata': Any(None, dict),
    Optional('terms_of_use'): Any(None, str),
},required=True)

data_tree_datasets_schema = Schema({
    'dataset_code': All(str, Length(min=1)),
    'name': All(str, Length(min=1)),
    'last_update': Any(None, typecheck(datetime)),
    'metadata': Any(None, dict),
}, required=True)

category_schema = Schema({
    'enable': typecheck(bool),
    'lock': typecheck(bool),
    'slug': All(str, Length(min=1)),
    'provider_name': All(str, Length(min=1)),
    'category_code': All(str, Length(min=1)),
    'position': int,
    'name': All(str, Length(min=1)),
    'parent': Any(None, str),
    'all_parents': Any(None, [], [str]),
    'datasets': [data_tree_datasets_schema],
    'doc_href': Any(None, str),
    Optional('tags'): Any(None, list),
    'metadata': Any(None, dict),
}, required=True)

dataset_schema = Schema({
    'enable': typecheck(bool),
    'lock': typecheck(bool),
    'name': All(str, Length(min=1)),
    'provider_name': All(str, Length(min=1)),
    'dataset_code': All(str, Length(min=1)),
    'doc_href': Any(None, str),
    'last_update': typecheck(datetime),
    'dimension_keys': Any(None, list, Length(min=1)),
    'attribute_keys': Any(None, list, Length(min=1)),
    'codelists': Any(None, dict),
    'concepts': Any(None, dict),
    Optional('tags'): Any(None, list),
    'metadata': Any(None, dict),
    Optional('notes'): Any(None, str),
    'slug': All(str, Length(min=1)),
    'download_first': typecheck(datetime),
    'download_last': typecheck(datetime),
    },required=True)

series_value_schema = Schema({
    'value': str,
    'period': All(str, Length(min=1)),
    'attributes': Any(None, dict),
}, required=True)

series_schema = Schema({
    'version': All(int, Range(min=0)),
    'last_update_ds': typecheck(datetime),
    'last_update_widu': typecheck(datetime),
    'name': All(str, Length(min=1)),
    'provider_name': All(str, Length(min=1)),
    'key': All(str, Length(min=1)),
    'dataset_code': All(str, Length(min=1)),
    'start_date': int,
    'end_date': int,
    'start_ts': typecheck(datetime),
    'end_ts': typecheck(datetime),    
    'values': [series_value_schema],
    'attributes': Any(None, dict),
    'dimensions': {str: str},
    'codelists': Any(None, dict),
    'frequency': All(str, Length(min=1)),
    Optional('notes'): Any(None, str),
    Optional('tags'): Any(None, list),
    'slug': All(str, Length(min=1)),
}, required=True)


