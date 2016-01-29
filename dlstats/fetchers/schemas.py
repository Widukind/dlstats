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
    'tags': Any(None, [], [str]),
    'metadata': Any(None, dict),
}, required=True)

dataset_schema = Schema({
    'enable': typecheck(bool),
    'lock': typecheck(bool),
    'name': All(str, Length(min=1)),
    'provider_name': All(str, Length(min=1)),
    'dataset_code': All(str, Length(min=1)),
    'doc_href': Any(None,str),
    'last_update': typecheck(datetime),

    'dimension_list': Any(None, {str: [All()]}),
    'attribute_list': Any(None, {str: [(str,str)]}),
    
    'dimension_keys': Any(None, list, Length(min=1)),
    'attribute_keys': Any(None, list, Length(min=1)),
    'codelists': Any(None, dict),
    'concepts': Any(None, dict),
    
    'metadata': Any(None, dict),
    Optional('notes'): str,
    Optional('tags'): [Any(str)],
    'slug': All(str, Length(min=1)),
    'download_first': typecheck(datetime),
    'download_last': typecheck(datetime),
    },required=True)

series_revision_schema = Schema({
    'value': str,
    'revision_date': date_validator,
}, required=True)

series_value_schema = Schema({
    'value': str,
    'release_date': date_validator,
    'ordinal': int,
    'period_o': All(str, Length(min=1)),
    'period': All(str, Length(min=1)),
    'attributes': Any(None, dict),
    Optional('revisions'): [series_revision_schema],
}, required=True)

series_schema = Schema({
    'name': All(str, Length(min=1)),
    'provider_name': All(str, Length(min=1)),
    'key': All(str, Length(min=1)),
    'dataset_code': All(str, Length(min=1)),
    'start_date': int,
    'end_date': int,
    'values': [series_value_schema],
    'attributes': Any(None, dict),
    'dimensions': {str: str},
    'frequency': All(str, Length(min=1)),
    Optional('notes'): Any(None, str),
    Optional('tags'): [Any(str)],
    'slug': All(str, Length(min=1)),
}, required=True)


