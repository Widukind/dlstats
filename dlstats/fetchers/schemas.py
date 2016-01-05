# -*- coding: utf-8 -*-

from datetime import datetime
import bson

from voluptuous import Required, All, Length, Schema, Invalid, Optional, Any, Extra

def date_validator(value):
    """Custom validator (only a few types are natively implemented in voluptuous)
    """
    if isinstance(value, datetime):
        return value
    else:
        raise Invalid('Input date was not of type datetime')

def typecheck(type, msg=None):
    """Coerce a value to a type.

    If the type constructor throws a ValueError, the value will be marked as
    Invalid.
    """
    def validator(value):
        if not isinstance(value,type):
            raise Invalid(msg or ('expected %s' % type.__name__))
        else:
            return value
    return validator

#Schema definition in voluptuous
revision_schema = {str: [{Required('value'): str,
                          Required('releaseDate'): date_validator}]}

codedict_schema = Schema({Extra: dict})

provider_schema = Schema({
    'name': All(str, Length(min=1)),
    'long_name': All(str, Length(min=1)),
    'slug': All(str, Length(min=1)),
    'region': All(str, Length(min=1)),
    'website': All(str, Length(min=9))
    },required=True)

def _data_tree(value):
    return data_tree_schema(value)

data_tree_schema = Schema({
    'name': All(str, Length(min=1)),
    'provider': All(str, Length(min=1)),
    'children': Any(None,[_data_tree]), 
    Optional('doc_href'): Any(None,str),
    Optional('last_update'): Any(None,typecheck(datetime)),
    'category_code': All(str, Length(min=1)),
    Optional('exposed'): typecheck(bool)
    }, required=True)


dataset_schema = Schema({
    'name': All(str, Length(min=1)),
    'provider': All(str, Length(min=1)),
    'dataset_code': All(str, Length(min=1)),
    'doc_href': Any(None,str),
    'last_update': typecheck(datetime),
    'dimension_list': {str: [All()]},
    'attribute_list': Any(None, {str: [(str,str)]}),
    Optional('notes'): str,
    Optional('tags'): [Any(str)],
    'slug': All(str, Length(min=1)),
    },required=True)

series_schema = Schema({
    'name': All(str, Length(min=1)),
    'provider': All(str, Length(min=1)),
    'key': All(str, Length(min=1)),
    'dataset_code': All(str, Length(min=1)),
    'start_date': int,
    'end_date': int,
    'values': [Any(str)],
    'release_dates': [date_validator],
    'attributes': Any({}, {str: [str]}),
    Optional('revisions'): Any(None, revision_schema),
    'dimensions': {str: str},
    'frequency': All(str, Length(min=1)),
    Optional('notes'): Any(None, str),
    Optional('tags'): [Any(str)],
    'slug': All(str, Length(min=1)),
    },required=True)

