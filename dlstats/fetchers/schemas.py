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
    'longName': All(str, Length(min=1)),
    'region': All(str, Length(min=1)),
    'website': All(str, Length(min=9))
    },required=True)

category_schema = Schema({
    'name': All(str, Length(min=1)),
    'provider': All(str, Length(min=1)),
    'children': Any(None,[typecheck(bson.objectid.ObjectId)]), 
    Optional('docHref'): Any(None,str),
    Optional('lastUpdate'): Any(None,typecheck(datetime)),
    'categoryCode': All(str, Length(min=1)),
    'exposed': typecheck(bool)
    }, required=True)


dataset_schema = Schema({
    'name': All(str, Length(min=1)),
    'provider': All(str, Length(min=1)),
    'datasetCode': All(str, Length(min=1)),
    'docHref': Any(None,str),
    'lastUpdate': typecheck(datetime),
    'dimensionList': {str: [All()]},
    'attributeList': Any(None, {str: [(str,str)]}),
    Optional('notes'): str
    },required=True)

series_schema = Schema({
    'name': All(str, Length(min=1)),
    'provider': All(str, Length(min=1)),
    'key': All(str, Length(min=1)),
    'datasetCode': All(str, Length(min=1)),
    'startDate': int,
    'endDate': int,
    'values': [Any(str)],
    'releaseDates': [date_validator],
    'attributes': Any({}, {str: [str]}),
    Optional('revisions'): Any(None, revision_schema),
    'dimensions': {str: str},
    'frequency': All(str, Length(min=1)),
    Optional('notes'): Any(None, str)
    },required=True)

es_dataset_schema = Schema({
    'name': All(str, Length(min=1)),
    'provider': All(str, Length(min=1)),
    'datasetCode': All(str, Length(min=1)),
    'docHref': Any(None,str),
    'lastUpdate': typecheck(datetime),
    'codeList': {str: [All([])]},
    'frequencies': [str]
    },required=True)

es_series_schema = Schema({
    '_op_type': Any('index','update'), 
    '_index': str,
    '_type': 'series',
    '_id': str, 
    'provider': str,
    'key': str,
    'name': str,
    'datasetCode': str,
    'dimensions': {str: [str, str]},
    'frequency': str
    }, required=True)

es_doc_schema = {
    Optional('name'): str,
    Optional('dimensions'): {str: [str, str]},
}

es_series_update_schema = Schema({
    '_op_type': Any('index','update'), 
    '_index': str,
    '_type': 'series',
    '_id': str,
    'doc': es_doc_schema
    }, required=True)
