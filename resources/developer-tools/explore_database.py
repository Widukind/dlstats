#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""Populate the namespace for easier exploration of the database"""
import pandas
import pymongo
import pysdmx
import pprint


client = pymongo.MongoClient()


def pprint_categories(db):
    for category in db.categories.find():
        pprint.pprint(category)


def pprint_series(db):
    for series in db.series.find():
        pprint.pprint(series)
