#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import random
import unittest
import eurostat
import pymongo
import types
import lxml.etree
import urllib

class TestEurostat(unittest.TestCase):
    """Test Eurostat"""

    def setUp(self):
        self.toto=eurostat.Eurostat()
        webpage = urllib.request.urlopen("file:///home/versiontest/27jan/dlstats/src/fetchers/table_of_contents.xml","r")
        table_of_contents = webpage.read()
        self.toto.table_of_contents = lxml.etree.fromstring(table_of_contents)
        webpage.close()
        self.toto.db = self.toto.client.t1
        self.toto.update_categories_db()

    def test_adummy(self):
        datatstat=self.toto.db.command("dbstats")
        for key, value in datatstat.items():
            print( key, value)
            
    def test_allelement(self):
        datatest=list(self.toto.db.categories.find())
        data = []
        with open("mjson.json") as f:
            data =[json.loads(line) for line in f]
        dataname=[]
        dataname=[r.get("name", "Missing: name") for r in data]
        rootid=self.toto.db.categories.find_one({"name": "Database by themes"})["_id"]
        rootchildid=self.toto.db.categories.find_one({"_id": rootid})
        testrootid=[r.get("_id", "Missing: _id") for r in data if r.get("name", "Missing: name")=='Database by themes']
        print('testrootid',testrootid)
        def testtree(idparent,testidparent):
            parentname=self.toto.db.categories.find_one({"_id": idparent})["name"]
            testparentname=[r.get("name", "Missing: name") for r in data if r.get("_id", "Missing: _id")==testidparent[0]][0]
            self.assertTrue(testparentname==parentname)
            try:
                childid=self.toto.db.categories.find_one({"_id": idparent})["children"]
                testchildid=[r.get("children", "Missing: children")[0] for r in data if r.get("_id", "Missing: _id")==testidparent[0]]
                print('testchildid:',testchildid)     
                for children in childid:
                    testtree(children,testchildid)
            except Exception:
                pass
        
        testtree(rootid,testrootid)            

    def test_Nbelement(self):
        mongo_NBm=self.toto.db.categories.find().count()    
        print('Nb of categories after update_categories: ', mongo_NBm)
        self.assertEqual(mongo_NBm, 7736)

    def test_firstelement(self):
        firstelement=self.toto.db.categories.find_one({"name": "Consumers - monthly data"})["name"]
        self.assertTrue(firstelement)

    def tearDown(self):
        self.toto.db = self.toto.client.t1  
        self.toto.db.command("dropDatabase")
        mongo_NBm=self.toto.db.categories.find().count()    
        print('Nb of categories after teardown: ', mongo_NBm)

if __name__ == '__main__':
    unittest.main()
