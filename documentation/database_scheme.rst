Database scheme
===============

journal = {name : str}
categories = {name : str, children : MongoID, url : str, id_journal: MongoID, id_series : MongoID}
series = {id_values = [MongoID], name = str}
values = {values : [int], dates : [int], id_journal : MongoID}
