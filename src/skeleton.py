import pymongo

class Skeleton(object):
    def __init__(self):
		self.client = pymongo.MongoClient()
    def update_categories_db(self):
        """Update the categories in MongoDB
        """
        raise NotImplementedError("All the methods from the Skeleton class must"
                                  "be implemented.")
    def update_series_db(self):
        """Update the series in MongoDB
        """
        raise NotImplementedError("All the methods from the Skeleton class must"
                                  "be implemented.")
