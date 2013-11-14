import pymongo

class Skeleton(object):
    def __init__(self):
        try:
            self.client = pymongo.MongoClient()
        except:
            raise Exception("Please launch the mongodb service")
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
