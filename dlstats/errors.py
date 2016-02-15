
class DlstatsException(Exception):
    
    def __init__(self, *args, **kwargs):
        self.provider_name = kwargs.pop("provider_name", None)
        self.dataset_code = kwargs.pop("dataset_code", None)
        self.comments = kwargs.pop("comments", None)
        super().__init__(*args, **kwargs)

class RejectUpdatedDataset(DlstatsException):
    """Reject if dataset is updated
    """

class LockedDataset(DlstatsException):
    """if lock=True
    """
    
class MaxErrors(DlstatsException):
    pass

class InterruptProcessSeriesData(DlstatsException):
    pass

class SeriesException(DlstatsException):

    def __init__(self, *args, **kwargs):
        self.bson = kwargs.pop("bson", None)
        super().__init__(*args, **kwargs)

class RejectFrequency(SeriesException):

    def __init__(self, *args, **kwargs):
        self.frequency = kwargs.pop("frequency", None)
        super().__init__(*args, **kwargs)

class RejectInvalidSeries(SeriesException):
    pass

class RejectEmptySeries(SeriesException):
    pass

class RejectUpdatedSeries(SeriesException):
    """Reject if series is updated
    """

    def __init__(self, *args, **kwargs):
        self.bson = kwargs.pop("bson", None)
        self.key = kwargs.pop("key", None)
        super().__init__(*args, **kwargs)

