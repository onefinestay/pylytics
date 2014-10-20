import datetime


class DimensionSelector(object):
    """ Returns a datetime value which is used to select the correct dimension
    row.

    Dimensions can evolve over time, so multiple dimensions can match a given 
    natural key. The differentiator is the time the rows were created. The
    selector allows us to select the dimension row which was valid at a certain
    point of time.

    """

    def __init__(self, date=None, time=None):
        """ By default the most recent matching dimension is returned."""
        self.date = date
        self.time = time

    def timestamp(self, instance):
        """
        Args:
            instance - a Fact instance.
        """        
        if not self.date and not self.time:
            return datetime.datetime.now()
        elif self.date and not self.time:
            date = instance[self.date]
            return datetime.datetime.combine(date, datetime.datetime.min.time())
        elif self.date and self.time:
            date = instance[self.date]
            time = instance[self.time]
            return datetime.datetime.combine(date, time)
