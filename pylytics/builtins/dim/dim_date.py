import datetime
from datetime import date

from pylytics.library.dim import Dim


START_DATE = date(2000, 01, 01)
END_DATE = datetime.datetime.now().date() + datetime.timedelta(
    weeks=104) # Approx 2 years.


class DimDate(Dim):

    def update(self):
        """
        Updates the dim_date table with all the dates from START_DATE to
        END_DATE.

        """
        # Status.
        msg = "Populating {0}".format(self.table_name)
        self._print_status(msg)

        # Get the last inserted date
        cur_date = self.connection.execute(
            "SELECT MAX(`date`) FROM `{}`".format(self.table_name))[0][0]
        
        if cur_date == None:
            # Build history.
            cur_date = START_DATE

        today = date.today()
        while cur_date <= END_DATE:
            quarter = (cur_date.month - 1)/3 + 1
            date_field = (
                cur_date.isoformat(),
                cur_date.day,
                cur_date.strftime("%a"), # Mon
                int(cur_date.strftime("%u")), # ISO day of week (numeric)
                int(cur_date.strftime("%U")), # ISO week number
                cur_date.strftime("%Y-%U"), # Year and week number (full week)
                int(cur_date.strftime("%m")), # Month number
                cur_date.strftime("%b"), # Month name
                cur_date.strftime("%Y-%m"), # Year and month (full month)
                quarter, # Quarter e.g. 1,2,3,4
                "Q{}".format(quarter), # Q1
                '{0}-{1}'.format(cur_date.year, quarter), # Year and quarter
                cur_date.year,
            )

            self.connection.execute(
                "INSERT IGNORE INTO `{0}` VALUES (NULL, {1}, NULL)".format(
                                    self.table_name,
                                    self._values_placeholder(len(date_field))),
                date_field,
                )

            cur_date = cur_date + datetime.timedelta(days=1)
