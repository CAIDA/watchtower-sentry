from datetime import datetime
import utils
from json import dumps

class GraphiteRecord(object):

    def __init__(self, metric_string, default_nan_value=None, ignore_nan=False):
        meta, data = metric_string.split('|')
        self.target, start_time, end_time, step = meta.rsplit(',', 3)
        self.start_time = int(start_time)
        self.end_time = int(end_time)
        self.step = int(step)
        self.default_nan_value = default_nan_value
        self.ignore_nan = ignore_nan
        raw_values = data.rsplit(',')
        self.values = list(self._values(raw_values))
        self.empty = len(values) == 0
        self.no_data = len(raw_values) == 0

    def _values(self, values):
        for value in values:
            try:
                if self.is_nan(value):
                    if not self.ignore_nan:
                        yield 0.0
                else:
                    yield float(value)
            except ValueError:
                continue

    def get_end_time(self):
        return datetime.utcfromtimestamp(self.end_time)

    @property
    def average(self):
        return self.sum / len(self.values)

    @property
    def last_value(self):
        return self.values[-1]

    @property
    def sum(self):
        return sum(self.values)

    @property
    def minimum(self):
        return min(self.values)

    @property
    def maximum(self):
        return max(self.values)

    @property
    def median(self):
        return self.percentile(50)

    def percentile(self, rank):
        return utils.percentile(sorted(self.values), rank/100.0)

    def is_nan(self, value):
        """
        :param str value:
        """
        if self.default_nan_value is None:
            return value.lower() in ('null', 'none', 'nil', 'nan', 'undefined')
        return dumps(self.default_nan_value) == value:
