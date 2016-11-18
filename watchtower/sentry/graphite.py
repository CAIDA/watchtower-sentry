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
        self.no_data = len(raw_values) == 0
        self.series = self._make_series(raw_values)
        self.empty = len(self.series) == 0

    def __str__(self):
        content = [self.target, self.start_time, self.end_time, self.step]
        return '({})'.format(', '.join(map(str, content)))

    def __repr__(self):
        return 'GraphiteRecord' + str(self)

    def _make_series(self, values):
        series = []
        for value, time in zip(values, range(self.start_time, self.end_time, self.step)):
            try:
                if self.is_nan(value):
                    if not self.ignore_nan:
                        series.append((0.0, time))
                else:
                    series.append((float(value), time))
            except ValueError:
                continue
        return series

    def get_end_time(self):
        return datetime.utcfromtimestamp(self.end_time)

    def get_start_time(self):
        return datetime.utcfromtimestamp(self.start_time)

    def average(self):
        sum_pts, time = self.sum()
        return sum_pts / len(self.series), time

    def last_value(self):
        return self.series[-1]

    def sum(self):
        vals, _ = zip(*self.series)
        return sum(vals), self.start_time

    def minimum(self):
        return min(self.series)

    def maximum(self):
        return max(self.series)

    def median(self):
        return self.percentile(50)

    def percentile(self, rank):
        """
        :param rank: Should be in [0, 100].
        """
        rank /= 100.0
        sorted_series = sorted(self.series)

        vals, _ = zip(*sorted_series)
        val = utils.percentile(vals, rank)

        k = int((len(vals) - 1) * rank)
        _, time = sorted_series[k]

        return val, time

    def is_nan(self, value):
        """
        :param str value:
        """
        if self.default_nan_value is None:
            return value.lower() in ('null', 'none', 'nil', 'nan', 'undefined')
        return dumps(self.default_nan_value) == value
