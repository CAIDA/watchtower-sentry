from datetime import datetime
import utils
from json import dumps
import math

class GraphiteRecord(object):

    def __init__(self, target, start_time, end_time, step, raw_values,
                 default_nan_value=None, ignore_nan=False):
        self.target = target

        self.start_time = int(start_time)
        self.end_time = int(end_time)
        self.step = int(step)

        self.default_nan_value = default_nan_value
        self.ignore_nan = ignore_nan

        self.values = raw_values
        self._init_series()

    @classmethod
    def from_string(cls, metric_string, **kwargs):
        meta, data = metric_string.split('|')
        target, start_time, end_time, step = meta.rsplit(',', 3)
        raw_values = data.rsplit(',')
        return cls(target, start_time, end_time, step, raw_values, **kwargs)

    def __str__(self):
        content = [self.target, self.start_time, self.end_time, self.step]
        return '({})'.format(', '.join(map(str, content)))

    def __repr__(self):
        return type(x).__name__ + str(self)

    def _init_series(self):
        assert (self.end_time - self.start_time) / self.step == len(self.values), \
            'Time range and step are not aligned with values'

        self.series = []
        for value, time in zip(self.values, range(self.start_time, self.end_time, self.step)):
            try:
                if self.is_nan(value):
                    if not self.ignore_nan:
                        self.series.append((0.0, time))
                else:
                    self.series.append((float(value), time))
            except ValueError:
                pass

        self.no_data = not self.values
        self.empty = not self.series

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

        k = math.floor((len(vals) - 1) * rank)
        _, time = sorted_series[int(k)]

        return val, time

    def is_nan(self, value):
        """
        :param str value:
        """
        return self.default_nan_value == eval(value)
        # if self.default_nan_value is None:
        #     return value.lower() in ('none', 'null', 'nil', 'nan', 'undefined')
        # return dumps(self.default_nan_value) == value

    def extend(self, record):
        """Append that series to the end of this series.
        Part of that series before the end of this series is ignored.
        Both series must be aligned, i.e., steps of both records are equal and the difference of
        time of any two points from both series must be a multiple of step.
        """
        assert (self.step == record.step) and \
            ((self.end_time - record.start_time) % self.step == 0), 'Series are not aligned'

        if record.end_time <= self.end_time:
            return
        values, _, self.end_time = record._slice_values(self.end_time + self.step, record.end_time)
        self.values.extend(values)
        self._init_series()

    def slice(self, start_time, end_time):
        """Create a new record with values being a slice of this record's values."""
        values, start_time, end_time = self._slice_values(start_time, end_time)
        return GraphiteRecord(self.target,
                              start_time,
                              end_time,
                              self.step,
                              values,
                              self.default_nan_value,
                              self.ignore_nan)

    def _slice_values(self, start_time, end_time):
        """
        Values are filled with default_nan_value, if time is out of range.
        Invervals on boundries are included.
        :return [int], int, int: sliced values and actually sliced time range
        """
        assert start_time < end_time, 'Invalid time range to slice'
        # TODO do not fill when on boundry. include intervals on boundries.

        i_start = int(math.floor(float(start_time - self.start_time) / self.step))
        i_end = int(math.ceil(float(end_time - self.start_time) / self.step))

        values = [self.default_nan_value] * -i_start
        values.extend(self.values[max(i_start, 0):i_end])
        values.extend([self.default_nan_value] * (i_end - len(self.values)))

        start_time = i_start * self.step + self.start_time
        end_time = i_end * self.step + self.start_time

        return values, start_time, end_time
