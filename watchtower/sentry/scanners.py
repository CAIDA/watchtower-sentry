"""Implement scanners."""

from tornado import httpclient as hc, gen, log, escape

from .alerts import CharthouseAlert, sliceable_deque
from .utils import parse_interval
from collections import deque, defaultdict
from datetime import datetime
from urlparse import urlparse, parse_qs


LOGGER = log.gen_log


class CharthouseScanner(CharthouseAlert):

    source = 'scanner'

    def configure(self, **options):
        super(CharthouseScanner, self).configure(**options)

        self.scan_from, self.scan_until = map(options.get, ('scan_from', 'scan_until'))
        assert self.scan_from and self.scan_until and (0 <= self.scan_from < self.scan_until), \
            'Invalid scanning start and end time'

        try:
            self.scan_step, self.scan_span = (parse_interval(options.get(s)) / 1e3 for s in
                ('scan_step', 'scan_span'))
        except Exception as e:
            LOGGER.exception(e)
            raise AssertionError('Invalid scanning step or span')
        assert self.scan_step > 0 and self.scan_span > 0, 'Invalid scanning span or step'

        self.prefetch_size = parse_interval(
            options.get('prefetch_size', self.reactor.options['prefetch_size'])) / 1e3
        # {target: (since, until, {record_name: record})}
        self.records_cache = defaultdict(lambda: (-1, -1, {}))

    def start(self):
        raise RuntimeError('Scanner does not have periodic callbacks')

    @gen.coroutine
    def load(self):
        graphite_url = self.reactor.options.get('graphite_url')
        steps = int((self.scan_until - 1 - self.scan_from) / self.scan_step)

        for i in range(steps):
            t = self.scan_from + i * self.scan_step - 1
            self.time_window, self.until = max(0, int(t - self.scan_span)), int(t)
            self.urls = [self._graphite_urls(
                query, graphite_url=graphite_url) for query in self.queries]
            LOGGER.debug('%s: scanning from %s to %s', self.name,
                         *map(datetime.utcfromtimestamp, (self.time_window, self.until)))

            yield super(CharthouseScanner, self).load()

        raise gen.Return(True)

    def _graphite_url(self, query, raw_data=False, graphite_url=None, since=None, until=None):
        return "{base}/render/?target={query}&from={since}&until={until}&rawData=true".format(
            base=graphite_url,
            query=escape.url_escape(query),
            since=since or self.time_window,
            until=until or self.until)
