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

    @gen.coroutine
    def _fetch_records(self, request, **kwargs):
        """Prefetch records for performance.
        Notice that records fetched from cache are not necessarily the same as
        those directly fetched from the server in some insignificant ways. For example,
        with the same request, this method may return records with all values being None,
        while the server could not return such records at all.
        """
        # Find cache for this request by its params
        graphite_url, expression, since, until, raw_data = self._parse_request(request)
        cached_since, cached_until, cache = self.records_cache[expression]
        assert since >= cached_since, 'Time is going backwards!'

        # Update cache if needed
        if until > cached_until:
            next_since = cached_until
            next_until = max(until, cached_until + self.prefetch_size)
            next_cache = {}

            # Discard old data
            for record_name, record in cache.items():
                trimmed_record = record.slice(cached_since, cached_until)
                if not trimmed_record.no_data:
                    next_cache[record_name] = trimmed_record

            # Fetch data
            next_url = self._graphite_url(query=expression,
                                          raw_data=raw_data,
                                          graphite_url=graphite_url,
                                          since=next_since,
                                          until=next_until)
            next_records = yield super(CharthouseScanner, self)._fetch_records(next_url, **kwargs)

            # Store new data
            for record in next_records:
                if record.target in next_cache:
                    next_cache[record.target].extend(record)
                else:
                    next_cache[record.target] = record

            # Update states
            self.records_cache[expression] = next_since, next_until, next_cache

        # Return data from cache
        return [r.slice(since, until) for r in self.records_cache.values()]

    @staticmethod
    def _parse_request(request):
        url = request.url if isinstance(request, hc.HTTPRequest) else request
        o = urlparse(url)
        graphite_url = o.netloc
        params = parse_qs(o)
        expression = escape.url_unescape(params['target'])
        since = int(params['since'])
        until = int(params['until'])
        raw_data = params.get('rawData') in ('', 'true')
        return graphite_url, expression, since, until, raw_data
