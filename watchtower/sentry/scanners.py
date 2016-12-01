"""Implement scanners."""

from tornado import httpclient as hc, gen, log, escape

from .alerts import CharthouseAlert, get_utcnow_ts
from .utils import parse_interval
from collections import defaultdict
from datetime import datetime
from urlparse import urlparse, parse_qs
from calendar import timegm


LOGGER = log.gen_log


class CharthouseScanner(CharthouseAlert):

    source = 'scanner'

    def configure(self, **options):
        try:
            self.scan_from, self.scan_until = (int(options.get(s, self.reactor.options[s]))
                for s in ('scan_from', 'scan_until'))
            assert self.scan_from and self.scan_until and \
                (0 <= self.scan_from < self.scan_until), 'invalid scanning start and end time'

            self.scan_step, self.scan_span = (int(parse_interval(options.get(
                s, self.reactor.options[s])) / 1e3) for s in ('scan_step', 'scan_span'))
            assert self.scan_step > 0 and self.scan_span > 0, 'invalid scanning span or step'

        except Exception as e:
            LOGGER.exception(e)
            raise ValueError('Invalid options: {}'.format(e))

        self.scan_idx = 0

        super(CharthouseScanner, self).configure(**options)

        self.prefetch_size = int(parse_interval(
            options.get('prefetch_size', self.reactor.options['prefetch_size'])) / 1e3)

        # increase timeout since we will query in bulk
        self.request_timeout = self.request_timeout * (self.prefetch_size / self.scan_step)

        init_scan_until = self.current_from - self.scan_step
        # {target: (from, until, {record_name: record})}
        self.records_cache = defaultdict(lambda: (-1, init_scan_until, []))

        # Options controlling how long a scanner can run consecutively before yielding to others
        self.busy_timeout = int(parse_interval(
            options.get('busy_timeout', self.reactor.options['busy_timeout'])) / 1e3)
        self.last_relaxed = get_utcnow_ts()

    def start(self):
        raise RuntimeError('Scanner does not have periodic callbacks')

    @gen.coroutine
    def load(self):
        graphite_url = self.reactor.options.get('graphite_url')
        steps = int((self.scan_until - 1 - self.scan_from) / self.scan_step)

        for self.scan_idx in range(steps):
            self._set_absolute_time_range()
            self.urls = [self._graphite_urls(
                query, graphite_url=graphite_url, raw_data=True) for query in self.queries]
            yield super(CharthouseScanner, self).load()

        raise gen.Return(True)

    def _set_absolute_time_range(self):
        t = self.scan_from + self.scan_idx * self.scan_step
        self.current_from = max(0, int(t - self.scan_span))
        self.current_until = int(t)
        self.current_now = get_utcnow_ts()

    @gen.coroutine
    def _fetch_records(self, request, history=False, **kwargs):
        """Prefetch records for performance.
        Notice that records fetched from cache are not necessarily the same as
        those directly fetched from the server in some insignificant ways. For example,
        with the same request, this method may return records with all values being None,
        while the server could not return such records at all.
        """
        # Find cache for this request by its params
        graphite_url, expression, raw_data = self._parse_request(request)
        request_target = ':'.join([self.name, expression, 'history' if history else 'current'])
        cached_from, cached_until, cache = self.records_cache[request_target]
        assert self.current_from >= cached_from, 'Time is going backwards!'

        LOGGER.debug('Getting data for %s (%s to %s) from cache' %
                     (expression, self.current_from, self.current_until))

        # Update cache if needed
        if self.current_until > cached_until:
            # next_from = cached_until
            next_from = cached_until + self.scan_step  # assume scan_step == step of GraphiteRecords
            next_until = min(max(self.current_until, next_from + self.prefetch_size), self.scan_until)
            #next_cache = []
            LOGGER.debug('Data not found in cache (current is %s to %s). Fetching %s from %s to %s',
                         cached_from, cached_until,
                         'history' if history else 'current',
                         next_from, next_until)

            # Discard old data
            #for record_name, record in cache:
            #    trimmed_record = record.slice(cached_from, cached_until)
            #    if not trimmed_record.no_data:
            #        next_cache[record_name] = trimmed_record

            # Fetch data
            last_from = self.current_from  # hax time range
            last_until = self.current_until
            self.current_from = next_from
            self.current_until = next_until
            next_url = self._graphite_url(
                query=expression, raw_data=raw_data, graphite_url=graphite_url)
            self.current_from = last_from  # recover time range
            self.current_until = last_until
            next_records = yield super(CharthouseScanner, self)._fetch_records(next_url, **kwargs)

            # Store new data
            #for record in next_records:
            #    if record.target in next_cache:
            #        next_cache[record.target].extend(record)
            #    else:
            #        next_cache[record.target] = record

            # Update states
            self.records_cache[request_target] = next_from, next_until, next_records
            cached_from, cached_until, cache = self.records_cache[request_target]

            relaxed = True

        else:
            relaxed = False

        # Give other scanners a chance to load data
        yield self.relax(relaxed)

        # Return data from cache
        records = [r.slice(self.current_from, self.current_until) for r in cache]
        raise gen.Return(records)

    @staticmethod
    def _parse_request(request):
        url = request.url if isinstance(request, hc.HTTPRequest) else request
        o = urlparse(url)
        graphite_url = o.netloc
        params = parse_qs(o.query)
        assert all(len(vs) == 1 for vs in map(params.get, ('target', 'rawData')) if vs), \
            'Exist duplicate keys in query of request'
        expression = escape.url_unescape(params['target'][0])
        raw_data = params.get('rawData', [None])[0] in ('', 'true')
        return graphite_url, expression, raw_data

    @gen.coroutine
    def relax(self, relaxed):
        if len(self.reactor.alerts) <= 1:
            return

        if relaxed:
            self.last_relaxed = get_utcnow_ts()
            return

        busy_time = get_utcnow_ts() - self.last_relaxed
        if busy_time > self.busy_timeout:
            LOGGER.debug('Scanner %s fall asleep after being busy for too long', self.name)
            yield gen.moment
            LOGGER.debug('Scanner %s is awaken', self.name)
            self.last_relaxed = get_utcnow_ts()
