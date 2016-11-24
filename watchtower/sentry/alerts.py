"""Implement alerts."""

from tornado import ioloop, httpclient as hc, gen, log, escape

from . import _compat as _
from .graphite import GraphiteRecord
from .utils import (
    HISTORICAL,
    LOGICAL_OPERATORS,
    convert_to_format,
    interval_to_graphite,
    parse_interval,
    parse_rule,
    format_time,
    percentile,
)
import math
from collections import deque, defaultdict
from itertools import islice
from datetime import datetime, timedelta
from calendar import timegm


LOGGER = log.gen_log
METHODS = "average", "last_value", "sum", "minimum", "maximum", "median", "percentile"
LEVELS = {
    'critical': 0,
    'warning': 10,
    'normal': 20,
}

AGGR_FUNCS = {
    'average': lambda vals: sum(vals) / len(vals),
    'last_value': lambda vals: vals[-1],
    'sum': sum,
    'minimum': min,
    'maximum': max,
    'median': lambda vals: percentile(sorted(vals), 0.5),
    'percentile': lambda vals, rank: percentile(sorted(vals), 0.5)
}


class sliceable_deque(deque):

    """Deque with slices support."""

    def __getitem__(self, index):
        """Support slices."""
        try:
            return deque.__getitem__(self, index)
        except TypeError:
            return type(self)(islice(self, index.start, index.stop, index.step))


class AlertFabric(type):

    """Register alert's classes and produce an alert by source."""

    alerts = {}

    def __new__(mcs, name, bases, params):
        """Register an Alert Class in self."""
        source = params.get('source')
        cls = super(AlertFabric, mcs).__new__(mcs, name, bases, params)
        if source:
            mcs.alerts[source] = cls
            LOGGER.info('Register Alert: %s', source)
        return cls

    def get(cls, reactor, source='charthouse', **options):
        """Get Alert Class by source."""
        acls = cls.alerts[source]
        return acls(reactor, **options)


class BaseAlert(_.with_metaclass(AlertFabric)):

    """Abstract basic alert class."""

    source = None

    def __init__(self, reactor, **options):
        """Initialize alert."""
        self.reactor = reactor
        self.options = options
        hc.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        self.client = hc.AsyncHTTPClient()

        try:
            self.configure(**options)
        except Exception as e:
            LOGGER.exception(e)
            raise ValueError("Invalid alert configuration: %s" % e)

        self.waiting = False
        self.state = {None: "normal", "waiting": "normal", "loading": "normal"}
        self.history = defaultdict(lambda: sliceable_deque([], self.history_size))

        LOGGER.info("Alert '%s': has inited", self)

    def __hash__(self):
        """Provide alert's hash."""
        return hash(self.name) ^ hash(self.source)

    def __eq__(self, other):
        """Check that other alert is the same."""
        return hash(self) == hash(other)

    def __str__(self):
        """String representation."""
        return "%s (%s)" % (self.name, self.interval)

    def configure(self, fqid=None, name=None, rules=None, queries=None, **options):
        """Configure the alert."""
        self.fqid = fqid
        self.name = name
        if not name or not fqid:
            raise AssertionError("Alert's name and FQID should be set.")

        if not rules:
            raise AssertionError("%s: Alert's rules is invalid" % name)
        self.rules = [parse_rule(rule) for rule in rules]
        self.rules = list(sorted(self.rules, key=lambda r: LEVELS.get(r.get('level'), 99)))

        assert queries and all(queries), "%s: Alert's queries are invalid" % self.name
        self.queries = queries
        self.current_query = None
        self.history_query = None

        self.interval = interval_to_graphite(
            options.get('interval', self.reactor.options['interval']))
        interval = parse_interval(self.interval)

        self.time_window = interval_to_graphite(
            options.get('time_window', options.get('interval', self.reactor.options['interval'])))

        self.until = interval_to_graphite(
            options.get('until', self.reactor.options['until'])
        )

        self._format = options.get('format', self.reactor.options['format'])
        self.request_timeout = options.get(
            'request_timeout', self.reactor.options['request_timeout'])
        self.connect_timeout = options.get(
            'connect_timeout', self.reactor.options['connect_timeout'])

        self.history_size = options.get('history_size', self.reactor.options['history_size'])
        self.history_size = parse_interval(self.history_size)
        self.history_size = int(math.ceil(self.history_size / interval))

        self.no_data = options.get('no_data', self.reactor.options['no_data'])
        self.loading_error = options.get('loading_error', self.reactor.options['loading_error'])

        self.history_method = \
            options.get('history_method',
                        self.reactor.options['history_method'])

        if self.reactor.options.get('debug'):
            self.callback = ioloop.PeriodicCallback(self.load, 5000)
        else:
            self.callback = ioloop.PeriodicCallback(self.load, interval)

        # For recording accurate time of last request
        self._set_absolute_time_range()

    def convert(self, value):
        """Convert self value."""
        return convert_to_format(value, self._format)

    def reset(self):
        """Reset state to normal for all targets.

        It will repeat notification if a metric is still failed.
        """
        for target in self.state:
            self.state[target] = "normal"

    def start(self):
        """Start checking."""
        self.callback.start()
        self.load()
        return self

    def stop(self):
        """Stop checking."""
        self.callback.stop()
        return self

    def check(self, current_records, history_records):
        """Check current value."""
        no_datas, normals = [], []
        violations = {}

        if len(current_records) != len(history_records):
            raise ValueError('Number of series in current and history queries '
                             'must match (%d != %d)'
                             % (len(current_records), len(history_records)))

        for current, history in zip(current_records, history_records):
            cval = self.get_record_value(current)
            hval = self.get_record_value(history)
            #LOGGER.debug("%s CURRENT [%s]: %s", self.name, current.target, cval)
            #LOGGER.debug("%s HISTORY [%s]: %s", self.name, history.target, hval)
            if cval is None:
                if current.no_data:  # in case all values are null yet there is data
                    no_datas.append((current, None, None))
            else:
                for rule in self.rules:
                    if self.evaluate_rule(rule, cval, current.target):
                        violations.setdefault(rule['level'], []).append((current, cval, rule))
                        if not self.options['ignore_alerted_history']:
                            if hval is not None:
                                self.history[current.target].append(hval)
                        break
                else:
                    normals.append((current, cval, None))
                    if hval is not None:
                        self.history[current.target].append(hval)

        self.notify_batch(self.no_data, no_datas)
        for level, data in violations.items():
            self.notify_batch(level, data)
        self.notify_batch('normal', normals)

    def evaluate_rule(self, rule, value, target):
        """Calculate the value."""
        def evaluate(expr):
            if expr in LOGICAL_OPERATORS.values():
                return expr
            rvalue = self.get_value_for_expr(expr, target)
            if rvalue is None:
                return False  # ignore this result
            return expr['op'](value, rvalue)

        evaluated = [evaluate(expr) for expr in rule['exprs']]
        while len(evaluated) > 1:
            lhs, logical_op, rhs = (evaluated.pop(0) for _ in range(3))
            evaluated.insert(0, logical_op(lhs, rhs))

        return evaluated[0]

    def get_history_val(self, target):
        history = self.history[target]
        if len(history) == 0:  # allow partial, but not empty, history
            return None
        method_tokens = self.history_method.split(' ', 1)
        if method_tokens[0] == 'percentile':
            return AGGR_FUNCS['percentile'](history, float(method_tokens[1]))
        else:
            return AGGR_FUNCS[self.history_method](history)

    def get_value_for_expr(self, expr, target):
        """I have no idea."""
        if expr in LOGICAL_OPERATORS.values():
            return None
        rvalue = expr['value']
        if rvalue == HISTORICAL:
            rvalue = self.get_history_val(target)
            #LOGGER.debug("%s HISTORY VAL: %s" % (target, rvalue))

        if rvalue is not None:
            rvalue = expr['mod'](rvalue)
        return rvalue

    def notify(self, level, value, target=None, ntype=None, rule=None):
        """Notify main reactor about event."""
        if self.check_state(level, target):
            return self.reactor.notify(level, self, value, target=target, ntype=ntype, rule=rule)

    def notify_batch(self, level, data):
        data = list(filter(lambda d: self.check_state(level, d[0].target), data))

        # Is there any entries in it?
        if not data:
            return False

        return self.reactor.notify_batch(level, self, data)

    def check_state(self, level, target):
        # Did we see the event before?
        if target in self.state and level == self.state[target]:
            return False

        # Do we see the event first time?
        if target not in self.state and level == 'normal' \
                and not self.reactor.options['send_initial']:
            return False

        self.state[target] = level
        return True

    def format_time_with_offset(self):
        return format_time(self.get_current_query_time())

    def _set_absolute_time_range(self):
        self.current_now = timegm(datetime.utcnow().timetuple())
        self.current_from = int(self.current_now - parse_interval(self.time_window) / 1e3)
        self.current_until = int(max(self.current_now,
                                     self.current_now - parse_interval(self.until) / 1e3))

    def get_current_query_time(self):
        """
        :return datetime:
        """
        return datetime.utcfromtimestamp(self.current_from)

    def load(self):
        """Load from remote."""
        raise NotImplementedError()


class GraphiteAlert(BaseAlert):

    """Check graphite records."""

    source = 'graphite'

    def configure(self, **options):
        """Configure the alert."""
        super(GraphiteAlert, self).configure(**options)

        self.default_nan_value = options.get(
            'default_nan_value', self.reactor.options['default_nan_value'])
        self.ignore_nan = options.get('ignore_nan', self.reactor.options['ignore_nan'])

        self.method = options.get('method', self.reactor.options['method'])
        method_tokens = self.method.split(' ', 1)
        self.method_name = method_tokens[0]
        self.method_params = method_tokens[1:]
        try:
            assert self.method_name in METHODS, 'unknown method'
            if self.method_name == 'percentile':
                assert len(self.method_params) == 1, 'requires one parameter'
                try:
                    self.method_params = [float(self.method_params[0])]
                except ValueError:
                    raise ValueError('rank is not a float')
                assert 0 <= rank <= 100, 'rank must be in the range [0,100]'
            else:
                assert not self.method_params, 'does not accept parameters'
        except Exception as e:
            raise ValueError("Invalid method '{}': {}".format(self.method, e))

        self.auth_username = self.reactor.options.get('auth_username')
        self.auth_password = self.reactor.options.get('auth_password')

        self.urls = []
        queries = self.queries
        self.queries = []
        for query in queries:
            if isinstance(query, basestring):
                query = {
                    'current': query,
                    'history': query,
                }
            self.queries.append(query)
            self.urls.append(self._graphite_urls(
                query, graphite_url=self.reactor.options.get('graphite_url'),
                raw_data=True))

        LOGGER.debug('%s: queries = %s', self.name, self.queries)
        LOGGER.debug('%s: urls = %s', self.name, self.urls)

    @gen.coroutine
    def load(self):
        """Load data from Graphite."""
        if self.waiting:
            self.notify('warning', 'Process takes too much time', target='waiting', ntype='common')
        else:
            self.waiting = True

            for query, url in zip(self.queries, self.urls):
                self.current_query = query['current']
                self.history_query = query['history']
                self.current_url = url['current']
                LOGGER.debug('%s: start checking %s from %s to %s. Current url: "%s". History url: "%s"',
                    self.name, self.current_query, self.current_from, self.current_until,
                    self.current_url, url['history'])
                try:
                    current_records = yield self._fetch_records(url['current'])

                    if query['current'] == query['history']:
                        history_records = current_records
                    else:
                        history_records = yield self._fetch_records(url['history'], history=True)

                    LOGGER.debug('%s recieved %s records', self.name, len(current_records) + len(history_records))
                    if len(current_records) == 0 or len(history_records) == 0:
                        self.notify(self.loading_error,
                                    'Loading error: Server returned an empty response',
                                    target='loading {}'.format(query),
                                    ntype='emptyresp')
                    else:
                        self.check(current_records, history_records)
                        self.notify('normal',
                                    'Metrics are loaded',
                                    target='loading {}'.format(query),
                                    ntype='common')
                except hc.HTTPError as e:
                    LOGGER.exception(e)
                    resp = e.response
                    if resp:
                        LOGGER.exception(resp.body)
                    self.notify(self.loading_error,
                                'Loading error: %s' % e,
                                target='loading {}'.format(query),
                                ntype='common')
                except Exception as e:
                    LOGGER.exception(e)
                    self.notify(self.loading_error,
                                'Loading error: %s' % e,
                                target='loading {}'.format(query),
                                ntype='common')

                yield self.relax()

            self.waiting = False

    def get_graph_url(self, target, graphite_url=None):
        """Get Graphite URL."""
        return self._graphite_url(target, graphite_url=graphite_url, raw_data=False)

    def _graphite_url(self, query, raw_data=False, graphite_url=None):
        query = escape.url_escape(query)
        graphite_url = graphite_url or self.reactor.options.get('public_graphite_url')
        url = "{base}/render/?target={query}&from={_from}&until={until}".format(
            base=graphite_url, query=query, _from=self.current_from, until=self.current_until)
        if raw_data:
            url = "{0}&rawData=true".format(url)
        return url

    def _graphite_urls(self, query, raw_data=False, graphite_url=None):
        """Build Graphite URLs (current + history)."""
        urls = {
            'current': self._graphite_url(query['current'],
                                          raw_data, graphite_url),
            'history': self._graphite_url(query['history'],
                                          raw_data, graphite_url)
        }
        return urls

    def get_record_value(self, record):
        if record.empty:
            return None
        val, _ = getattr(record, self.method_name)(*self.method_params)
        return val

    def get_record_time(self, record):
        if record.empty:
            return record.get_start_time()
        _, time = getattr(record, self.method_name)(*self.method_params)
        return datetime.utcfromtimestamp(time)

    @gen.coroutine
    def _fetch_records(self, request, **kwargs):
        response = yield self.client.fetch(request,
                                           auth_username=self.auth_username,
                                           auth_password=self.auth_password,
                                           request_timeout=self.request_timeout,
                                           connect_timeout=self.connect_timeout,
                                           **kwargs)
        records = [GraphiteRecord.from_string(line.decode('utf-8'),
                                              default_nan_value=self.default_nan_value,
                                              ignore_nan=self.ignore_nan)
                   for line in response.buffer]
        raise gen.Return(records)


class CharthouseAlert(GraphiteAlert):

    source = 'charthouse'

    def get_graph_url(self, target=None, charthouse_url=None):
        """Get Charthouse URL."""
        return self._charthouse_url(target, charthouse_url=charthouse_url)

    def _charthouse_url(self, query=None, charthouse_url=None):
        """Build Charthouse URL."""
        query = escape.url_escape(query or self.current_query)
        charthouse_url = charthouse_url or self.reactor.options.get('charthouse_url')

        # Show a span of extra 12 hours around the window, centered
        # Make width of span configurable?
        # Must use timestamp with from & until instead of relative time in emails
        default_window = timedelta(hours=6).total_seconds()
        _from = max(0, self.current_from - default_window)
        until = max(self.current_now, self.current_until + default_window)

        url = "{base}/explorer#expression={query}&from={_from}&until={until}".format(
            base=charthouse_url, query=query, _from=_from, until=until)
        return url
