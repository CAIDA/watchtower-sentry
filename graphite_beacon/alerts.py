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
)
import math
from collections import deque, defaultdict
from itertools import islice
from datetime import datetime, timedelta


LOGGER = log.gen_log
METHODS = "average", "last_value", "sum", "minimum", "maximum", "median", "percentile"
LEVELS = {
    'critical': 0,
    'warning': 10,
    'normal': 20,
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
        """Check that other alert iis the same."""
        return hash(self) == hash(other)

    def __str__(self):
        """String representation."""
        return "%s (%s)" % (self.name, self.interval)

    def configure(self, name=None, rules=None, query=None, **options):
        """Configure the alert."""
        self.name = name
        if not name:
            raise AssertionError("Alert's name should be defined and not empty.")

        if not rules:
            raise AssertionError("%s: Alert's rules is invalid" % name)
        self.rules = [parse_rule(rule) for rule in rules]
        self.rules = list(sorted(self.rules, key=lambda r: LEVELS.get(r.get('level'), 99)))

        assert query, "%s: Alert's query is invalid" % self.name
        self.query = query

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

        if self.reactor.options.get('debug'):
            self.callback = ioloop.PeriodicCallback(self.load, 5000)
        else:
            self.callback = ioloop.PeriodicCallback(self.load, interval)

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

    def check(self, records):
        """Check current value."""
        for value, target in records:
            LOGGER.info("%s [%s]: %s", self.name, target, value)
            if value is None:
                self.notify(self.no_data, value, target)
                continue
            for rule in self.rules:
                if self.evaluate_rule(rule, value, target):
                    self.notify(rule['level'], value, target, rule=rule)
                    if not self.options['ignore_alerted_history']:
                        self.history[target].append(value)
                    break
            else:
                self.notify('normal', value, target, rule=rule)
                self.history[target].append(value)

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

    def get_value_for_expr(self, expr, target):
        """I have no idea."""
        if expr in LOGICAL_OPERATORS.values():
            return None
        rvalue = expr['value']
        if rvalue == HISTORICAL:
            history = self.history[target]
            if len(history) < self.history_size:
                return None
            rvalue = sum(history) / float(len(history))

        rvalue = expr['mod'](rvalue)
        return rvalue

    def notify(self, level, value, target=None, ntype=None, rule=None):
        """Notify main reactor about event."""
        # Did we see the event before?
        if target in self.state and level == self.state[target]:
            return False

        # Do we see the event first time?
        if target not in self.state and level == 'normal' \
                and not self.reactor.options['send_initial']:
            return False

        self.state[target] = level
        return self.reactor.notify(level, self, value, target=target, ntype=ntype, rule=rule)

    def load(self):
        """Load from remote."""
        raise NotImplementedError()


class GraphiteAlert(BaseAlert):

    """Check graphite records."""

    source = 'graphite'

    def configure(self, **options):
        """Configure the alert."""
        super(GraphiteAlert, self).configure(**options)

        self.method = options.get('method', self.reactor.options['method'])
        self.default_nan_value = options.get(
            'default_nan_value', self.reactor.options['default_nan_value'])
        self.ignore_nan = options.get('ignore_nan', self.reactor.options['ignore_nan'])
        method_tokens = self.method.split(' ', 1)
        assert method_tokens[0] in METHODS, "Method is invalid"
        if method_tokens[0] == 'percentile':
            try:
                rank = float(method_tokens[1])
            except ValueError:
                raise ValueError('Invalid percentile: %s' % method_tokens[1])
            assert 0 <= rank <= 100, 'Percentile must be in the range [0,100]'

        self.auth_username = self.reactor.options.get('auth_username')
        self.auth_password = self.reactor.options.get('auth_password')

        self.url = self._graphite_url(
            self.query, graphite_url=self.reactor.options.get('graphite_url'), raw_data=True)
        LOGGER.debug('%s: url = %s', self.name, self.url)

    @gen.coroutine
    def load(self):
        """Load data from Graphite."""
        LOGGER.debug('%s: start checking: %s', self.name, self.query)
        if self.waiting:
            self.notify('warning', 'Process takes too much time', target='waiting', ntype='common')
        else:
            self.waiting = True
            try:
                response = yield self.client.fetch(self.url, auth_username=self.auth_username,
                                                   auth_password=self.auth_password,
                                                   request_timeout=self.request_timeout,
                                                   connect_timeout=self.connect_timeout)
                records = (
                    GraphiteRecord(line.decode('utf-8'), self.default_nan_value, self.ignore_nan)
                    for line in response.buffer)
                data = [
                    (None if record.empty else self._get_record_attr(record), record.target)
                    for record in records]
                if len(data) == 0:
                    self.notify(self.loading_error,
                                'Loading error: Server returned an empty response',
                                target='loading',
                                ntype='emptyresp')
                else:
                    self.check(data)
                    self.notify('normal', 'Metrics are loaded', target='loading', ntype='common')
            except Exception as e:
                self.notify(
                    self.loading_error, 'Loading error: %s' % e, target='loading', ntype='common')
            self.waiting = False

    def get_graph_url(self, target, graphite_url=None):
        """Get Graphite URL."""
        return self._graphite_url(target, graphite_url=graphite_url, raw_data=False)

    def _graphite_url(self, query, raw_data=False, graphite_url=None):
        """Build Graphite URL."""
        query = escape.url_escape(query)
        graphite_url = graphite_url or self.reactor.options.get('public_graphite_url')

        url = "{base}/render/?target={query}&from=-{time_window}&until=-{until}".format(
            base=graphite_url, query=query, time_window=self.time_window, until=self.until)
        if raw_data:
            url = "{0}&rawData=true".format(url)
        return url

    def _get_record_attr(self, record):
        method_tokens = self.method.split(' ', 1)
        if method_tokens[0] == 'percentile':
            return record.percentile(float(method_tokens[1]))
        else:
            return getattr(record, self.method)


class CharthouseAlert(GraphiteAlert):

    source = 'charthouse'

    def get_graph_url(self, target=None, charthouse_url=None):
        """Get Charthouse URL."""
        return self._charthouse_url(target, charthouse_url=charthouse_url)

    def _charthouse_url(self, query=None, charthouse_url=None):
        """Build Charthouse URL."""
        query = escape.url_escape(query or self.query)
        charthouse_url = charthouse_url or self.reactor.options.get('charthouse_url')
        
        # Show a span of extra 12 hours around the window, centered
        # Must use timestamp with from & until instead of relative time in emails
        now = datetime.now().timestamp()
        def_window = timedelta(hours=6).total_seconds()
        since = int(now - parse_interval(self.time_window) - def_window)
        until = int(now - parse_interval(self.until) + def_window)

        url = "{base}/explorer#expression={query}&from={since}&until={until}".format(
            base=charthouse_url, query=query, since=since, until=until)
        return url


class URLAlert(BaseAlert):

    """Check URLs."""

    source = 'url'

    @staticmethod
    def get_data(response):
        """Value is response.status."""
        return response.code

    @gen.coroutine
    def load(self):
        """Load URL."""
        LOGGER.debug('%s: start checking: %s', self.name, self.query)
        if self.waiting:
            self.notify('warning', 'Process takes too much time', target='waiting', ntype='common')
        else:
            self.waiting = True
            try:
                response = yield self.client.fetch(
                    self.query, method=self.options.get('method', 'GET'),
                    request_timeout=self.request_timeout,
                    connect_timeout=self.connect_timeout,
                    validate_cert=self.options.get('validate_cert', True))
                self.check([(self.get_data(response), self.query)])
                self.notify('normal', 'Metrics are loaded', target='loading', ntype='common')

            except Exception as e:
                self.notify('critical', str(e), target='loading', ntype='common')

            self.waiting = False
