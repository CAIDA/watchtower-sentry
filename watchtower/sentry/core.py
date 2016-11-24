import os
from re import compile as re, M

import json
import logging
from tornado import ioloop, log, gen

from .alerts import BaseAlert
from .utils import parse_interval
from .handlers import registry
import scanners

try:
    import yaml
except ImportError:
    yaml = None


LOGGER = log.gen_log

COMMENT_RE = re('//\s+.*$', M)


class Reactor(object):

    """ Class description. """

    defaults = {
        'auth_password': None,
        'auth_username': None,
        'config': 'config.json',
        'critical_handlers': ['log', 'smtp'],
        'debug': False,
        'format': 'short',
        'graphite_url': 'http://localhost',
        'charthouse_url': 'http://localhost',
        'history_size': '1day',
        'interval': '10minute',
        'logging': 'info',
        'method': 'average',
        'history_method': 'median',
        'no_data': 'critical',
        'normal_handlers': ['log', 'smtp'],
        'pidfile': None,
        'prefix': '[WATCHTOWER]',
        'public_graphite_url': None,
        'public_charthouse_url': None,
        'repeat_interval': '2hour',
        'request_timeout': 20.0,
        'connect_timeout': 20.0,
        'send_initial': False,
        'until': '0second',
        'warning_handlers': ['log', 'smtp'],
        'default_nan_value': None,
        'ignore_nan': False,
        'loading_error': 'critical',
        'ignore_alerted_history': False,
        'alerts': [],
    }

    def __init__(self, **options):
        self.alerts = set()
        self.loop = ioloop.IOLoop.instance()
        self.options = dict(self.defaults)
        self.reinit(**options)
        self.callback = ioloop.PeriodicCallback(
            self.repeat, parse_interval(self.options['repeat_interval']))

    def reinit(self, *args, **options):
        LOGGER.info('Read configuration')

        self.options.update(options)

        self.include_config(self.options.get('config'))
        for config in self.options.pop('include', []):
            self.include_config(config)

        if not self.options['public_graphite_url']:
            self.options['public_graphite_url'] = self.options['graphite_url']

        LOGGER.setLevel(_get_numeric_log_level(self.options.get('logging', 'info')))
        registry.clean()

        self.handlers = {'warning': set(), 'critical': set(), 'normal': set()}
        self.reinit_handlers('warning')
        self.reinit_handlers('critical')
        self.reinit_handlers('normal')

        for alert in list(self.alerts):
            alert.stop()
            self.alerts.remove(alert)

        self.alerts = set(
            BaseAlert.get(self, **opts).start() for opts in self.options.get('alerts'))

        LOGGER.debug('Loaded with options:')
        LOGGER.debug(json.dumps(self.options, indent=2))
        return self

    def include_config(self, config):
        LOGGER.info('Load configuration: %s' % config)
        if config:
            loader = yaml.load if yaml and config.endswith('.yml') else json.loads
            try:
                with open(config) as fconfig:
                    source = COMMENT_RE.sub("", fconfig.read())
                    config = loader(source)
                    self.options.get('alerts').extend(config.pop("alerts", []))
                    self.options.update(config)

            except (IOError, ValueError) as e:
                LOGGER.exception(e)
                LOGGER.error('Invalid config file: %s' % config)

    def reinit_handlers(self, level='warning'):
        for name in self.options['%s_handlers' % level]:
            try:
                self.handlers[level].add(registry.get(self, name))
            except Exception as e:
                LOGGER.error('Handler "%s" did not init. Error: %s' % (name, e))

    def repeat(self):
        LOGGER.info('Reset alerts')
        for alert in self.alerts:
            alert.reset()

    def start(self, *args):
        if self.options.get('pidfile'):
            with open(self.options.get('pidfile'), 'w') as fpid:
                fpid.write(str(os.getpid()))
        self.callback.start()
        LOGGER.info('Reactor starts')
        self.loop.start()

    def stop(self, *args):
        self.callback.stop()
        self.loop.stop()
        if self.options.get('pidfile'):
            os.unlink(self.options.get('pidfile'))
        LOGGER.info('Reactor has stopped')

    def notify(self, level, alert, value, target=None, ntype=None, rule=None):
        """ Provide the event to the handlers. """
        if ntype is None:
            ntype = alert.source

        handlers = self.handlers.get(level, [])

        LOGGER.info('Notifying %s handler(s)%s with %s:%s:%s:%s',
                    len(handlers),
                    ':' + ','.join(map(str, handlers)) if handlers else '',
                    level,
                    alert,
                    value,
                    target or '')

        for handler in handlers:
            handler.notify(level, alert, value, target=target, ntype=ntype, rule=rule)

    def notify_batch(self, level, alert, data):
        handlers = self.handlers.get(level, [])

        LOGGER.info('Notifying %s handler(s)%s with %s:%s:%s entries',
                    len(handlers),
                    ':' + ','.join(map(str, handlers)) if handlers else '',
                    level,
                    alert,
                    len(data))

        for handler in handlers:
            handler.notify_batch(level, alert, alert.source, data)

_LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARN': logging.WARN,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


def _get_numeric_log_level(level):
    """Convert a textual log level to the numeric constants expected by the
    :meth:`logging.Logger.setLevel` method.

    This is required for compatibility with Python 2.6 where there is no conversion
    performed by the ``setLevel`` method. In Python 2.7 textual names are converted
    to numeric constants automatically.

    :param basestring name: Textual log level name
    :return: Numeric log level constant
    :rtype: int
    """
    if not isinstance(level, int):
        try:
            return _LOG_LEVELS[str(level).upper()]
        except KeyError:
            raise ValueError("Unknown log level: %s" % level)
    return level


class ScannerReactor(Reactor):
    """Reactor for watchtower scanner"""

    scanner_defaults = {
        'scan_span': '1m',
        'scan_step': '5m',
        'scan_from': None, # should be absolute time
        'scan_until': None,
        'prefetch_size': '1day'
    }

    def __init__(self, **options):
        super(ScannerReactor, self).__init__(**options)
        self.loop.add_callback(self._scan)

        # Prevent periodic callback
        self.callback = ioloop.PeriodicCallback(id, 1e10)

    def reinit(self, *args, **options):
        LOGGER.info('Read configuration')

        self.options.update(self.scanner_defaults)
        self.options.update(options)

        self.include_config(self.options.get('config'))
        for config in self.options.pop('include', []):
            self.include_config(config)

        if not self.options['public_graphite_url']:
            self.options['public_graphite_url'] = self.options['graphite_url']

        LOGGER.setLevel(_get_numeric_log_level(self.options.get('logging', 'info')))
        registry.clean()

        self.handlers = {'warning': set(), 'critical': set(), 'normal': set()}
        self.reinit_handlers('warning')
        self.reinit_handlers('critical')
        self.reinit_handlers('normal')

        for alert in list(self.alerts):
            alert.stop()
            self.alerts.remove(alert)

        self.alerts = set()
        for opts in self.options.get('alerts'):
            opts['source'] = 'scanner'
            opts.setdefault('scan_step', self.options.get('scan_step'))
            opts.setdefault('scan_span', self.options.get('scan_span'))
            opts['interval'] = opts['scan_step']

            self.alerts.add(BaseAlert.get(self, **opts))

        LOGGER.debug('Loaded with options:')
        LOGGER.debug(json.dumps(self.options, indent=2))

        return self

    @gen.coroutine
    def _scan(self):
        yield [alert.load() for alert in self.alerts]

        LOGGER.info('All scans are done')
        self.stop()
        raise gen.Return(True)
