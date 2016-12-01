import signal

from tornado.options import define, options

from .core import ScannerReactor


define('config', default=ScannerReactor.defaults['config'], help='Path to an configuration file (YAML)')
define('pidfile', default=ScannerReactor.defaults['pidfile'], help='Set pid file')
define('graphite_url', default=ScannerReactor.defaults['graphite_url'], help='Graphite URL')
define('scan_from', default=ScannerReactor.scanner_defaults['scan_from'], help='Starting time')
define('scan_until', default=ScannerReactor.scanner_defaults['scan_until'], help='Ending time')


def run_scanner():
    options.parse_command_line()

    r = ScannerReactor(**options.as_dict())

    signal.signal(signal.SIGTERM, r.stop)
    signal.signal(signal.SIGINT, r.stop)
    if hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, r.reinit)

    r.start()


if __name__ == '__main__':
    run_scanner()
