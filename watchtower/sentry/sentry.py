import sys
import os
import signal
import re
import json
import time
import calendar
import logging
import logging.handlers
import traceback
import argparse
import requests

exitstatus = 0

try:
    import yaml
except ImportError:
    yaml = None

COMMENT_RE = re.compile('//\s+.*$', re.M)


def main(options):
    logger.debug("#### main start")

#    signal.signal(signal.SIGTERM, s.stop)
#    signal.signal(signal.SIGINT, s.stop)
#    if hasattr(signal, 'SIGHUP'):
#        signal.signal(signal.SIGHUP, s.reinit)

    s = Sentry(options)
    s.run()
    logger.debug("#### main done")


# Convert a time string in 'YYYY-mm-dd [HH:MM[:SS]]' format (in UTC) to a unix
# timestamp
def strtimegm(str):
    for fmt in [ "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d" ]:
        try:
            return calendar.timegm(time.strptime(str, fmt))
        except:
            continue
    raise ValueError("Invalid date '%s'; expected 'YYYY-mm-dd [HH:MM[:SS]]'" % str)

class Historical:
    def __init__(self, options):
        self.options = options
        self.loop = None
        self.client = None
        for name in ['starttime', 'endtime', 'batchduration', 'expression', 'url']:
            if not name in options:
                raise RuntimeError("Missing required parameter '%s' in 'historical'" % name)
        self.start_time = strtimegm(options['starttime'])
        self.end_time = strtimegm(options['endtime'])
        self.batch_duration = int(options['batchduration'])
        self.queryparams = self.options['queryparams'] \
            if 'queryparams' in self.options else None
        self.end_batch = self.start_time

    def make_next_request(self):
        self.start_batch = self.end_batch
        if self.start_batch >= self.end_time:
            return False
        self.end_batch += self.batch_duration
        if self.end_batch >= self.end_time:
            self.end_batch = self.end_time
        post_data = {
            'from': self.start_batch,
            'until': self.end_batch,
            'expression': self.options['expression']
        }
        if self.queryparams:
            post_data.update(self.queryparams)
        logger.debug("#### request: %d - %d" % (self.start_batch, self.end_batch))
        self.request = requests.post(self.options['url'], data = post_data, timeout = 60)
        return True

    def handle_response(self):
        logger.debug("#### response code: %d" % self.request.status_code)
        self.request.raise_for_status()
        result = self.request.json()
        logger.debug("#### response: %s - %s\n" % (result['queryParameters']['from'], result['queryParameters']['until']))

        for key in result['data']['series']:
            t = int(result['data']['series'][key]['from'])
            step = int(result['data']['series'][key]['step'])
            for value in result['data']['series'][key]['values']:
                print([key, value, t])
                t += step


    def run(self):
        logger.debug("#### historic start")
        # TODO: do make next request in a thread parallel with processing the prev response
        while self.make_next_request():
            self.handle_response()
        logger.debug("#### historic done")


class Realtime:
    def __init__(self, options):
        raise RuntimeError('not yet implemented')


class Sentry:
    def __init__(self, options):
        self.options = options
        self.config = None
        configname = os.path.abspath(self.options.config)
        if configname:
            self.load_config(configname)
        if 'datasource' not in self.config:
            raise RuntimeError("config is missing 'datasource'")
        if 'historical' in self.config['datasource']:
            if 'realtime' in self.config['datasource']:
                raise RuntimeError("'datasource' may contain only one of 'historical' or 'realtime'")
            self.source = Historical(self.config['datasource']['historical'])
        elif 'realtime' in self.config['datasource']:
            self.source = Realtime(self.config['datasource']['realtime'])
        else:
            raise RuntimeError("'datasource' is missing one of 'historical' or 'realtime'")

    def load_config(self, filename):
        logger.info('Load configuration: %s' % filename)

        if False:
            if filename.endswith('.yaml'):
                if not yaml:
                    raise RuntimeError("yaml not supported")
                loader = yaml.safe_load
            else:
                loader = json.loads
            try:
                with open(filename) as f:
                    source = COMMENT_RE.sub("", f.read())
                    self.config = loader(source)
            except (IOError, ValueError) as e:
                raise RuntimeError('Invalid config file %s %s' % (filename, str(e.args)))
            except Exception as e:
                raise RuntimeError('Invalid config file %s\n%s' % (filename, str(e)))
        else:
            try:
                with open(filename) as f:
                    source = COMMENT_RE.sub("", f.read())
                    self.config = yaml.safe_load(source)
            except (IOError, ValueError) as e:
                raise RuntimeError('Invalid config file %s %s' % (filename, str(e.args)))
            except Exception as e:
                raise RuntimeError('Invalid config file %s\n%s' % (filename, str(e)))

    def run(self):
        logger.debug("#### sentry start")
        self.source.run()
        logger.debug("#### sentry done")

if __name__ == '__main__':
    default_cfg_file = 'sentry.yaml'
    default_log_level = 'INFO'
    parser = argparse.ArgumentParser(description =
        "Detect outages in IODA data and send alerts to watchtower-alert.")
    parser.add_argument("-c", "--config",
        help = ("name of configuration file [%s]" % default_cfg_file))
    parser.add_argument("-L", "--loglevel",
        help = ("logging level [%s]" % default_log_level))
    options = parser.parse_args()

    loghandler = logging.StreamHandler()
    loghandler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d ' +
        '[%(process)d] ' +
        # '%(threadName)s ' +
        '%(levelname)-8s: %(name)s: %(message)s',
        '%H:%M:%S'))
    logging.getLogger().addHandler(loghandler) # root logger
    logger = logging.getLogger('watchtower.sentry') # sentry logger
    logger.setLevel(options.loglevel)

    logger.debug('#### main running')

    # Main body logs all exceptions
    try:
        main(options)
        # print("timestr: %d" % strtimegm(sys.argv[1]))
    except Exception as e:
        logger.critical('Exception:\n' + traceback.format_exc())
        exitstatus = 1

exit(exitstatus)
