import sys
import os
import signal
import re
from urllib.parse import urlencode
import json
import time
import calendar
import logging

from tornado import gen, httpclient, ioloop, log
from tornado.options import define, options

try:
    import yaml
except ImportError:
    yaml = None

LOGGER = log.gen_log

COMMENT_RE = re.compile('//\s+.*$', re.M)

####################################################
# DECORATED
@gen.coroutine
def a_decorated():
    b = yield c()
    raise gen.Return(b)

# NATIVE
async def a_native():
    b = await c()
    return b
####################################################

define('config', default='sentry.yaml', help='Path to configuration file (YAML/JSON)')
# define('pidfile', default='sentry.pid', help='Name of pid file')


def main():
    options.parse_command_line()

    s = Sentry(options.as_dict())

#    signal.signal(signal.SIGTERM, s.stop)
#    signal.signal(signal.SIGINT, s.stop)
#    if hasattr(signal, 'SIGHUP'):
#        signal.signal(signal.SIGHUP, s.reinit)

    LOGGER.debug("#### app start")
    s.run()
    LOGGER.debug("#### app done")


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
            return
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
        body = urlencode(post_data)
        request = httpclient.HTTPRequest(self.options['url'], method='POST',
            headers=None, body=body)
        LOGGER.debug("#### request: %d - %d" % (self.start_batch, self.end_batch))
        LOGGER.debug("#### request body: %s" % body)
        response = self.client.fetch(request)
        self.loop.add_future(response, self.handle_response)

    def handle_response(self, response):
        # start the next request so it runs while we process the prev response
        self.make_next_request()

        LOGGER.debug("#### request time: %d" % response.result().request_time)
        LOGGER.debug("#### response code: %d" % response.result().code)
        result = json.loads(response.result().body)
        LOGGER.debug("#### response: %s - %s\n" % (result['queryParameters']['from'], result['queryParameters']['until']))

        for key in result['data']['series']:
            t = int(result['data']['series'][key]['from'])
            step = int(result['data']['series'][key]['step'])
            for value in result['data']['series'][key]['values']:
                print([key, value, t])
                t += step

        if self.start_batch >= self.end_time:
            self.loop.stop()

    def run(self):
        LOGGER.debug("#### historic start")
        self.loop = ioloop.IOLoop.current()
        self.client = httpclient.AsyncHTTPClient()
        self.make_next_request()
        self.loop.start()
        LOGGER.debug("#### historic done")

class Sentry:
    def __init__(self, optdict):
        self.options = optdict
        self.config = None
        configname = self.options.get('config')
        if configname:
            self.load_config(configname)
        if 'historical' in self.config['datasource'] and 'realtime' in self.config['datasource']:
            raise RuntimeError("'datasource' may contain only one of 'historical' or 'realtime'")
        if 'historical' in self.config['datasource']:
            self.source = Historical(self.config['datasource']['historical'])
        #if 'realtime' in self.config['datasource']:
        #    self.source = Realtime(self.config['datasource']['realtime'])

    def load_config(self, filename):
        LOGGER.info('Load configuration: %s' % filename)
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

    def run(self):
        LOGGER.debug("#### sentry start")
        self.source.run()
        LOGGER.debug("#### sentry done")

if __name__ == '__main__':
    try:
        main()
        # print("timestr: %d" % strtimegm(sys.argv[1]))
    except Exception as e:
        LOGGER.exception(e)

