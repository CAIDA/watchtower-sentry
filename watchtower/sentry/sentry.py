import sys
import os
import signal
import re
import urllib
import json
import time
import calendar
import logging

from tornado import gen, httpclient, ioloop, log
from tornado.options import define, options

try:
    import yaml
except ImportError:
    try:
        import pyyaml
        yaml = pyyaml
    except ImportError:
        yaml = None


LOGGER = log.gen_log

COMMENT_RE = re.compile('//\s+.*$', re.M)


define('config', default='sentry.yaml', help='Path to configuration file (YAML/JSON)')
# define('pidfile', default='sentry.pid', help='Name of pid file')


def main():
    options.parse_command_line()

    s = Sentry(options.as_dict())

#    signal.signal(signal.SIGTERM, s.stop)
#    signal.signal(signal.SIGINT, s.stop)
#    if hasattr(signal, 'SIGHUP'):
#        signal.signal(signal.SIGHUP, s.reinit)

    print("#### app start")
    s.run()
    print("#### app done")


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
        # self.batch_duration = 21600
        self.batch_duration = 3600
        self.start_time = 1546300800
        self.end_time =   1546322400
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
            'expression': 'active.ping-slash24.geo.netacuity.NA.*.probers.team-*.caida-sdsc.*.up_slash24_cnt',
            'aggrScheme': 'db',
            'maxPointsPerSeries': 100,
        }
        body = urllib.urlencode(post_data)
        url = 'https://ioda.caida.org/data/ts/json'
        request = httpclient.HTTPRequest(url, method='POST', headers=None, body=body)
        print("#### request: %d - %d" % (self.start_batch, self.end_batch))
        response = self.client.fetch(request)
        self.loop.add_future(response, self.handle_response)

    def handle_response(self, response):
        # start the next request so it runs while we process the prev response
        self.make_next_request()

        print("#### request time: %d" % response.result().request_time)
        print("#### response code: %d" % response.result().code)
        result = json.loads(response.result().body)
        print("#### response: %s - %s\n" % (result["queryParameters"]["from"], result["queryParameters"]["until"]))

        if self.start_batch >= self.end_time:
            self.loop.stop()

    def run(self):
        print("#### historic start")
        self.loop = ioloop.IOLoop.current()
        self.client = httpclient.AsyncHTTPClient()
        self.make_next_request()
        self.loop.start()
        print("#### historic done")

class Sentry:
    def __init__(self, optdict):
        self.options = optdict
        self.config = None
        configname = self.options.get('config')
        if configname:
            self.load_config(configname)
        self.source = Historical(self.config['datasource']['historical'])

    def load_config(self, filename):
        LOGGER.info('Load configuration: %s' % filename)
        LOGGER.info('yaml: %s' % (not (not yaml)))
        LOGGER.info('suffix: %s' % filename.endswith('.yaml'))
        loader = yaml.safe_load if yaml and filename.endswith('.yaml') else json.loads
        try:
            with open(filename) as f:
                source = COMMENT_RE.sub("", f.read())
                self.config = loader(source)
        except (IOError, ValueError) as e:
            raise RuntimeError('Invalid config file %s %s' % (filename, str(e.args)))

    def run(self):
        print("#### sentry start")
        self.source.run()
        print("#### sentry done")

if __name__ == '__main__':
    try:
        main()
        # print("timestr: %d" % strtimegm(sys.argv[1]))
    except Exception as e:
        LOGGER.exception(e)

