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
import jsonschema
from pytimeseries.tsk.proxy import TskReader

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


class UserError(RuntimeError):
    pass

# end class UserError


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
        # TODO: make next request in a thread parallel with processing the
        # prev response
        while self.make_next_request():
            self.handle_response()
        logger.debug("#### historic done")

# end class Historical


class Realtime:
    def __init__(self, options):
        self.tsk_reader = TskReader(
                options['topicprefix'],
                options['channelname'],
                options['consumergroup'],
                options['brokers'],
                None,
                False)
        self.shutdown = False
        self.msg_time = None
        self.msgbuf = None

    def _msg_cb(self, msg_time, version, channel, msgbuf, msgbuflen):
        if self.msgbuf == None or self.msgbuf != msgbuf:
            label = "new"
        else:
            label = "repeated"
        logger.debug("#### %s msg: %d bytes at %d" % (label, msgbuflen, msg_time))
        self.msgbuf = msgbuf
        self.msg_time = msg_time

    def _kv_cb(self, key, val):
        print([key, val, self.msg_time])

    def run(self):
        logger.debug("#### realtime start")
        while not self.shutdown:
            msg = self.tsk_reader.poll(10000)
            if msg is None:
                break
            if not msg.error():
                self.tsk_reader.handle_msg(msg.value(),
                    self._msg_cb, self._kv_cb)
                eof_since_data = 0
            elif msg.error().code() == \
                    confluent_kafka.KafkaError._PARTITION_EOF:
                # no new messages, wait a bit and then force a flush
                eof_since_data += 1
                if eof_since_data >= 10:
                    break
            else:
                logging.error("Unhandled Kafka error, shutting down")
                logging.error(msg.error())
                self.shutdown = True

        logger.debug("#### realtime done")

# end class Realtime


class Sentry:
    def __init__(self, options):
        self.options = options
        self.config = None
        configname = os.path.abspath(self.options.config)
        if configname:
            self.load_config(configname)
        if 'historical' in self.config['datasource']:
            self.source = Historical(self.config['datasource']['historical'])
        elif 'realtime' in self.config['datasource']:
            self.source = Realtime(self.config['datasource']['realtime'])

    schema = {
        "title": "Watchtower-Sentry configuration schema",
        "type": "object",
        "properties": {
            "datasource": {
                "type": "object",
                "properties": {
                    "historical": {
                        "type": "object",
                        "properties": {
                            "starttime":     { "type": "string" },
                            "endtime":       { "type": "string" },
                            "url":           { "type": "string" },
                            "batchduration": { "type": "number" },
                            "ignorenull":    { "type": "boolean" },
                            "expression":    { "type": "string" },
                            "queryparams":   { "type": "object" },
                        },
                        "required": ["starttime", "endtime", "url",
                            "batchduration", "expression"]
                    },
                    "realtime": {
                        "type": "object",
                        "properties": {
                            "brokers":       { "type": "string" },
                            "consumergroup": { "type": "string" },
                            "topicprefix":   { "type": "string" },
                            "channelname":   { "type": "string" },
                            "pattern":       { "type": "string" },
                        },
                        "required": ["brokers", "consumergroup", "topicprefix",
                            "channelname", "pattern"]
                    }
                },
                # "datasource" requires ONE of "historical" or "realtime"
                "oneOf": [
                    { "required": ["historical"] },
                    { "required": ["realtime"] }
                ],
            },
            "aggregation": {
                "type": "object",
                # XXX...
            },
            "detection": {
                "type": "object",
                # XXX...
            },
            "alerting": {
                "type": "object",
                # XXX...
            },
        }
    }

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
                raise RuntimeError('Invalid config file %s %s' %
                    (filename, str(e.args)))
            except Exception as e:
                raise RuntimeError('Invalid config file %s\n%s' %
                    (filename, str(e)))
        else:
            try:
                with open(filename) as f:
                    source = COMMENT_RE.sub("", f.read())
                    self.config = yaml.safe_load(source)
            except (IOError, ValueError) as e:
                raise RuntimeError('Invalid config file %s %s' % (filename, str(e.args)))
            except Exception as e:
                raise RuntimeError('Invalid config file %s\n%s' % (filename, str(e)))

        # "jsonschema" actually validates the loaded data structure, not the
        # raw text, so works whether the text was yaml or json.
        try:
            jsonschema.validate(instance = self.config, schema = self.schema)
        except jsonschema.exceptions.ValidationError as e:
            raise UserError(type(e).__name__ + ': ' + str(e))
            return 1

    def run(self):
        logger.debug("#### sentry start")
        self.source.run()
        logger.debug("#### sentry done")

# end class Sentry


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

    logger.debug('#### logger initialized')

    # Main body logs all exceptions
    try:
        exitstatus = main(options)
        # print("timestr: %d" % strtimegm(sys.argv[1]))
    except UserError as e:
        logger.critical(str(e))
        exitstatus = 1
    except Exception as e:
        # possible programming error; include traceback
        logger.critical(type(e).__name__ + ':\n' + traceback.format_exc())
        exitstatus = 255

    logger.debug('#### __main__ done')
    sys.exit(exitstatus)
