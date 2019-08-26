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

    logger.debug("#### main abort") # XXX
    sys.exit(0)

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


class Datasource:
    def __init__(self, options):
        print("###### Datasource.__init__")
        self.options = options

    @staticmethod
    def new(options):
        if 'expression' not in options:
            raise RuntimeError('"datasource" missing required parameter '
                '"expression".')
        if 'historical' in options and 'realtime' in options:
            raise RuntimeError('"datasource" requires exactly one of '
                '"historical" or "realtime"; found both.')
        elif 'historical' in options:
            return Historical(options)
        elif 'realtime' in options:
            return Realtime(options)
        else:
            raise RuntimeError('"datasource" requires exactly one of '
                '"historical" or "realtime"; found neither.')

class Historical(Datasource):
    def __init__(self, options):
        print("###### Historical.__init__")
        super().__init__(options)
        self.expression = options['expression']
        self.options = options['historical']
        self.loop = None
        self.client = None
        self.start_time = strtimegm(self.options['starttime'])
        self.end_time = strtimegm(self.options['endtime'])
        self.batch_duration = int(self.options['batchduration'])
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
            'expression': self.expression
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


class Realtime(Datasource):
    def __init__(self, options):
        print("###### Realtime.__init__")
        super().__init__(options)
        self.expression = options['expression']
        options = options['realtime']
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
        regex = Sentry.glob_to_regex(self.expression)
        print("#### expression: " + self.expression)
        print("#### regex:      " + regex)
        self.expression_re = re.compile(bytes(regex, 'ascii'))

    def _msg_cb(self, msg_time, version, channel, msgbuf, msgbuflen):
        if self.msgbuf == None or self.msgbuf != msgbuf:
            label = "new"
        else:
            label = "repeated"
        logger.debug("#### %s msg: %d bytes at %d" % (label, msgbuflen, msg_time))
        self.msgbuf = msgbuf
        self.msg_time = msg_time

    def _kv_cb(self, key, val):
        if (self.expression_re.match(key)):
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
        configname = self.options.config if self.options.config \
            else default_cfg_file
        configname = os.path.abspath(configname)
        if configname:
            self.load_config(configname)
        self.source = Datasource.new(self.config['datasource'])

    schema = {
        "title": "Watchtower-Sentry configuration schema",
        "type": "object",
        "properties": {
            "datasource": {
                "type": "object",
                "properties": {
                    "expression":    { "type": "string" },
                    "historical": {
                        "type": "object",
                        "properties": {
                            "starttime":     { "type": "string" },
                            "endtime":       { "type": "string" },
                            "url":           { "type": "string" },
                            "batchduration": { "type": "number" },
                            "ignorenull":    { "type": "boolean" },
                            "queryparams":   { "type": "object" },
                        },
                        "required": ["starttime", "endtime", "url",
                            "batchduration"]
                    },
                    "realtime": {
                        "type": "object",
                        "properties": {
                            "brokers":       { "type": "string" },
                            "consumergroup": { "type": "string" },
                            "topicprefix":   { "type": "string" },
                            "channelname":   { "type": "string" },
                        },
                        "required": ["brokers", "consumergroup", "topicprefix",
                            "channelname"]
                    }
                },
# The error message for this is rather inscrutable, so we'll do our own check
# in Datasource.__init__().
#                # "datasource" requires "expression" plus exactly one of
#                # "historical" or "realtime"
#                # "required": ["expression"],
#                "oneOf": [
#                    { "required": ["expression", "historical"] },
#                    { "required": ["expression", "realtime"] }
#                ],
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

        try:
            with open(filename) as f:
                source = COMMENT_RE.sub("", f.read())
                self.config = yaml.safe_load(source)
        except Exception as e:
            raise UserError('Invalid config file %s\n%s' %
                (filename, str(e))) from None

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

    # Convert a DBATS glob to a regex.
    # Unlike DBATS, this also allows non-nested parens for aggregate grouping.
    #
    # From DBATS docs:
    # The pattern is similar to shell filename globbing, except that hierarchical components are separated by '.' instead of '/'.
    #   * matches any zero or more characters (except '.')
    #   ? matches any one character (except '.')
    #   [...] matches any one character in the character class (except '.')
    #       A leading '^' negates the character class
    #       Two characters separated by '-' matches any ASCII character between the two characters, inclusive
    #   {...} matches any one string of characters in the comma-separated list of strings
    #   Any other character matches itself.
    #   Any special character can have its special meaning removed by preceeding it with '\'.
    @staticmethod
    def glob_to_regex(glob):
        re_meta = '.^$*+?{}[]|()'
        glob_meta = '*?{}[]()'
        regex = '^'
        i = 0
        parens = 0
        while i < len(glob):
            if glob[i] == '\\':
                i += 1
                if i >= len(glob):
                    raise UserError("illegal trailing '\\' in pattern")
                elif glob[i] not in glob_meta:
                    raise UserError("illegal escape '\\%s' in pattern" % glob[i])
                elif glob[i] in re_meta:
                    regex += '\\'
                regex += glob[i]
                i += 1
            elif glob[i] == '*':
                regex += '[^.]*'
                i += 1
            elif glob[i] == '?':
                regex += '[^.]'
                i += 1
            elif glob[i] == '[':
                regex += '['
                i += 1
                if i < len(glob) and glob[i] == '^':
                    regex += '^.'
                    i += 1
                while True:
                    if i >= len(glob):
                        raise UserError("unmatched '[' in pattern")
                    if glob[i] == '\\' and i+1 < len(glob):
                        regex += glob[i:i+2]
                        i += 2
                    else:
                        regex += glob[i]
                        i += 1
                        if glob[i-1] == ']':
                            break
            elif glob[i] == '{':
                regex += '(?:' # non-capturing group
                i += 1
                while True:
                    if i >= len(glob):
                        raise UserError("unmatched '{' in pattern")
                    elif glob[i] == '\\':
                        if i+1 >= len(glob):
                            raise UserError("illegal trailing '\\' in pattern")
                        regex += glob[i:i+2]
                        i += 2
                    elif glob[i] == ',':
                        regex += '|'
                        i += 1
                    elif glob[i] == '}':
                        regex += ')'
                        i += 1
                        break
                    elif glob[i] in '.*{}[]()':
                        raise UserError("illegal character '%s' inside {} in pattern" % glob[i])
                    else:
                        if glob[i] in re_meta:
                            regex += '\\'
                        regex += glob[i]
                        i += 1
            elif glob[i] == '(':
                if parens > 0:
                    raise UserError("illegal nested parentheses in pattern")
                parens += 1
                regex += '('  # capturing group
                i += 1
            elif glob[i] == ')' and parens:
                parens -= 1
                regex += ')'
                i += 1
            else:
                if glob[i] in re_meta:
                    regex += '\\'
                regex += glob[i]
                i += 1
        if parens > 0:
            raise UserError("unmatched '(' in pattern")
        regex += '$'
        return regex

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
    parser.add_argument("--debug-glob",
        help = ("convert a glob to a regex"))
    options = parser.parse_args()

    loghandler = logging.StreamHandler()
    loghandler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d ' +
        '[%(process)d] ' +
        # '%(threadName)s ' +
        '%(levelname)-8s: %(name)s: %(message)s',
        '%H:%M:%S'))
    logging.getLogger().addHandler(loghandler) # root logger
    logger = logging.getLogger('watchtower.sentry') # sentry logger
    logger.setLevel(options.loglevel if options.loglevel else default_log_level)

    logger.debug('#### logger initialized')

    # Main body logs all exceptions
    try:
        if options.debug_glob:
            print(Sentry.glob_to_regex(options.debug_glob))
            sys.exit(0)
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
