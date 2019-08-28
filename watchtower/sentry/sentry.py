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
import threading
import jsonschema
from pytimeseries.tsk.proxy import TskReader

exitstatus = 0

try:
    import yaml
except ImportError:
    yaml = None

COMMENT_RE = re.compile('//\s+.*$', re.M)


def main(options):
    logger.debug("main()")

#    signal.signal(signal.SIGTERM, s.stop)
#    signal.signal(signal.SIGINT, s.stop)
#    if hasattr(signal, 'SIGHUP'):
#        signal.signal(signal.SIGHUP, s.reinit)

    s = Sentry(options)

    s.run()
    logger.debug("main done")


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
        logger.debug("Datasource.__init__")
        self.done = False
        self.options = options
        self.incoming = []
        self.producable = True
        self.consumable = False
        # The reader thread produces data by reading it from its source and
        # appending it to self.incoming.
        self.reader = threading.Thread(target = self.run_reader,
            daemon = True, # program need not join() this thread to exit
            name = "\x1b[31mDS.reader")
        lock = threading.Lock()
        self.cond_producable = threading.Condition(lock)
        self.cond_consumable = threading.Condition(lock)

    @staticmethod
    def new(options):
        if 'historical' in options:
            return Historical(options)
        elif 'realtime' in options:
            return Realtime(options)
        else:
            return None

    # Consume data produced by the reader thread.
    def run(self):
        logger.debug("Datasource.run()")
        self.reader.start()
        try:
            while True:
                # wait for reader thread to fill self.incoming
                data = None
                with self.cond_consumable:
                    logger.debug("cond_consumable check")
                    while not self.consumable and not self.done:
                        logger.debug("cond_consumable.wait")
                        self.cond_consumable.wait()
                    if self.consumable:
                        data = self.incoming
                        self.incoming = None
                    elif self.done == True:
                        logger.debug("Datasource.run(): end-of-stream")
                        break
                    else: # if self.done:
                        logger.debug("Datasource.run(): error in reader")
                        break
                    self.consumable = False
                    logger.debug("cond_consumable.wait DONE (%d items)" %
                        (len(data)))
                # Tell reader thread that self.incoming is ready to be refilled
                with self.cond_producable:
                    logger.debug("cond_producable.notify")
                    self.producable = True
                    self.cond_producable.notify()
                # Yield control to the reader so it can request the next set of
                # data; then while it waits for the response it will return
                # control to this thread.
                time.sleep(0)
                # Process the data.
                for entry in data:
                    yield entry
            self.reader.join()
        except:
            e = sys.exc_info()[1]
            logger.critical(type(e).__name__ + ':\n' + traceback.format_exc())
            with self.cond_producable:
                logger.debug("cond_producable.notify")
                self.done = "exception in Datasource.run"
                self.cond_producable.notify()

class Historical(Datasource):
    def __init__(self, options):
        logger.debug("Historical.__init__")
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
        logger.debug("request: %d - %d" % (self.start_batch, self.end_batch))
        self.request = requests.post(self.options['url'], data = post_data, timeout = 60)
        return True

    def handle_response(self):
        logger.debug("response code: %d" % self.request.status_code)
        self.request.raise_for_status()
        result = self.request.json()
        logger.debug("response: %s - %s" % (result['queryParameters']['from'], result['queryParameters']['until']))

        # wait for self.incoming to be empty
        with self.cond_producable:
            logger.debug("cond_producable check")
            while not self.producable and not self.done:
                logger.debug("cond_producable.wait")
                self.cond_producable.wait()
            if self.done:
                return
            self.incoming = [];
            self.producable = False
            logger.debug("cond_producable.wait DONE")
        for key in result['data']['series']:
            t = int(result['data']['series'][key]['from'])
            step = int(result['data']['series'][key]['step'])
            ascii_key = bytes(key, 'ascii')
            for value in result['data']['series'][key]['values']:
                self.incoming.append((ascii_key, value, t))
                t += step
        # tell computation thread that self.incoming is now full
        with self.cond_consumable:
            logger.debug("cond_consumable.notify")
            self.consumable = True
            self.cond_consumable.notify()

    def run_reader(self):
        try:
            logger.debug("historic.run()")
            while not self.done and self.make_next_request():
                self.handle_response()
            logger.debug("historic done")
            with self.cond_consumable:
                logger.debug("cond_consumable.notify (done=True)")
                self.done = True
                self.cond_consumable.notify()
        except:
            e = sys.exc_info()[1]
            logger.critical(type(e).__name__ + ':\n' + traceback.format_exc())
            with self.cond_consumable:
                logger.debug("cond_consumable.notify (exception)")
                self.done = "exception in historical reader"
                self.cond_consumable.notify()

# end class Historical


class Realtime(Datasource):
    def __init__(self, options):
        logger.debug("Realtime.__init__")
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
        self.msg_time = None
        self.msgbuf = None
        regex = Sentry.glob_to_regex(self.expression)
        logger.debug("expression: " + self.expression)
        logger.debug("regex:      " + regex)
        self.expression_re = re.compile(bytes(regex, 'ascii'))

    def _msg_cb(self, msg_time, version, channel, msgbuf, msgbuflen):
        if self.msgbuf == None or self.msgbuf != msgbuf:
            label = "new"
        else:
            label = "repeated"
        logger.debug("%s msg: %d bytes at %d" % (label, msgbuflen, msg_time))
        self.msgbuf = msgbuf
        self.msg_time = msg_time

    def _kv_cb(self, key, val):
        if (self.expression_re.match(key)):
            self.incoming.append((key, val, self.msg_time))

    def run_reader(self):
        try:
            logger.debug("realtime.run()")
            while not self.done:
                logger.debug("tsk_reader_poll")
                msg = self.tsk_reader.poll(10000)
                if msg is None:
                    break
                if not msg.error():
                    # wait for self.incoming to be empty
                    with self.cond_producable:
                        logger.debug("cond_producable check")
                        while not self.producable:
                            logger.debug("cond_producable.wait")
                            self.cond_producable.wait()
                        self.incoming = [];
                        self.producable = False;
                        logger.debug("cond_producable.wait DONE")
                    self.tsk_reader.handle_msg(msg.value(),
                        self._msg_cb, self._kv_cb)
                    eof_since_data = 0
                    # tell computation thread that self.incoming is now full
                    with self.cond_consumable:
                        logger.debug("cond_consumable.notify")
                        self.consumable = True
                        self.cond_consumable.notify()
                elif msg.error().code() == \
                        confluent_kafka.KafkaError._PARTITION_EOF:
                    # no new messages, wait a bit and then force a flush
                    eof_since_data += 1
                    if eof_since_data >= 10:
                        break
                else:
                    logging.error("Unhandled Kafka error, shutting down")
                    logging.error(msg.error())
                    with self.cond_consumable:
                        logger.debug("cond_consumable.notify (error)")
                        self.done = "error in realtime reader"
                        self.cond_consumable.notify()
                    break
            logger.debug("realtime done")
            if not self.done:
                with self.cond_consumable:
                    logger.debug("cond_consumable.notify (done=True)")
                    self.done = True
                    self.cond_consumable.notify()
        except:
            e = sys.exc_info()[1]
            logger.critical(type(e).__name__ + ':\n' + traceback.format_exc())
            with self.cond_consumable:
                logger.debug("cond_consumable.notify (exception)")
                self.done = "exception in realtime reader"
                self.cond_consumable.notify()

# end class Realtime


class AggSum:
    def __init__(self, options, input):
        logger.debug("AggSum.__init__")
        self.input = input
        self.options = options
        self.expression = options['expression']
        self.ascii_expression = bytes(self.expression, 'ascii')
        self.datadict = dict() # map of (groupid, t) to [count, sum]
        self.groupsize = int(options['groupsize']) if 'groupsize' in options \
            else None

        regex = Sentry.glob_to_regex(self.expression)
        logger.debug("expression: " + self.expression)
        logger.debug("regex:      " + regex)
        self.expression_re = re.compile(bytes(regex, 'ascii'))

    def run(self):
      if False:
        for entry in self.input():
            logger.debug("AG: " + str(entry))
            yield entry
      else:
        logger.debug("AggSum.run()")
        for entry in self.input():
            logger.debug("AG: " + str(entry))
            key, value, t = entry
            match = self.expression_re.match(key)
            if not match:
                continue
            groupid = match.groups()
            dictkey = (groupid, t)
            if dictkey not in self.datadict:
                self.datadict[dictkey] = [0, 0]
            self.datadict[dictkey][0] += 1
            if value is not None:
                self.datadict[dictkey][1] += value

            logger.debug("k=%s, v=%s, t=%d; count=%d, sum=%s" %
                (str(groupid), str(value), t,
                    self.datadict[dictkey][0], self.datadict[dictkey][1]))

            if self.groupsize and \
                    self.datadict[dictkey][0] == self.groupsize:
                sum = value
                sum = self.datadict[dictkey][1]
                groupkey = self.ascii_expression
                for part in groupid:
                    logger.debug('part: ' + str(part))
                    groupkey = re.sub(b"\([^)]*\)", part, groupkey)
                yield (groupkey, sum, t)

            # TODO: also check timeout

        logger.debug("AggSum.run() done")

# end class AggSum


class Sink:
    def __init__(self, input):
        self.input = input

    def run(self):
        logger.debug("Sink.run()")
        for entry in self.input():
            logger.info("## " + str(entry))
        logger.debug("Sink.run() done")

# end class Sink


class Sentry:
    def __init__(self, options):
        self.options = options
        self.config = None
        configname = self.options.config if self.options.config \
            else default_cfg_file
        configname = os.path.abspath(configname)
        if configname:
            self.load_config(configname)
        if 'loglevel' in self.config:
            logger.setLevel(self.config['loglevel'])
        self.source = Datasource.new(self.config['datasource'])
        self.agg = AggSum(self.config['aggregation'], self.source.run)
        self.sink = Sink(self.agg.run)

    schema = {
        "title": "Watchtower-Sentry configuration schema",
        "type": "object",
        "properties": {
            "loglevel": { "type": "string" },
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
                        "additionalProperties": { "not": {} },
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
                        "additionalProperties": { "not": {} },
                        "required": ["brokers", "consumergroup", "topicprefix",
                            "channelname"]
                    }
                },
                "additionalProperties": { "not": {} },
                "required": ["expression"],
                # "datasource" requires exactly 1 of "historical" or "realtime"
                "oneOf": [
                    { "required": ["historical"] },
                    { "required": ["realtime"]   },
                ],
            },
            "aggregation": {
                "type": "object",
                "properties": {
                    "method":      { "type": "string" },
                    "expression":  { "type": "string" },
                    "groupsize":   { "type": "number" },
                    "timeout":     { "type": "number" },
                    "droppartial": { "type": "boolean" },
                },
                "required": ["method", "expression"],
            },
            "detection": {
                "type": "object",
                # XXX...
            },
            "alerting": {
                "type": "object",
                # XXX...
            },
        },
        "additionalProperties": { "not": {} },
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
          if True:
            msg = e.message
            # Some messages begin with a potentially very long dump of the
            # value of a json instance.  We strip the value, since we're going
            # to print the path of that instance.
            if msg.startswith(repr(e.instance)):
                msg = msg[len(repr(e.instance)):]
            path = ''.join([('[%d]' % i) if isinstance(i, int) \
                else ('.%s' % str(i)) for i in e.absolute_path])
            raise UserError(type(e).__name__ + ' in ' + filename + ' at root' +
                path + ': ' + msg)
          elif False:
            raise UserError(type(e).__name__ + ': ' + str(e))
          elif False:
            raise UserError(type(e).__name__ + ': ' + str(e.message))
          else:
            raise UserError(type(e).__name__ + ': ' +
                "\nmessage:" + str(e.message) +
                "\nschema:" + str(e.schema) +
                "\nschema_path:" + str(e.schema_path) +
                "\npath:" + str(e.path) +
                "\nabsolute_path:" + str(e.absolute_path) +
                "\ninstance:" + str(e.instance)
                )
            return 1

    def run(self):
        logger.debug("sentry.run()")
        self.sink.run()
        logger.debug("sentry done")

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
    loghandler.setFormatter(logging.Formatter(
        '%(asctime)s.%(msecs)03d '
        # '%(name)s[%(process)d] '
        '%(process)d:'
        '%(threadName)-10s '
        '%(levelname)-8s: %(message)s'
        '\x1b[m',
        '%H:%M:%S'))
    logging.getLogger().addHandler(loghandler) # root logger
    logger = logging.getLogger('watchtower.sentry') # sentry logger
    logger.setLevel(options.loglevel if options.loglevel else default_log_level)

    logger.debug('logger initialized')

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
    except:
        # possible programming error; include traceback
        e = sys.exc_info()[1]
        logger.critical(type(e).__name__ + ':\n' + traceback.format_exc())
        exitstatus = 255

    logger.debug('__main__ done')
    sys.exit(exitstatus)
