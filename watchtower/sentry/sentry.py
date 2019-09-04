import sys
import os
import signal
import re
import json
import logging
import logging.handlers
import traceback
import argparse
import importlib
import SentryModule

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



class Sentry:
    schema = {
        "title": "Watchtower-Sentry configuration schema",
        "type": "object",
        "properties": {
            "loglevel": { "type": "string" },
            "pipeline": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                    },
                    "required": [ "name" ],
                }
            }
        },
        "additionalProperties": { "not": {} },
    }

    def __init__(self, options):
        self.config = None
        configname = options.config if options.config else default_cfg_file
        configname = os.path.abspath(configname)
        if configname:
            self._load_config(configname)
        if 'loglevel' in self.config:
            logging.getLogger().setLevel(self.config['loglevel'])

        self.pipeline = []
        prev_run = None
        for modconfig in self.config['pipeline']:
            modname = modconfig['name']
            # load the module
            pymod = importlib.import_module(modname)
            # get the module's class
            modprefix, classname = modname.rsplit(".", 1)
            pyclass = getattr(pymod, classname)
            # construct an instance of the class
            mod = pyclass(modconfig, prev_run)
            self.pipeline.append(mod)
            prev_run = mod.run

    def _load_config(self, filename):
        logger.info('Load configuration: %s' % filename)

        try:
            with open(filename) as f:
                source = COMMENT_RE.sub("", f.read())
                self.config = yaml.safe_load(source)
        except Exception as e:
            raise SentryModule.UserError('Invalid config file %s\n%s' %
                (filename, str(e))) from None

        SentryModule.schema_validate(self.config, self.schema, "root")

    def run(self):
        logger.debug("sentry.run()")
        self.pipeline[-1].run()
        logger.debug("sentry done")

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
        '%(name)-20s '
        '%(process)d:'
        '%(threadName)-10s '
        '%(levelname)-8s: %(message)s'
        '\x1b[m',
        '%H:%M:%S'))
    loglevel = options.loglevel if options.loglevel else default_log_level
    rootlogger = logging.getLogger()
    rootlogger.addHandler(loghandler)
    rootlogger.setLevel(loglevel)
    logger = logging.getLogger('watchtower.sentry') # sentry logger

    # Main body logs all exceptions
    try:
        if options.debug_glob:
            print(SentryModule.glob_to_regex(options.debug_glob))
            sys.exit(0)
        exitstatus = main(options)
        # print("timestr: %d" % strtimegm(sys.argv[1]))
    except SentryModule.UserError as e:
        logger.critical(str(e))
        exitstatus = 1
    except:
        # possible programming error; include traceback
        e = sys.exc_info()[1]
        logger.critical(type(e).__name__ + ':\n' + traceback.format_exc())
        exitstatus = 255

    logger.debug('__main__ done')
    sys.exit(exitstatus)
