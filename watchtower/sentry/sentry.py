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


cfg_schema = {
    "title": "Watchtower-Sentry configuration schema",
    "type": "object",
    "properties": {
        "loglevel": { "type": "string" },             # global loglevel
        "pipeline": {                                 # list of modules
            "type": "array",
            "items": {                                # module
                "type": "object",
                "properties": {
                    "name":     { "type": "string" }, # module name
                    "loglevel": { "type": "string" }, # module loglevel
                    # other module properties are defined by module
                },
                "required": [ "name" ],
            }
        }
    },
    "additionalProperties": { "not": {} },
}


def main(options):
    logger.debug("main()")

#    signal.signal(signal.SIGTERM, s.stop)
#    signal.signal(signal.SIGINT, s.stop)
#    if hasattr(signal, 'SIGHUP'):
#        signal.signal(signal.SIGHUP, s.reinit)

    s = Sentry(options)

    s.run()
    logger.debug("main() done")


class Sentry:

    def __init__(self, options):
        self.configfile = None
        configname = os.path.abspath(options.configfile)
        if configname:
            self._load_config(configname)
        if 'loglevel' in self.config:
            logging.getLogger().setLevel(self.config['loglevel'])

        self.last_mod = None
        for i, modconfig in enumerate(self.config['pipeline']):
            if self.last_mod and self.last_mod.isSink:
                raise SentryModule.UserError('Module %s is a sink; it must be '
                    'last in pipeline' % self.config['pipeline'][i-1]['name'])
            modname = modconfig['name']
            # load the module
            pymod = importlib.import_module(modname)
            # get the module's class
            modprefix, classname = modname.rsplit(".", 1)
            pyclass = getattr(pymod, classname)
            # construct an instance of the class
            input = self.last_mod.run if self.last_mod else None
            mod = pyclass(modconfig, input)
            if (not input) != mod.isSource:
                sign = '' if mod.isSource else ' not'
                raise SentryModule.UserError('Module %s is%s a source; it '
                    'must%s be first in pipeline' % (modname, sign, sign))
            self.last_mod = mod

        if not self.last_mod.isSink:
            raise SentryModule.UserError('Module %s is not a sink; it must not '
                'be last in pipeline' % self.config['pipeline'][-1]['name'])

    def _load_config(self, filename):
        logger.info('Load configuration: %s' % filename)

        try:
            with open(filename) as f:
                source = COMMENT_RE.sub("", f.read())
                self.config = yaml.safe_load(source)
        except Exception as e:
            raise SentryModule.UserError('Invalid config file %s\n%s' %
                (filename, str(e))) from None

        SentryModule.schema_validate(self.config, cfg_schema, "root")

    def run(self):
        logger.debug("sentry.run()")
        self.last_mod.run()
        logger.debug("sentry done")

# end class Sentry


if __name__ == '__main__':
    default_cfg_file = 'sentry.yaml'
    default_log_level = 'INFO'
    parser = argparse.ArgumentParser(description =
        "Detect outages in IODA data and send alerts to watchtower-alert.")
    parser.add_argument("-c", "--configfile",
        help = ("name of configuration file [%s]" % default_cfg_file),
        default = default_cfg_file)
    parser.add_argument("-L", "--loglevel",
        help = ("logging level [%s]" % default_log_level),
        default = default_log_level)
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
    loglevel = options.loglevel
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
