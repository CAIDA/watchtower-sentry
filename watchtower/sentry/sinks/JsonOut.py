import sys
import logging
import json
import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "file": {"type": "string"} # omitted or '-' means stdout
    },
}

class JsonOut(SentryModule.Sink):
    def __init__(self, config, gen):
        logger.debug("JsonOut.__init__")
        super().__init__(config, logger, gen)
        self.filename = config.get('file', '-')

    def run(self):
        logger.debug("JsonOut.run()")
        f = sys.stdout
        try:
            if self.filename != '-':
                f = open(self.filename, 'w')
            for entry in self.gen():
                key, value, t = entry
                key = str(key, 'ascii')
                json.dump((key, value, t), f, separators=(',', ':'))
                f.write('\n')
        finally:
            if f is not sys.stdout:
                f.close()
        logger.debug("JsonOut.run() done")
