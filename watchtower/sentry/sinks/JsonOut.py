import logging
import json
import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "file": {"type": "string"}
    },
    "required": ['file']
}

class JsonOut(SentryModule.SentryModule):
    def __init__(self, config, gen):
        logger.debug("JsonOut.__init__")
        super().__init__(config, add_cfg_schema, logger, gen, isSink=True)
        self.filename = config['file']

    def run(self):
        logger.debug("JsonOut.run()")
        with open(self.filename, 'w') as f:
            for entry in self.gen():
                key, value, t = entry
                key = str(key, 'ascii')
                json.dump((key, value, t), f, separators=(',', ':'))
                f.write('\n')
        logger.debug("JsonOut.run() done")
