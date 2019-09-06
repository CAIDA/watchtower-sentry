import logging
import json
import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "file": {"type": "string"},
    },
    "required": ["file"]
}

class JsonIn(SentryModule.SentryModule):

    def __init__(self, config, input):
        logger.debug("JsonIn.__init__")
        super().__init__(config, add_cfg_schema, logger, input, isSource=True)
        self.filename = config['file']

    def run(self):
        logger.debug("JsonIn.run()")
        with open(self.filename) as f:
            for line in f:
                key, value, t = json.loads(line)
                key = bytes(key, 'ascii')
                yield (key, value, t)

        logger.debug("JsonIn.run() done")
