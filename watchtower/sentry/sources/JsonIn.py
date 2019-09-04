import logging
import SentryModule
import json

logger = logging.getLogger(__name__)

cfg_schema = {
    "type": "object",
    "properties": {
        "name":        { "type": "string" },
        "file":        { "type": "string" },
    },
    "required": ["file"],
}

class JsonIn(SentryModule.SentryModule):

    def __init__(self, config, input):
        logger.debug("JsonIn.__init__")
        super().__init__(config, cfg_schema, logger)
        if input is not None:
            raise UserError('Module %s must be first in pipeline\n' %
                (config['name']))
        self.filename = config['file']

    def run(self):
        logger.debug("JsonIn.run()")
        with open(self.filename) as f:
            for line in f:
                key, value, t = json.loads(line)
                key = bytes(key, 'ascii')
                yield((key, value, t))

        logger.debug("JsonIn.run() done")

