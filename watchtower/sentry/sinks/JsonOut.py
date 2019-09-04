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


class JsonOut(SentryModule.SentryModule):
    def __init__(self, config, input):
        logger.debug("JsonOut.__init__")
        super().__init__(config, cfg_schema, logger, input, isSink = True)
        self.filename = config['file']

    def run(self):
        logger.debug("JsonOut.run()")
        with open(self.filename, 'w') as f:
            for entry in self.input():
                key, value, t = entry
                key = str(key, 'ascii')
                json.dump((key, value, t), f, separators=(',',':'))
                f.write('\n')
        logger.debug("JsonOut.run() done")

