import logging
import json
import fileinput
import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "file": {"type": "string"},  # omitted or "-" means stdin
    }
}

class JsonIn(SentryModule.SentryModule):

    def __init__(self, config, gen):
        logger.debug("JsonIn.__init__")
        super().__init__(config, add_cfg_schema, logger, gen, isSource=True)
        self.filenames = [config['file']] if 'file' in config else []

    def run(self):
        logger.debug("JsonIn.run()")
        with fileinput.input(files=self.filenames) as f:
            for line in f:
                key, value, t = json.loads(line)
                key = bytes(key, 'ascii')
                yield (key, value, t)

        logger.debug("JsonIn.run() done")
