import logging
import SentryModule

logger = logging.getLogger(__name__)

cfg_schema = {
    "type": "object",
    "properties": {
        "name":        { "type": "string" },
    },
    "additionalProperties": { "not": {} },
}


class Print_kvt(SentryModule.SentryModule):
    def __init__(self, config, input):
        logger.debug("Print_kvt.__init__")
        super().__init__(config, cfg_schema, logger)
        self.input = input

    def run(self):
        logger.debug("Print_kvt.run()")
        for entry in self.input():
            logger.info("## " + str(entry))
        logger.debug("Print_kvt.run() done")

