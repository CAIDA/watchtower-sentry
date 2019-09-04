import logging
import SentryModule

logger = logging.getLogger(__name__)


class Print_kvt(SentryModule.SentryModule):
    schema = {
        "type": "object",
        "properties": {
            "name":        { "type": "string" },
        },
        "additionalProperties": { "not": {} },
    }

    def __init__(self, options, input):
        logger.debug("Print_kvt.__init__")
        super().__init__(options, self.schema)
        self.input = input

    def run(self):
        logger.debug("Print_kvt.run()")
        for entry in self.input():
            logger.info("## " + str(entry))
        logger.debug("Print_kvt.run() done")

