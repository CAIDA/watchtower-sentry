import logging
import SentryModule

logger = logging.getLogger(__name__)


class Print_kvt(SentryModule.SentryModule):
    def __init__(self, config, input):
        logger.debug("Print_kvt.__init__")
        super().__init__(config, None, logger, input, isSink = True)

    def run(self):
        logger.debug("Print_kvt.run()")
        for entry in self.input():
            print(str(entry))
        logger.debug("Print_kvt.run() done")

