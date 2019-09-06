import logging
import SentryModule

logger = logging.getLogger(__name__)


class Print_kvt(SentryModule.SentryModule):
    def __init__(self, config, gen):
        logger.debug("Print_kvt.__init__")
        super().__init__(config, None, logger, gen, isSink=True)

    def run(self):
        logger.debug("Print_kvt.run()")
        for entry in self.gen():
            print(str(entry))
        logger.debug("Print_kvt.run() done")
