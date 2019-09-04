import logging
import SentryModule

logger = logging.getLogger(__name__)

class ToSigned(SentryModule.SentryModule):
    def __init__(self, options, input):
        logger.debug("ToSigned.__init__")
        self.input = input

    @staticmethod
    def unsignedToSigned(number, bitlength):
        if number is None:
            return None
        negativeBits = (-1 << (bitlength - 1))
        if number & negativeBits:    # if lowest negative bit is on
            number |= negativeBits   # turn them all on (i.e. sign extension)
        return number

    def run(self):
        logger.debug("ToSigned.run()")
        for entry in self.input():
            logger.debug("TS: " + str(entry))
            key, value, t = entry
            value = self.unsignedToSigned(value, 64)
            yield (key, value, t)

