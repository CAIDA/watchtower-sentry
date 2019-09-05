"""
Filter that passes on entries with a matching key.

Configuration parameters:
    expression: (string) a DBATS-style glob pattern to compare against keys.
"""

import logging
import re
import SentryModule

logger = logging.getLogger(__name__)

cfg_schema = {
    "type": "object",
    "properties": {
        "name":          { "type": "string" },
        "expression":    { "type": "string" },
    },
    "required": ["expression"]
}

class Keyfilter(SentryModule.SentryModule):
    def __init__(self, config, input):
        logger.debug("Keyfilter.__init__")
        super().__init__(config, cfg_schema, logger, input)
        self.expression = config['expression']
        regex = SentryModule.glob_to_regex(self.expression)
        logger.debug("expression: " + self.expression)
        logger.debug("regex:      " + regex)
        self.expression_re = re.compile(bytes(regex, 'ascii'))

    def run(self):
        logger.debug("Keyfilter.run()")
        for entry in self.input():
            key, value, t = entry
            if (self.expression_re.match(key)):
                yield (key, value, t)

