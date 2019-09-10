"""
Filter that passes on entries with a matching key.

Configuration parameters:
    expression: (string) a DBATS-style glob pattern to compare against keys.
"""

import logging
import re
import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "expression": {"type": "string"}
    },
    "required": ['expression']
}

class Keyfilter(SentryModule.SentryModule):
    def __init__(self, config, gen):
        logger.debug("Keyfilter.__init__")
        super().__init__(config, add_cfg_schema, logger, gen)
        self.expression = config['expression']
        regex = SentryModule.glob_to_regex(self.expression)
        logger.debug("expression: %s", self.expression)
        logger.debug("regex:      %s", regex)
        self.expression_re = re.compile(bytes(regex, 'ascii'))

    def run(self):
        logger.debug("Keyfilter.run()")
        for entry in self.gen():
            key, value, t = entry
            if self.expression_re.match(key):
                yield (key, value, t)