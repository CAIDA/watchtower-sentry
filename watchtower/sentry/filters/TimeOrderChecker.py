"""
Check that all data points for a given key are in chronological order

Configuration parameters:
    [none]

Input:  (key, value, time)
Output:  (key, value, time)
"""

import logging
import time
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "name": {"type": "string"},
    },
    "required": []
}


class TimeOrderChecker(SentryModule.SentryModule):
    def __init__(self, config, gen, ctx):
        logger.debug("TimeOrderChecker.__init__")
        super().__init__(config, logger, gen)
        self.name = config.get("name", "TimeOrderChecker")
        self.last_key_time = {}  # last_key_time[key] = ts

    def run(self):
        logger.debug("TimeOrderChecker.run()")
        for (key, val, t) in self.gen():
            if key not in self.last_key_time:
                self.last_key_time[key] = t
            else:
                if self.last_key_time[key] >= t:
                    logger.error("[%s] Out-of-order data for '%s'. "
                                 "Last time: %d, this time: %d" %
                                 (self.name, key, self.last_key_time[key], t))
                    raise ValueError
                self.last_key_time[key] = t
            yield (key, val, t)
