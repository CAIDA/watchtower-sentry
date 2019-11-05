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
    "properties": {},
    "required": []
}


class TimeOrderChecker(SentryModule.SentryModule):
    def __init__(self, config, gen, ctx):
        logger.debug("TimeOrderChecker.__init__")
        super().__init__(config, logger, gen)
        self.last_key_time = {}  # last_key_time[key] = ts

    def run(self):
        logger.debug("TimeOrderChecker.run()")
        for (key, val, t) in self.gen():
            if key not in self.last_key_time:
                self.last_key_time[key] = t
            else:
                assert self.last_key_time[key] < t
                self.last_key_time[key] = t
            yield (key, val, t)
