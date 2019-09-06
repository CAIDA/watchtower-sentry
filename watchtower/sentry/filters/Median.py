"""Filter that calculates the median.

Configuration parameters:
    history: (number) Number of seconds of data over which to calculate.
    warmup: (number) Minimum number of seconds of data to collect before
        generating output.
    inpainting: (object) Not yet implemented.
"""

import logging
import bisect
from collections import deque
import SentryModule

logger = logging.getLogger(__name__)
debug = True

add_cfg_schema = {
    "properties": {
        "history":       {"type": "number"},
        "warmup":        {"type": "number"},
    #   "inpainting":    {"type": "object"},
    },
    "required": ["history", "warmup"]
}


def sortedlist_add_remove(slist, additem, rmitem):
    """ Remove one item from a sorted list and add another item. """

    # Find the set of values between the value being removed and the point
    # where the new value will be inserted, and shift them towards the
    # removed value, thus overwriting the removed value and making a hole
    # to insert the new value.  On average, with a list of length N, this
    # algorithm will need to shift ~N/3 items (compared to N for the most
    # naive remove-then-insert algorithm).  (Some kind of tree would
    # probably be more efficient, though maybe not by as much as one might
    # expect, due to greater overhead.  And there are no trees in the
    # python standard library.  Consider
    # http://www.grantjenks.com/docs/sortedcontainers/.)
    logger.debug("slist:  %s", repr(slist))
    if rmitem < additem:
        left = bisect.bisect_right(slist, rmitem)
        right = bisect.bisect_left(slist, additem, lo=left)
        logger.debug("rm=%d,add=%d: left=%d, right=%d",
            rmitem, additem, left, right)
        slist[left-1:right-1] = slist[left:right]
        slist[right-1] = additem
    elif additem < rmitem:
        left = bisect.bisect_right(slist, additem)
        right = bisect.bisect_left(slist, rmitem, lo=left)
        logger.debug("add=%d,rm=%d: left=%d, right=%d",
            additem, rmitem, left, right)
        slist[left+1:right+1] = slist[left:right]
        slist[left] = additem
    # else, removing and inserting the same value is a no-op


class Median(SentryModule.SentryModule):
    def __init__(self, config, gen):
        logger.debug("Median.__init__")
        super().__init__(config, add_cfg_schema, logger, gen)
        self.warmup = config['warmup']
        self.history_duration = config['history']
        if self.history_duration <= self.warmup:
            raise SentryModule.UserError('module %s: history (%d) must be '
                'greater than ' 'warmup (%d)' %
                (self.modname, self.history_duration, self.warmup))
        self.data = dict()

    class Data:
        def __init__(self):
            self.queue = deque()  # list of (v,t) ordered by t
            self.values = None    # sorted list of values

    def run(self):
        logger.debug("Median.run()")
        for entry in self.gen():
            logger.debug("MD: %s", str(entry))
            key, value, t = entry

            if key not in self.data:
                data = Median.Data()
                self.data[key] = data
            else:
                data = self.data[key]
            data.queue.append((value, t))
            if data.queue[0][1] > t - self.warmup:
                continue # not enough points yet

            window_start = t - self.history_duration

            if not data.values:
                # Warmup is done; initialize sorted list of values (including
                # the new value)
                data.values = sorted([v for v, t in data.queue])
                logger.debug("sorted: %s", repr(data.values))

            else:
                if data.queue[0][1] > window_start:
                    # Window is not full.  Insert value into the sorted list.
                    bisect.insort(data.values, value)
                else:
                    # Window is full.  We want to remove the oldest value and
                    # insert the new value.
                    oldest = data.queue.popleft()

                    if debug:
                        correct = list(data.values)
                        correct.remove(oldest[0])
                        bisect.insort(correct, value)

                    sortedlist_add_remove(data.values, value, oldest[0])

                    if debug and data.values != correct:
                        raise RuntimeError("bad sort for %s at %d\n"
                            "old = %d, new = %d\ncorrect: %s\nresult:  %s" %
                            (key, t, oldest[0], value,
                            repr(correct), repr(data.values)))

            # If window is still overfull, remove more old items.  This can
            # happen when there's a time gap in new arrivals.
            while data.queue[0][1] <= window_start:
                oldest = data.queue.popleft()
                logger.warning("removing extra old item (%s, %d, %d)",
                    key, value, t)
                data.values.remove(oldest[0])

            logger.debug("values: %s", repr(data.values))

            yield (key, data.values[len(data.values)//2], t)
