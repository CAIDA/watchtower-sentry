"""Filter that calculates the moving median over time.

Configuration parameters ('*' indicates required parameter):
    history*: (integer) Number of seconds of data over which to calculate.
    warmup*: (integer) Minimum number of seconds of data to collect before
        generating output.
    inpainting:
        min: (number <1.0) inpaint if (value/median) falls below this value
        max: (number >1.0) inpaint if (value/median) rises above this value
        maxduration*: (integer) maximum time (in seconds) to inpaint.  If this
            time is exceeded, the previously inpainted values are replaced
            with their original values for purposes of calculating the median.
            I.e., the values previously considered extreme will now be
            considered the new normal.

Input:  (key, value, time)

Output:  (key, value, time)
    key is the same as input key.
    value is the median of all values for the same key where (old.time >
        new.time - history).
    time is the same as input time.
"""

import logging
import bisect
from collections import deque
import SentryModule

logger = logging.getLogger(__name__)
debug = True

add_cfg_schema = {
    "properties": {
        "history":       {"type": "integer", "exclusiveMinimum": 0},
        "warmup":        {"type": "integer", "exclusiveMinimum": 0},
        "inpainting":    {
            "type": "object",
            "properties": {
                "min":         {"type": "number", "exclusiveMaximum": 1},
                "max":         {"type": "number", "exclusiveMinimum": 1},
                "maxduration": {"type": "integer", "exclusiveMinimum": 0},
            },
            "additionalProperties": False,
            "required": ["maxduration"],
        },
    },
    "required": ["history", "warmup"]
}


def _sortedlist_add_remove(slist, additem, rmitem):
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
    ## logger.debug("slist:  %s", repr(slist))
    if rmitem < additem:
        left = bisect.bisect_right(slist, rmitem)
        right = bisect.bisect_left(slist, additem, lo=left)
        logger.debug("rm=%d,add=%d: left=%d, right=%d", rmitem, additem, left, right)
        slist[left-1:right-1] = slist[left:right]
        slist[right-1] = additem
    elif additem < rmitem:
        left = bisect.bisect_right(slist, additem)
        right = bisect.bisect_left(slist, rmitem, lo=left)
        logger.debug("add=%d,rm=%d: left=%d, right=%d", additem, rmitem, left, right)
        slist[left+1:right+1] = slist[left:right]
        slist[left] = additem
    #else: # removing and inserting the same value is a no-op
        #logger.debug("add=%d,rm=%d: no-op", additem, rmitem)


def _median(slist):
    return slist[len(slist)//2]


class Median(SentryModule.SentryModule):
    def __init__(self, config, gen):
        logger.debug("Median.__init__")
        super().__init__(config, logger, gen)
        self.warmup = config['warmup']
        self.history_duration = config['history']
        if self.history_duration <= self.warmup:
            raise SentryModule.UserError('module %s: history (%d) must be '
                'greater than ' 'warmup (%d)' %
                (self.modname, self.history_duration, self.warmup))
        if 'inpainting' in config:
            inp = config['inpainting']
            self.inpaint_maxduration = inp.get('maxduration', None)
            self.inpaint_min = inp.get('min', None)
            self.inpaint_max = inp.get('max', None)
        else:
            self.inpaint_maxduration = None
            self.inpaint_min = None
            self.inpaint_max = None
        self.data = dict()

    class Data:
        def __init__(self):
            self.q = deque()   # list of (v,t) ordered by t (maybe inpainted)
            self.values = None # sorted list of values
            self.raw_q = None  # list of raw (v,t) collected while inpainting
            self.inpaint_start = None # when did inpainting start

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
            if not data.q or data.q[0][1] > t - self.warmup:
                # not enough points yet.  Just store the new value.
                data.q.append((value, t))
                continue

            window_start = t - self.history_duration

            if not data.values:
                # Warmup is done; initialize sorted list of values (not
                # including the new value)
                data.values = sorted([v for v, t in data.q])
                logger.debug("sorted: %s", repr(data.values))

            # If window is overfull, remove old items.  This can happen when
            # there's a time gap in new arrivals.
            while data.q and data.q[0][1] < window_start:
                oldest = data.q.popleft()
                logger.warning("removing extra old item (%s, %d, %d)",
                    key, oldest[0], oldest[1])
                data.values.remove(oldest[0])

            # Calculate predicted value based on data in the window (not
            # including the new value)
            predicted = _median(data.values)
            ratio = value/predicted if predicted else None
            logger.debug("predicted=%s, value=%s, ratio=%s",
                repr(predicted), repr(value), repr(ratio))

            newval = value

            if ratio is not None and (
                    (self.inpaint_min and ratio < self.inpaint_min) or
                    (self.inpaint_max and ratio > self.inpaint_max)):
                # New value is extreme
                if (self.inpaint_min and ratio < self.inpaint_min):
                    logger.debug("ratio %f < min %f", ratio, self.inpaint_min)
                if (self.inpaint_max and ratio > self.inpaint_max):
                    logger.debug("ratio %f > max %f", ratio, self.inpaint_max)
                if not data.inpaint_start:
                    # Start inpainting
                    logger.debug("### extreme value: start inpainting")
                    data.inpaint_start = t
                    data.raw_q = deque()
                    data.raw_q.append((value, t))
                    newval = predicted
                elif data.inpaint_start > t - self.inpaint_maxduration:
                    # Continue inpainting
                    logger.debug("### extreme value: continue inpainting")
                    data.raw_q.append((value, t))
                    newval = predicted
                else:
                    # Undo previous inpainting (extreme is the new normal)
                    logger.debug("### extreme value: new normal")
                    popped = 0
                    while data.q and data.q[-1][1] >= data.inpaint_start:
                        vt = data.q.pop()
                        logger.debug("popped: %s", repr(vt))
                        popped += 1
                    logger.debug("raw_q: %s", repr(data.raw_q))
                    if popped != len(data.raw_q):
                        logger.error("inpainted items (%s) != raw items (%d) "
                            "at (%s, %d), inpaint_start=%d",
                            popped, len(data.raw_q), key, t, data.inpaint_start)
                    data.q.extend(data.raw_q)
                    data.raw_q = None
                    data.values = sorted([v for v, t in data.q])
                    logger.debug("sorted: %s", repr(data.values))
                    data.inpaint_start = None
            elif data.inpaint_start:
                # We were inpainting, but new value is not extreme.
                # Leave old inpainted values in history and forget buffered
                # raw values.
                logger.debug("### return to normal: cancel inpainting")
                data.inpaint_start = None
                data.raw_q = None

            if data.q[0][1] > window_start:
                # Window is not full.  Insert newval into the sorted list.
                logger.debug("insert %d", newval)
                bisect.insort(data.values, newval)
            else:
                # Window is full.  We want to remove the oldest value and
                # insert the new value (which may be raw or inpainted).
                oldest = data.q.popleft()

                if debug:
                    correct = list(data.values)
                    correct.remove(oldest[0])
                    bisect.insort(correct, newval)

                _sortedlist_add_remove(data.values, newval, oldest[0])

                if debug and data.values != correct:
                    raise RuntimeError("bad sort for %s at %d\n"
                        "old = %d, new = %d\nexpect: %s\ngot:     %s" %
                        (key, t, oldest[0], newval,
                        repr(correct), repr(data.values)))

            data.q.append((newval, t))

            # Now that we've inserted the new value, and potentially restored
            # some raw values that were previously inpainted, calculate the
            # result for the new value.
            result = newval / _median(data.values)

            logger.debug("values: %s", repr(data.values))

            yield (key, result, t)
