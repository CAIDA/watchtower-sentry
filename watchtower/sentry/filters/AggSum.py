"""Filter that sums values across a group of keys.

Configuration parameters ('*' indicates required parameter):
    expression*: (string) A DBATS-style glob pattern that input keys must
        match.  Aggregation groups are identified by the substring(s) that
        match(es) parenthesized subexpression(s).
    groupsize: (integer) Expected number of inputs per group.  Once a group
        has this many values, the output can be generated, even if timeout has
        not been reached.
    timeout*: (integer) Max time (in seconds) to wait for inputs to arrive for
        a group before generating output for the group (unless {droppartial}
        is set).
    droppartial: (boolean) If this is set, then groups with fewer than
        {groupsize} datapoints after {timeout} seconds should be dropped
        rather than generating output.

Input:  (key, value, time)

Output:  (key, value, time)
    key is generated from {expression}, with parenthesized groups replaced
        with the matching part of the input key.
    value is the sum of values for all inputs with the same time and whose key
        maps to the same output key.
    time is the same as input time.
"""
import logging
from collections import OrderedDict
import re
import time
import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "expression":  {"type": "string"},
        "groupsize":   {"type": "integer", "exclusiveMinimum": 0},
        "timeout":     {"type": "integer", "exclusiveMinimum": 0},
        "droppartial": {"type": "boolean"},
    },
    "required": ["expression", "timeout"]
}

class AggSum(SentryModule.SentryModule):

    class _Agginfo:
        def __init__(self, firsttime, count, vsum):
            self.firsttime = firsttime
            self.count = count
            self.vsum = vsum

    def __init__(self, config, gen):
        logger.debug("AggSum.__init__")
        super().__init__(config, logger, gen)
        self.expression = config['expression']
        self.ascii_expression = bytes(self.expression, 'ascii')
        self.timeout = config['timeout']
        self.groupsize = config.get('groupsize', None)
        self.droppartial = config.get('droppartial', False)

        # aggdict stores intermediate results of aggregation.  It's ordered so
        # we can search for stale entries and finalize them.
        # key: (groupid, time)
        #     groupid is a tuple of substrings matched by parens in expression.
        # value: _Agginfo of [firsttime, count, vsum]
        self.aggdict = OrderedDict()

        # old_keys stores the timestamp of the most recent complete or
        # expired data for each group
        self.old_keys = dict()

        regex = SentryModule.glob_to_regex(self.expression)
        logger.debug("expression: %s", self.expression)
        logger.debug("regex:      %s", regex)
        self.expression_re = re.compile(bytes(regex, 'ascii'))

    # replace parens in expression with group id
    # (this could be optimized by pre-splitting expression)
    def groupkey(self, groupid):
        groupkey = self.ascii_expression
        for part in groupid:
            groupkey = re.sub(rb"\([^)]*\)", part, groupkey, count=1)
        return groupkey

    def run(self):
        logger.debug("AggSum.run()")
        for entry in self.gen():
            logger.debug("AG: %s", entry)
            key, value, t = entry
            match = self.expression_re.match(key)
            if not match:
                continue
            groupid = match.groups()
            aggkey = (groupid, t)

            now = time.time()

            if aggkey in self.aggdict:
                agginfo = self.aggdict[aggkey]
            elif groupid in self.old_keys and t < self.old_keys[groupid]:
                logger.error("unexpected data for old aggregate (%r, %d) "
                    "from %s", groupid, t, key)
                continue
            else:
                agginfo = AggSum._Agginfo(firsttime=now, count=0, vsum=0)
                self.aggdict[aggkey] = agginfo

            agginfo.count += 1
            if value is not None:
                agginfo.vsum += value

            logger.debug("k=%r, v=%r, t=%d; count=%d, vsum=%s",
                groupid, value, t, agginfo.count, agginfo.vsum)

            if self.groupsize and agginfo.count == self.groupsize:
                groupkey = self.groupkey(groupid)
                logger.debug("reached groupsize for %r after %ds",
                    aggkey, now - agginfo.firsttime)
                yield (groupkey, agginfo.vsum, t)
                if groupid not in self.old_keys or t < self.old_keys[groupid]:
                    self.old_keys[groupid] = t
                del self.aggdict[aggkey]

            expiry_time = now - self.timeout
            while self.aggdict:
                aggkey, agginfo = next(iter(self.aggdict.items()), None)
                if agginfo.firsttime > expiry_time:
                    break
                self.aggdict.popitem(False)
                groupid, t = aggkey
                groupkey = self.groupkey(groupid)
                logger.debug("reached timeout for %r after %d entries",
                    aggkey, agginfo.count)
                if not self.droppartial:
                    yield (groupkey, agginfo.vsum, t)
                if groupid not in self.old_keys or t > self.old_keys[groupid]:
                    self.old_keys[groupid] = t

        logger.debug("AggSum.run() done")
