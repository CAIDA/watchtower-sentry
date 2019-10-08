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
from .. import SentryModule

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
        """Intermediate results of aggregation"""
        def __init__(self, first_seen, count, vsum):
            self.first_seen = first_seen
            self.count = count
            self.vsum = vsum

    def __init__(self, config, gen, ctx):
        logger.debug("AggSum.__init__")
        super().__init__(config, logger, gen)
        self.expression = config['expression']
        self.ascii_expression = bytes(self.expression, 'ascii')
        self.timeout = config['timeout']
        self.groupsize = config.get('groupsize', None)
        self.droppartial = config.get('droppartial', False)

        # agg_by_group is a 2-level dict that stores agginfo keyed by groupid,
        # then timestamp, so it's easy to find all agginfos for a given group.
        self.agg_by_group = dict()

        # agg_by_seen stores agginfo keyed by (groupid, t) and ordered by
        # first_seen, so it's easy to find all stale agginfos.
        self.agg_by_seen = OrderedDict()

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

            agginfo = None
            if groupid in self.agg_by_group:
                if t in self.agg_by_group[groupid]:
                    agginfo = self.agg_by_group[groupid][t]
            elif groupid in self.old_keys and t < self.old_keys[groupid]:
                logger.error("unexpected data for old aggregate (%r, %d) "
                    "from %s", groupid, t, key)
                continue
            else:
                self.agg_by_group[groupid] = dict()
            if not agginfo:
                agginfo = AggSum._Agginfo(first_seen=now, count=0, vsum=0)
                self.agg_by_group[groupid][t] = agginfo
                self.agg_by_seen[(groupid, t)] = agginfo

            agginfo.count += 1
            if value is not None:
                agginfo.vsum += value

            logger.debug("k=%r, v=%r, t=%d; count=%d, vsum=%s",
                groupid, value, t, agginfo.count, agginfo.vsum)

            if self.groupsize and agginfo.count == self.groupsize:
                groupkey = self.groupkey(groupid)
                logger.debug("reached groupsize for %r after %ds",
                    aggkey, now - agginfo.first_seen)
                del self.agg_by_group[groupid][t]
                del self.agg_by_seen[(groupid, t)]

                # Assume that data for a given key will always arrive in time
                # order.  Then, if we have all the data for a group at time t,
                # but we are missing data for that group at some earlier time,
                # we can assume that old data will never arrive, and we can
                # generate the old result.  (Note: if we didn't do this, then
                # in order to preserve timestamp order for this group's
                # results, we would have to defer outputting this aggregate
                # until older aggregates for this group time out.)
                oldtimes = sorted([oldtime for oldtime in
                    self.agg_by_group[groupid].keys() if oldtime < t])
                for oldtime in oldtimes:
                    old_agginfo = self.agg_by_group[groupid][oldtime]
                    logger.debug("giving up on %r with %d/%d items",
                        (groupid, oldtime), old_agginfo.count,
                        self.groupsize)
                    yield (groupkey, old_agginfo.vsum, oldtime)
                    del self.agg_by_group[groupid][oldtime]
                    del self.agg_by_seen[(groupid, oldtime)]

                yield (groupkey, agginfo.vsum, t)
                if groupid not in self.old_keys or t < self.old_keys[groupid]:
                    self.old_keys[groupid] = t

            expiry_time = now - self.timeout
            while self.agg_by_seen:
                aggkey, agginfo = next(iter(self.agg_by_seen.items()), None)
                if agginfo.first_seen > expiry_time:
                    break
                self.agg_by_seen.popitem(False)
                groupid, t = aggkey
                del self.agg_by_group[groupid][t]
                groupkey = self.groupkey(groupid)
                logger.debug("reached timeout for %r with %d/%d items",
                    aggkey, agginfo.count, self.groupsize)
                if not self.droppartial:
                    yield (groupkey, agginfo.vsum, t)
                if groupid not in self.old_keys or t > self.old_keys[groupid]:
                    self.old_keys[groupid] = t

        logger.debug("AggSum.run() done")
