import logging
from collections import OrderedDict
import re
import time
import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "expression":  {"type": "string"},
        "groupsize":   {"type": "number"},
        "timeout":     {"type": "number"},
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
        super().__init__(config, add_cfg_schema, logger, gen)
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

        self.complete_keys = dict()

        regex = SentryModule.glob_to_regex(self.expression)
        logger.debug("expression: %s", self.expression)
        logger.debug("regex:      %s", regex)
        self.expression_re = re.compile(bytes(regex, 'ascii'))

    # replace parens in expression with group id
    # (this could be optimized by pre-splitting expression)
    def groupkey(self, groupid):
        groupkey = self.ascii_expression
        for part in groupid:
            logger.debug('part: %s', str(part))
            groupkey = re.sub(rb"\([^)]*\)", part, groupkey)
        return groupkey

    def run(self):
        logger.debug("AggSum.run()")
        for entry in self.gen():
            logger.debug("AG: %s", str(entry))
            key, value, t = entry
            match = self.expression_re.match(key)
            if not match:
                continue
            groupid = match.groups()
            aggkey = (groupid, t)

            if aggkey in self.complete_keys:
                logger.error("unexpected data for complete aggregate (%s, %d)",
                    self.groupkey(groupid), t)
                continue

            now = time.time()

            if aggkey not in self.aggdict:
                agginfo = AggSum._Agginfo(firsttime=now, count=0, vsum=0)
                self.aggdict[aggkey] = agginfo
            else:
                agginfo = self.aggdict[aggkey]
            agginfo.count += 1
            if value is not None:
                agginfo.vsum += value

            logger.debug("k=%s, v=%s, t=%d; count=%d, vsum=%s",
                str(groupid), str(value), t, agginfo.count, agginfo.vsum)

            if self.groupsize and agginfo.count == self.groupsize:
                groupkey = self.groupkey(groupid)
                logger.debug("reached groupsize for %s after %ds",
                    str(aggkey), now - agginfo.firsttime)
                yield (groupkey, agginfo.vsum, t)
                self.complete_keys[groupkey] = True
                del self.aggdict[aggkey]

            expiry_time = now - self.timeout
            while self.aggdict:
                first_aggkey = next(iter(self.aggdict))
                if self.aggdict[first_aggkey].firsttime > expiry_time:
                    break
                aggkey, agginfo = self.aggdict.popitem(False)
                groupkey = self.groupkey(aggkey[0])
                logger.debug("reached timeout for %s after %d entries",
                    str(aggkey), agginfo.count)
                if not self.droppartial:
                    yield (groupkey, agginfo.vsum, aggkey[1])
                self.complete_keys[groupkey] = True

            # TODO: prune very old entries from complete_keys

        logger.debug("AggSum.run() done")
