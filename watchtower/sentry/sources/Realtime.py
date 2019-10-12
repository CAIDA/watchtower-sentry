"""Source that reads (k,v,t) tuples from a live TSK (Time Series Kafka) service.

Configuration parameters ('*' indicates required parameter):
    expression*: (string) A DBATS-style glob pattern that input keys must
        match.
    brokers*: (string) Comma-separated list of kafka brokers.
    interval: (number) Expected time between data points
    timeout: (number) Seconds to wait for new data to arrive before using buffer
    consumergroup*: (string) Kafka consumer group.
    topicprefix*: (string) Kafka topic prefix.
    channelname*: (string) Kafka channel name.

Output context variables: expression

Output:  (key, value, time)
   Output will include some amount (perhaps several days worth) of buffered
   data prior to the near-realtime data.
"""

import confluent_kafka
import logging
import re
import time
from pytimeseries.tsk.proxy import TskReader
from .. import SentryModule
from ._Datasource import Datasource


logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "expressions": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1
        },
        "interval": {"type": "number"},
        "timeout": {"type": "number"},
        "brokers": {"type": "string"},
        "consumergroup": {"type": "string"},
        "topicprefix":   {"type": "string"},
        "channelname":   {"type": "string"},
    },
    "required": ["expressions", "interval", "brokers", "consumergroup",
                 "topicprefix", "channelname"]
}

class Realtime(Datasource):

    def __init__(self, config, gen, ctx):
        logger.debug("Realtime.__init__")
        super().__init__(config, logger, gen, ctx)
        self.expressions = config['expressions']
        self.interval = config['interval']
        self.timeout = config['timeout']
        self.tsk_reader = TskReader(
                config['topicprefix'],
                config['channelname'],
                config['consumergroup'],
                config['brokers'],
                commit_offsets=True
        )
        self.msg_time = None
        regexes = [SentryModule.glob_to_regex(exp) for exp in self.expressions]
        logger.debug("expressions: %s", self.expressions)
        logger.debug("regexes:     %s", regexes)
        self.expression_res = [re.compile(bytes(regex, 'ascii')) for regex in regexes]
        self.last_key_time = {}  # last_key_time[key] = ts
        self.kv_buf = {}         # kv_buf[key][ts] = val
        self.kv_buf_timer = {}  # kv_buf_timer[key] = last_append_ts

    def _handle_kv(self, key, val):
        # special case to handle first time we see a key
        if key not in self.last_key_time:
            self.last_key_time[key] = None
            self.kv_buf[key] = {}
            self.kv_buf_timer[key] = None

        # precompute some oft used values
        lkt = self.last_key_time[key]
        now = time.time()
        kbt = self.kv_buf_timer[key]
        # decide if we'll check the buffer regardless of what happens
        # i.e., because it has been a while since we last saw a value
        # for this key
        force_buffer_use = (kbt is not None) and ((kbt + self.timeout) <= now)
        # we need to check the buffer if we're going to force its use
        check_buffer = force_buffer_use

        if lkt is None or self.msg_time == lkt + self.interval:
            # this is exactly the timestamp we expect to see for this key,
            # simply append it and update tracking info
            self.incoming.append((key, val, self.msg_time))
            self.last_key_time[key] = lkt = self.msg_time
            self.kv_buf_timer[key] = now
            force_buffer_use = False
            check_buffer = True
        elif self.msg_time > lkt + self.interval:
            # future data point, buffer it
            self.kv_buf[key][self.msg_time] = val
        # else:
        # (self.msg_time <= lkt), too old, drop

        if check_buffer:
            # see if there are things in the buffer we can return
            buf_times = sorted(self.kv_buf[key].keys())
            for bt in buf_times:
                if force_buffer_use or bt == lkt + self.interval:
                    self.incoming.append((key, self.kv_buf[key][bt], bt))
                    self.last_key_time[key] = lkt = bt
                    self.kv_buf_timer[key] = now
                    del self.kv_buf[key][bt]
                    force_buffer_use = False
                else:
                    break

    def _msg_cb(self, msg_time, version, channel, msgbuf, msgbuflen):
        if self.msg_time is None or msg_time > self.msg_time:
            logger.info("TSK msg time %d" % msg_time)
        self.msg_time = msg_time

    def _kv_cb(self, key, val):
        for regex in self.expression_res:
            if regex.match(key):
                self._handle_kv(key, val)
                return

    def reader_body(self):
        logger.debug("realtime.run_reader()")
        while not self.done:
            logger.debug("tsk_reader_poll")
            msg = self.tsk_reader.poll(10000)
            if msg is None:
                logger.debug("TSK msg: None")
                break
            if not msg.error():
                # wait for self.incoming to be empty
                logger.debug("TSK msg: non-error")
                with self.cond_producable:
                    logger.debug("cond_producable check")
                    while not self.producable and not self.done:
                        logger.debug("cond_producable.wait")
                        self.cond_producable.wait()
                    self.incoming = []
                    self.producable = False
                    logger.debug("cond_producable.wait DONE")
                if self.done: # in case consumer stopped early
                    break
                self.tsk_reader.handle_msg(msg.value(),
                    self._msg_cb, self._kv_cb)
                # tell computation thread that self.incoming is now full
                with self.cond_consumable:
                    logger.debug("cond_consumable.notify")
                    self.consumable = True
                    self.cond_consumable.notify()
            elif msg.error().code() == \
                    confluent_kafka.KafkaError._PARTITION_EOF:
                # no new messages
                logger.debug("TSK msg: PARTITION_EOF")
            else:
                logger.error("Unhandled Kafka error, shutting down")
                logger.error(msg.error())
                with self.cond_consumable:
                    logger.debug("cond_consumable.notify (error)")
                    self.reader_exc = RuntimeError("kafka: %s" % msg.error())
                    self.done = True
                    self.cond_consumable.notify()
                break
        logger.debug("realtime done")
