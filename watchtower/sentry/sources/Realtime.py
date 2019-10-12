"""Source that reads (k,v,t) tuples from a live TSK (Time Series Kafka) service.

Configuration parameters ('*' indicates required parameter):
    expression*: (string) A DBATS-style glob pattern that input keys must
        match.
    brokers*: (string) Comma-separated list of kafka brokers.
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
        "brokers":       {"type": "string"},
        "consumergroup": {"type": "string"},
        "topicprefix":   {"type": "string"},
        "channelname":   {"type": "string"},
    },
    "required": ["expressions", "brokers", "consumergroup", "topicprefix",
        "channelname"]
}

class Realtime(Datasource):

    def __init__(self, config, gen, ctx):
        logger.debug("Realtime.__init__")
        super().__init__(config, logger, gen, ctx)
        self.expressions = config['expressions']
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

    def _msg_cb(self, msg_time, version, channel, msgbuf, msgbuflen):
        if self.msg_time is None or msg_time > self.msg_time:
            logger.info("TSK msg time %d" % msg_time)
        self.msg_time = msg_time

    def _kv_cb(self, key, val):
        for regex in self.expression_res:
            if regex.match(key):
                self.incoming.append((key, val, self.msg_time))
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
