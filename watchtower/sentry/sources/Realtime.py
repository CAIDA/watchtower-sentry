import sys
import logging
import re
from pytimeseries.tsk.proxy import TskReader
import SentryModule
from sources._Datasource import Datasource


logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "expression":    {"type": "string"},
        "brokers":       {"type": "string"},
        "consumergroup": {"type": "string"},
        "topicprefix":   {"type": "string"},
        "channelname":   {"type": "string"},
    },
    "required": ["expression", "brokers", "consumergroup", "topicprefix",
        "channelname"]
}

class Realtime(Datasource):

    def __init__(self, config, input):
        logger.debug("Realtime.__init__")
        super().__init__(config, add_cfg_schema, logger, input)
        self.expression = config['expression']
        self.tsk_reader = TskReader(
                config['topicprefix'],
                config['channelname'],
                config['consumergroup'],
                config['brokers'],
                None,
                False)
        self.msg_time = None
        self.msgbuf = None
        regex = SentryModule.glob_to_regex(self.expression)
        logger.debug("expression: %s", self.expression)
        logger.debug("regex:      %s", regex)
        self.expression_re = re.compile(bytes(regex, 'ascii'))

    def _msg_cb(self, msg_time, version, channel, msgbuf, msgbuflen):
        if self.msgbuf is None or self.msgbuf != msgbuf:
            label = "new"
        else:
            label = "repeated"
        logger.debug("%s msg: %d bytes at %d", label, msgbuflen, msg_time)
        self.msgbuf = msgbuf
        self.msg_time = msg_time

    def _kv_cb(self, key, val):
        if self.expression_re.match(key):
            self.incoming.append((key, val, self.msg_time))

    def run_reader(self):
        try:
            logger.debug("realtime.run_reader()")
            while not self.done:
                logger.debug("tsk_reader_poll")
                msg = self.tsk_reader.poll(10000)
                if msg is None:
                    break
                if not msg.error():
                    # wait for self.incoming to be empty
                    with self.cond_producable:
                        logger.debug("cond_producable check")
                        while not self.producable:
                            logger.debug("cond_producable.wait")
                            self.cond_producable.wait()
                        self.incoming = []
                        self.producable = False
                        logger.debug("cond_producable.wait DONE")
                    self.tsk_reader.handle_msg(msg.value(),
                        self._msg_cb, self._kv_cb)
                    eof_since_data = 0
                    # tell computation thread that self.incoming is now full
                    with self.cond_consumable:
                        logger.debug("cond_consumable.notify")
                        self.consumable = True
                        self.cond_consumable.notify()
                elif msg.error().code() == \
                        confluent_kafka.KafkaError._PARTITION_EOF:
                    # no new messages, wait a bit and then force a flush
                    eof_since_data += 1
                    if eof_since_data >= 10:
                        break
                else:
                    logging.error("Unhandled Kafka error, shutting down")
                    logging.error(msg.error())
                    with self.cond_consumable:
                        logger.debug("cond_consumable.notify (error)")
                        self.done = "error in realtime reader"
                        self.cond_consumable.notify()
                    break
            logger.debug("realtime done")
            if not self.done:
                with self.cond_consumable:
                    logger.debug("cond_consumable.notify (done=True)")
                    self.done = True
                    self.cond_consumable.notify()
        except:
            e = sys.exc_info()[1]
            logger.critical(type(e).__name__ + ':\n' + traceback.format_exc())
            with self.cond_consumable:
                logger.debug("cond_consumable.notify (exception)")
                self.done = "exception in realtime reader"
                self.cond_consumable.notify()
