"""Sink that detects extreme values and sends alert objects to a kafka cluster.

Configuration parameters ('*' indicates required parameter):
    fqid*: (string) Unique identifier for this data source.
    name*: (string) Human-readable name for this data source.
    min: (number <1.0) Generate alert if value falls below this value.
    max: (number >1.0) Generate alert if value rises above this value.
    brokers*: (string) Comma-separated list of kafka brokers.
    topicprefix*: (string) Kafka topic prefix.

    At least one of {min} or {max} is required.

Input context variables: expression*, method*

Input:  (key, value, time)

Sink result:  alert objects sent to kafka cluster.
"""

import json
import logging
import confluent_kafka
import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "fqid":        {"type": "string"},
        "name":        {"type": "string"},
        "min":         {"type": "number", "exclusiveMaximum": 1.0},
        "max":         {"type": "number", "exclusiveMinimum": 1.0},
        "brokers":     {"type": "string"},
        "topicprefix": {"type": "string"},
    },
    "required": ["fqid", "name", "brokers", "topicprefix"],
    "oneOf": [{"required": ["min"]}, {"required": ["max"]}]
}

class AlertKafka(SentryModule.Sink):
    def __init__(self, config, gen, ctx):
        logger.debug("AlertKafka.__init__")
        super().__init__(config, logger, gen)
        self.fqid = config['fqid']
        self.name = config['name']
        self.brokers = config['brokers']
        self.topicprefix = config['topicprefix']
        self.min = config.get('min', None)
        self.max = config.get('max', None)
        self.condition_label = [
            "< %r" % self.min,   # -1
            "normal",            # 0
            "> %r" % self.max    # 1
        ]

        self.topic = "%s-alerts" % (self.topicprefix)
        self.alert_status = dict()
        kp_cfg = {
            'bootstrap.servers': self.brokers,
        }
        self.kproducer = confluent_kafka.Producer(kp_cfg)
        try:
            self.expression = ctx['expression']
            self.method = ctx['method']
        except KeyError as e:
            raise RuntimeError('%s expects ctx[%s] to be set by a previous '
                'module' % (self.modname, str(e)))

    def run(self):
        logger.debug("AlertKafka.run()")
        for entry in self.gen():
            logger.debug("AK: %s", str(entry))
            key, value, t = entry

            # Trigger any available delivery report callacks from previous
            # produce() calls
            self.kproducer.poll(0)

            if value is None:
                continue
            if key not in self.alert_status:
                self.alert_status[key] = 0
            if self.min is not None and value < self.min:
                alert_status = -1
            elif self.max is not None and value > self.max:
                alert_status = 1
            else:
                alert_status = 0
            if alert_status != self.alert_status[key]:
                self.alert_status[key] = alert_status
                # Cram our alert data into the watchtower-alert legacy format
                record = {
                    "fqid": self.fqid,
                    "name": self.name,
                    "level": "critical" if alert_status != 0 else "normal",
                    "time": t,
                    "expression": self.expression,
                    "history_expression": None,
                    "method": self.method,
                    "violations": [{
                        "expression": str(key, 'ascii'),
                        "condition": self.condition_label[alert_status+1],
                        "value": value,
                        "history_value": None,
                        "history": None,
                        "time": t,
                    }],
                }

                # Asynchonously produce a message.  The delivery report
                # callback will be triggered from poll() above, or flush()
                # below, when the message has been successfully delivered or
                # failed permanently.
                msg = json.dumps(record, separators={',',':'})
                self.kproducer.produce(self.topic,
                    value=bytes(msg, 'ascii'),
                    key=key,
                    on_delivery=self.kp_delivery_report)
                print(json.dumps(record))

        self.kproducer.flush()
        logger.debug("AlertKafka.run() done")

    def kp_delivery_report(self, err, msg):
        if err is not None:
            logger.error("message delivery failed: {}".format(err))
        else:
            logger.debug("message delivered to {} [{}]".format(
                msg.topic(), msg.partition()))
