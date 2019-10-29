"""Sink that detects extreme values and sends alert objects to a kafka cluster.

Configuration parameters ('*' indicates required parameter):
    fqid*: (string) Unique identifier for this data source.
    name*: (string) Human-readable name for this data source.
    min: (number <1.0) Generate alert if value falls below this value.
    max: (number >1.0) Generate alert if value rises above this value.
    minduraton: (number) Only generate alerts for events at least this long.
    brokers*: (string) Comma-separated list of kafka brokers.
    topic*: (string) Kafka topic prefix.

    At least one of {min} or {max} is required.

Input context variables: expression*, method*

Input:  (key, value, time)

Sink result:  alert objects sent to kafka cluster.
"""

import json
import logging
import confluent_kafka
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "fqid":        {"type": "string"},
        "name":        {"type": "string"},
        "min":         {"type": "number", "exclusiveMaximum": 1.0},
        "max": {"type": "number", "exclusiveMinimum": 1.0},
        "minduration": {"type": "number"},
        "brokers":     {"type": "string"},
        "topic": {"type": "string"},
        "disable":     {"type": "boolean"}, # for debugging
    },
    "required": ["fqid", "name", "brokers", "topic"],
    "oneOf": [{"required": ["min"]}, {"required": ["max"]}]
}

class AlertKafka(SentryModule.Sink):
    def __init__(self, config, gen, ctx):
        logger.debug("AlertKafka.__init__")
        super().__init__(config, logger, gen)
        self.fqid = config['fqid']
        self.name = config['name']
        self.brokers = config['brokers']
        self.topic = config['topic']
        self.min = config.get('min', None)
        self.max = config.get('max', None)
        self.minduration = config.get('minduration', None)
        self.disable = config.get('disable', False)
        self.condition_label = [
            "< %r" % self.min,   # -1
            "normal",            # 0
            "> %r" % self.max    # 1
        ]

        self.alert_status = dict()  # alert_status[key] = [0,1,-1]
        self.alert_state = dict()  # alert_state[key] = (time, value, actual, predicted)
        kp_cfg = {
            'bootstrap.servers': self.brokers,
        }
        self.kproducer = confluent_kafka.Producer(kp_cfg)
        try:
            self.method = ctx['method']
        except KeyError as e:
            raise RuntimeError('%s expects ctx[%s] to be set by a previous '
                'module' % (self.modname, str(e)))

    def _produce_alert(self, status, t, key, value, actual, predicted):
        # Cram our alert data into the watchtower-alert legacy format
        record = {
            "fqid": self.fqid,
            "name": self.name,
            "level": "critical" if status != 0 else "normal",
            "time": t,
            "expression": None,
            "history_expression": None,
            "method": self.method,
            "violations": [{
                "expression": str(key, 'ascii'),
                "condition": self.condition_label[status + 1],
                "value": value if actual is None else actual,
                "history_value": predicted,  # may be None
                "history": None,
                "time": t,
            }],
        }

        # Asynchonously produce a message.  The delivery report
        # callback will be triggered from poll() above, or flush()
        # below, when the message has been successfully delivered or
        # failed permanently.
        msg = json.dumps(record, separators=(',', ':'))
        if self.disable:
            print(msg)
        else:
            self.kproducer.produce(self.topic,
                                   value=bytes(msg, 'ascii'),
                                   key=key,
                                   on_delivery=self.kp_delivery_report)

    def run(self):
        logger.debug("AlertKafka.run()")
        for entry in self.gen():
            logger.debug("AK: %s", str(entry))
            key, value, t = entry

            # Trigger any available delivery report callacks from previous
            # produce() calls
            self.kproducer.poll(0)

            if isinstance(value, tuple):
                (value, actual, predicted) = value
            else:
                actual = None
                predicted = None

            if value is None:
                continue

            if key not in self.alert_status:
                self.alert_status[key] = 0
            if self.min is not None and value < self.min:
                # "too-low" alert
                alert_status = -1
            elif self.max is not None and value > self.max:
                # "too-high" alert
                alert_status = 1
            else:
                # "normal" alert
                alert_status = 0
            if alert_status != self.alert_status[key]:
                self.alert_status[key] = alert_status
                self.alert_state[key] = (t, value, actual, predicted)
                # only produce alert if minduration is not set
                if self.minduration is None:
                    self._produce_alert(alert_status, t, key, value,
                                        actual, predicted)
            elif alert_status != 0 and self.minduration is not None \
                    and key in self.alert_state:
                # ongoing non-normal event, check minduration
                (init_t, init_v, init_a, init_p) = self.alert_state[key]
                if init_t + self.minduration >= t:
                    self._produce_alert(alert_status, init_t, key, init_v,
                                        init_a, init_p)
                    del self.alert_state[key]

        self.kproducer.flush()
        logger.debug("AlertKafka.run() done")

    def kp_delivery_report(self, err, msg):
        if err is not None:
            logger.error("message delivery failed: %r", err)
        else:
            logger.debug("message delivered to %r [%r]",
                msg.topic(), msg.partition())
