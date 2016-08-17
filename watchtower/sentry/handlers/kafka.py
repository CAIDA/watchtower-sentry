import calendar
import json
import pykafka

from watchtower.sentry.handlers import AbstractHandler, LOGGER
from watchtower.sentry.utils import extract_condition


class KafkaHandler(AbstractHandler):
    """Publish events to Kafka"""

    name = 'kafka'

    defaults = {
        'brokers': 'localhost:9092',
        'topic_prefix': 'watchtower',
        'alert_topic': 'alerts',
        'error_topic': 'errors',
    }

    def _connect_topic(self, topic_name):
        full_name = "%s-%s" % (self.options['topic_prefix'], topic_name)
        LOGGER.debug("Connecting topic %s" % full_name)
        return self.kc.topics[full_name.encode("ascii")].get_sync_producer()

    def init_handler(self):
        # connect to kafka
        self.kc = pykafka.KafkaClient(hosts=self.options['brokers'])
        # create topic handles
        self.alert_t = self._connect_topic(self.options['alert_topic'])
        self.error_t = self._connect_topic(self.options['error_topic'])

    def notify(self, level, alert, value, target=None, ntype=None, rule=None):
        LOGGER.debug('Handler (%s) %s', self.name, level)

        if ntype == alert.source:
            self.insert_alert(level, alert,
                              alert.get_time_with_offset(),
                              [self.create_violation(alert, target,
                                                     value, rule)])
        else:
            # Usually means internal error or time-series-unrelated events
            self.insert_error(alert, ntype, value)

    def create_violation(self, alert, target, value, rule):
        return {
            'target': target,
            'value': value,
            'condition': extract_condition(rule),
            'history': list(alert.history[target]),
            'history_val': alert.get_history_val(target)
        }

    def notify_batch(self, level, alert, ntype, data):
        """Insert an alert with multiple targets"""
        # Should be called only when there is no error
        LOGGER.debug('Handler (%s-batch) %s', self.name, level)

        if ntype == alert.source:
            violations = [self.create_violation(alert, record.target,
                                                value, rule)
                          for record, value, rule in data]
            self.insert_alert(level, alert, data[0][0].get_end_time(),
                              violations)
        else:
            raise RuntimeError('Call notify() to insert error')

    def insert_alert(self, level, alert, time, violations):
        self.alert_t.produce(json.dumps({
            'name': alert.name,
            'time': calendar.timegm(time.timetuple()),
            'level': level,
            'expression': alert.current_query,
            'method': alert.method,
            'violations': violations,
        }))

    def insert_error(self, alert, ntype, message):
        self.error_t.produce(json.dumps({
            'name': alert.name,
            'time': calendar.timegm(alert.get_time_with_offset().timetuple()),
            'expression': alert.current_query,
            'type': ntype or 'Unknown',  # should be undefined behavior.
            'message': message
        }))
