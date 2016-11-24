import calendar

from watchtower.sentry.handlers import AbstractHandler, LOGGER
from watchtower.sentry.utils import extract_condition
import watchtower.alert


class KafkaHandler(AbstractHandler):
    """Publish events to Kafka"""

    name = 'kafka'

    defaults = {
        'brokers': 'localhost:9092',
        'topic_prefix': 'watchtower',
        'alert_topic': 'alerts',
        'error_topic': 'errors',
    }

    def init_handler(self):
        # connect to kafka
        self.producer = \
            watchtower.alert.Producer(brokers=self.options['brokers'],
                                      topic_prefix=self.options['topic_prefix'],
                                      alert_topic=self.options['alert_topic'],
                                      error_topic=self.options['error_topic'])

    def notify(self, level, alert, value, target=None, ntype=None, rule=None):
        LOGGER.debug('Handler (%s) %s', self.name, level)

        if ntype == alert.source:
            raise RuntimeError('Call notify_batch() to insert violations with accurate time')
        else:
            # Usually means internal error or time-series-unrelated events
            self.insert_error(alert, ntype, value)

    @staticmethod
    def create_violation(alert, record, value, rule):
        return watchtower.alert.Violation(
            time=calendar.timegm(alert.get_record_time(record).timetuple()),
            expression=record.target,
            value=value,
            condition=extract_condition(rule),
            history=list(alert.history[record.target]),
            history_value=alert.get_history_val(record.target)
        )

    def notify_batch(self, level, alert, ntype, data):
        """Insert an alert with multiple targets"""
        # Should be called only when there is no error
        # LOGGER.debug('Handler (%s-batch) %s', self.name, level)

        if ntype == alert.source:
            violations = [self.create_violation(alert, record, value, rule)
                          for record, value, rule in data]
            self.insert_alert(level, alert, alert.get_last_query_time(), violations)
        else:
            raise RuntimeError('Call notify() to insert error without violations data')

    def insert_alert(self, level, alert, time, violations):
        # LOGGER.debug('Alert expression: %s' % alert.current_query)
        self.producer.produce_alert(
            watchtower.alert.Alert(
                fqid=alert.fqid,
                name=alert.name,
                time=calendar.timegm(time.timetuple()),
                level=level,
                expression=alert.current_query,
                history_expression=alert.history_query,
                method=alert.method,
                violations=violations
            )
        )

    def insert_error(self, alert, ntype, message):
        self.producer.produce_error(watchtower.alert.Error(
            fqid=alert.fqid,
            name=alert.name,
            time=calendar.timegm(alert.get_last_query_time().timetuple()),
            expression=alert.current_query,
            history_expression=alert.history_query,
            type=ntype or 'Unknown',  # Should be undefined behavior
            message=message
        ))
