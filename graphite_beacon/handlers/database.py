from os.path import expanduser
from sqlalchemy import (MetaData, Table, Column,
                        Integer, String, DateTime, Float,
                        ForeignKey, Sequence,
                        create_engine)
from sqlalchemy.engine.url import URL
from graphite_beacon.handlers import AbstractHandler, LOGGER
from graphite_beacon.utils import extract_condition


class DatabaseHandler(AbstractHandler):
    """Handle events to database"""

    name = 'database'

    defaults = {
        'drivername': 'sqlite',
        'username': None,
        'password': None,
        'host': expanduser('~/graphite-beacon/alerts.db'),
        'port': None,
        'databasename': None,
        'engine_params': {},
        'alert_table_name': 'watchtower_alert',
        'violation_table_name': 'watchtower_violation',
        'error_table_name': 'watchtower_error',
    }

    def init_handler(self):
        meta = MetaData()
        self.t = Table(self.options['alert_table_name'], meta,
            Column('id', Integer, Sequence('graphitebeacon_alert_id_seq'), primary_key=True),
            Column('name', String, nullable=False),
            Column('time', DateTime, nullable=False),
            Column('level', String, nullable=False),
            Column('method', String, nullable=False),
            Column('expression', String, nullable=False))  # Internal message which is irrelevant to time series
        # Violations are all occurences of rule violations grouped by level under an alert
        self.t_violation = Table(self.options['violation_table_name'], meta,
            Column('id', Integer, Sequence('graphitebeacon_violation_id_seq'), primary_key=True),
            Column('target', String, nullable=False),  # The actual query if * is used in expression
            Column('condition', String),
            Column('value', Float),  # value that violates the rule
            Column('alert_id', Integer, ForeignKey(self.t.c.id)))
        self.t_error = Table(self.options['error_table_name'], meta,
            Column('id', Integer, Sequence('graphitebeacon_error_id_seq'), primary_key=True),
            Column('name', String, nullable=False),
            Column('time', DateTime, nullable=False),
            Column('expression', String, nullable=False),
            Column('type', String, nullable=False), # usually 'common'
            Column('message', String, nullable=False))

        engine_options = [self.options[n] for n in (
            'drivername', 'username', 'password', 'host', 'port', 'databasename')]
        host = expanduser(engine_options[3])
        if 'sqlite' in engine_options[0] and host:
            host = '/' + host
        engine_options[3] = host

        self.url = str(URL(*engine_options))
        LOGGER.debug('Database engine url: %s', self.url)
        
        self.engine = create_engine(self.url, **self.options['engine_params'])
        meta.create_all(self.engine)

    def notify(self, level, alert, value, target=None, ntype=None, rule=None):
        LOGGER.debug('Handler (%s) %s', self.name, level)
        LOGGER.debug('Insert record into %s', self.url)

        if ntype == alert.source:
            alert_id = self.insert_alert(level, alert, alert.get_time_with_offset())
            
            self.insert_violations([{
                'alert_id': alert_id,
                'target': target,
                'value': value,
                'condition': extract_condition(rule)
            }])
        else:
            # Usually means internal error or time-series-unrelated events
            self.insert_error(alert, ntype, value)

    def notify_batch(self, level, alert, ntype, data):
        """Insert an alert with multiple targets"""
        # Should be called only when there is no error
        LOGGER.debug('Handler (%s-batch) %s', self.name, level)
        LOGGER.debug('Insert records into %s', self.url)

        if ntype == alert.source:
            alert_id = self.insert_alert(level, alert, data[0][0].get_end_time())
            
            self.insert_violations([{
                'alert_id': alert_id,
                'target': record.target,
                'value': value,
                'condition': extract_condition(rule)
            } for record, value, rule in data])
        else:
            raise RuntimeError('Call notify() to insert error')

    def insert_alert(self, level, alert, time):
        with self.engine.connect() as conn:
            ins = self.t.insert().values(
                name=alert.name,
                time=time,
                level=level,
                expression=alert.current_query,
                method=alert.method)

            result = conn.execute(ins)
            [alert_id] = result.inserted_primary_key
            return alert_id

    def insert_violations(self, data):
        """
        :type data: [{
            'alert_id': ...
            'target': ...,
            'value': ...,
            'condition': ...
        }]
        """
        with self.engine.connect() as conn:
            ins = self.t_violation.insert().values(data)
            conn.execute(ins)

    def insert_error(self, alert, ntype, message):
        with self.engine.connect() as conn:
            ins = self.t_error.insert().values(
                name=alert.name,
                time=alert.get_time_with_offset(),
                expression=alert.current_query,
                type=ntype or 'Unknown', # should be undefined behavior. Just in case
                message=message)

            conn.execute(ins)
