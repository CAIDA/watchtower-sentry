from os.path import expanduser
from datetime import datetime
from sqlalchemy import (MetaData, Table, Column, ForeignKey,
                        Integer, String, DateTime,
                        create_engine)
from graphite_beacon.handlers import AbstractHandler, LOGGER


meta = MetaData()
t_violation = Table('violation', meta,
                    Column('id', Integer, primary_key=True),
                    Column('method', String, nullable=False),
                    Column('rule', String, nullable=False),
                    # value that violates the rule
                    Column('value', String, nullable=False)) 
t = Table('alert', meta,
          Column('id', Integer, primary_key=True),
          Column('name', String, nullable=False),
          Column('time', DateTime, nullable=False),
          Column('level', String, nullable=False),
          Column('query', String, nullable=False),
          Column('reason', String, nullable=False),
          Column('violation_id', Integer, ForeignKey(t_violation.c.id)))


class DatabaseHandler(AbstractHandler):
    """Handle events to database"""

    name = 'database'

    defaults = {
        'url': 'sqlite:///' + expanduser('~/graphite-beacon/alerts.db'),
        'engine_params': {},
    }

    def init_handler(self):
        self.engine = create_engine(self.options['url'],
                                    # echo=self.reactor.options['logging'] == 'debug',
                                    **self.options['engine_params'])
        meta.create_all(self.engine)

    def notify(self, level, *args, **kwargs):
        LOGGER.debug('Handler (%s) %s', self.name, level)

        LOGGER.debug('Insert record into %s', self.options['url'])
        self._record(level, *args, **kwargs)

    def get_reason(self, level, target, value):
        if level == 'normal':
            return '{} is back to normal'.format(target)
        elif target == 'loading':
            return value
        else:
            return '({}) violates rules. Current value: {}'.format(target, value)

    def _record(self, level, alert, value, target=None, ntype=None, rule=None):
        with self.engine.connect() as conn:
            if target != 'loading':
                ins = t_violation.insert() \
                    .values(method=alert.method, rule=rule['raw'], value=value)
                result = conn.execute(ins)
                [violation_id] = result.inserted_primary_key
            else:
                violation_id = None

            ins = t.insert().values(name=alert.name,
                                    time=datetime.now(),
                                    level=level,
                                    query=alert.query,
                                    reason=self.get_reason(level, target, value),
                                    violation_id=violation_id)
            conn.execute(ins)

