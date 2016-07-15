from os.path import expanduser
from datetime import datetime
from sqlalchemy import (MetaData, Table, Column,
                        Integer, String, DateTime, Float,
                        ForeignKey, Sequence,
                        create_engine)
from sqlalchemy.engine.url import URL
from graphite_beacon.handlers import AbstractHandler, LOGGER


meta = MetaData()
t_violation = Table('watchtower_violation', meta,
                    Column('id', Integer, Sequence(
                        'violation_id_seq'), primary_key=True),
                    # the actual query if * is used
                    Column('target', String, nullable=False),
                    Column('method', String, nullable=False),
                    Column('rule', String, nullable=False),
                    # value that violates the rule
                    Column('value', Float, nullable=False))
t = Table('watchtower_alert', meta,
          Column('id', Integer, Sequence('alert_id_seq'), primary_key=True),
          Column('name', String, nullable=False),
          Column('time', DateTime, nullable=False),
          Column('level', String, nullable=False),
          Column('query', String, nullable=False),
          Column('type', String, nullable=False),
          Column('description', String, nullable=False),
          Column('violation_id', Integer, ForeignKey(t_violation.c.id)))


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
    }

    def init_handler(self):
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

    def notify(self, level, *args, **kwargs):
        LOGGER.debug('Handler (%s) %s', self.name, level)

        LOGGER.debug('Insert record into %s', self.url)
        self._record(level, *args, **kwargs)

    def _record(self, level, alert, value, target=None, ntype=None, rule=None):
        with self.engine.connect() as conn:
            if ntype == alert.source:
                cond = rule['raw'].split(':')[-1].strip()
                ins = t_violation.insert().values(method=alert.method,
                                                  target=target,
                                                  rule=cond,
                                                  value=float(value))
                result = conn.execute(ins)
                [violation_id] = result.inserted_primary_key
                desc = 'Rule violated'
            else:
                violation_id = None
                desc = value

            ins = t.insert().values(name=alert.name,
                                    time=datetime.now(),
                                    level=level,
                                    query=alert.query,
                                    type=ntype,
                                    description=desc,
                                    violation_id=violation_id)
            conn.execute(ins)
