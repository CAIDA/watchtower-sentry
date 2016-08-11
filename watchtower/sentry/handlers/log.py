from watchtower.sentry.handlers import AbstractHandler, LOGGER


class LogHandler(AbstractHandler):

    """Handle events to log output."""

    name = 'log'

    def init_handler(self):
        self.logger = LOGGER

    def notify(self, level, *args, **kwargs):
        message = self.get_short(level, *args, **kwargs)
        if level == 'normal':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warn(message)
        elif level == 'critical':
            self.logger.error(message)

    def notify_batch(self, level, alert, ntype, data):
        for (record, value, rule) in data:
            self.notify(level, alert, value, target=record.target, ntype=ntype, rule=rule)
