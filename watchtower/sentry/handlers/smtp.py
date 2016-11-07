import datetime as dt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP

from tornado import gen, concurrent

from watchtower.sentry.handlers import AbstractHandler, TEMPLATES, LOGGER
from watchtower.sentry import utils


class SMTPHandler(AbstractHandler):

    name = 'smtp'

    # Default options
    defaults = {
        'host': 'localhost',
        'port': 25,
        'username': None,
        'password': None,
        'from': 'beacon@graphite',
        'to': None,
        'use_tls': False,
        'html': True,
        'graphite_url': None,
    }

    def init_handler(self):
        """ Check self options. """
        assert self.options.get('host') and self.options.get('port'), "Invalid options"
        assert self.options.get('to'), 'Recipients list is empty. SMTP disabled.'
        if not isinstance(self.options['to'], (list, tuple)):
            self.options['to'] = [self.options['to']]

    def notify(self, level, alert, value, target=None, ntype=None, rule=None):
        LOGGER.debug("Handler (%s) %s", self.name, level)

        msg = self.get_message(
            level=level, alert=alert, value=value, target=target, ntype=ntype, rule=rule)
        msg['Subject'] = self.get_short(level, alert, value, target=target, ntype=ntype, rule=rule)
        self.send_message(msg)

    def notify_batch(self, level, alert, ntype, data):
        LOGGER.debug("Handler (%s-batch) %s", self.name, level)

        alert_time = alert.get_record_time(data[0][0])
        msg = self.get_message(True,
            ntype=ntype, level=level, alert=alert, data=data, utils=utils, time=alert_time)
        msg['Subject'] = self.get_short_batch(level, alert, ntype)
        self.send_message(msg)

    def get_message(self, batch=False, ntype=None, **kwargs):
        suffix = '-batch' if batch else ''
        txt_tmpl = TEMPLATES[ntype]['text' + suffix]
        ctx = dict(reactor=self.reactor, dt=dt, utils=utils, **self.options)
        ctx.update(**kwargs)
        msg = MIMEMultipart('alternative')
        plain = MIMEText(str(txt_tmpl.generate(**ctx)), 'plain')
        msg.attach(plain)
        if self.options['html']:
            html_tmpl = TEMPLATES[ntype]['html' + suffix]
            html = MIMEText(str(html_tmpl.generate(**ctx)), 'html')
            msg.attach(html)
        return msg

    @gen.coroutine
    def send_message(self, msg):
        msg['From'] = self.options['from']
        msg['To'] = ", ".join(self.options['to'])

        smtp = SMTP()
        yield smtp_connect(smtp, self.options['host'], self.options['port'])

        if self.options['use_tls']:
            yield smtp_starttls(smtp)

        if self.options['username'] and self.options['password']:
            yield smtp_login(smtp, self.options['username'], self.options['password'])

        try:
            LOGGER.debug("Send message to: %s", ", ".join(self.options['to']))
            smtp.sendmail(self.options['from'], self.options['to'], msg.as_string())
        finally:
            smtp.quit()


@concurrent.return_future
def smtp_connect(smtp, host, port, callback):
    callback(smtp.connect(host, port))


@concurrent.return_future
def smtp_starttls(smtp, callback):
    callback(smtp.starttls())


@concurrent.return_future
def smtp_login(smtp, username, password, callback):
    callback(smtp.login(username, password))

#  pylama:ignore=E1120
