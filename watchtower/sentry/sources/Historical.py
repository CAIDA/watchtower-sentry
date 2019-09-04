import sys
import logging
import requests
import SentryModule
from sources._Datasource import Datasource

logger = logging.getLogger(__name__)


class Historical(Datasource):
    schema = {
        "type": "object",
        "properties": {
            "name":          { "type": "string" },
            "expression":    { "type": "string" },
            "starttime":     { "type": "string" },
            "endtime":       { "type": "string" },
            "url":           { "type": "string" },
            "batchduration": { "type": "number" },
            "ignorenull":    { "type": "boolean" },
            "queryparams":   { "type": "object" },
        },
        "additionalProperties": { "not": {} },
        "required": ["expression", "starttime", "endtime", "url",
            "batchduration"]
    }

    def __init__(self, options, input):
        logger.debug("Historical.__init__")
        super().__init__(options, self.schema, input)
        self.options = options
        self.loop = None
        self.client = None
        self.expression = self.options['expression']
        self.start_time = SentryModule.strtimegm(self.options['starttime'])
        self.end_time = SentryModule.strtimegm(self.options['endtime'])
        self.batch_duration = self.options['batchduration']
        self.queryparams = self.options.get('queryparams', None)
        self.end_batch = self.start_time

    def make_next_request(self):
        self.start_batch = self.end_batch
        if self.start_batch >= self.end_time:
            return False
        self.end_batch += self.batch_duration
        if self.end_batch >= self.end_time:
            self.end_batch = self.end_time
        post_data = {
            'from': self.start_batch,
            'until': self.end_batch,
            'expression': self.expression
        }
        if self.queryparams:
            post_data.update(self.queryparams)
        logger.debug("request: %d - %d" % (self.start_batch, self.end_batch))
        self.request = requests.post(self.options['url'], data = post_data, timeout = 60)
        return True

    def handle_response(self):
        logger.debug("response code: %d" % self.request.status_code)
        self.request.raise_for_status()
        result = self.request.json()
        logger.debug("response: %s - %s" % (result['queryParameters']['from'], result['queryParameters']['until']))

        # wait for self.incoming to be empty
        with self.cond_producable:
            logger.debug("cond_producable check")
            while not self.producable and not self.done:
                logger.debug("cond_producable.wait")
                self.cond_producable.wait()
            if self.done:
                return
            self.incoming = [];
            self.producable = False
            logger.debug("cond_producable.wait DONE")
        for key in result['data']['series']:
            t = int(result['data']['series'][key]['from'])
            step = int(result['data']['series'][key]['step'])
            ascii_key = bytes(key, 'ascii')
            for value in result['data']['series'][key]['values']:
                self.incoming.append((ascii_key, value, t))
                t += step
        # tell computation thread that self.incoming is now full
        with self.cond_consumable:
            logger.debug("cond_consumable.notify")
            self.consumable = True
            self.cond_consumable.notify()

    def run_reader(self):
        try:
            logger.debug("historic.run_reader()")
            while not self.done and self.make_next_request():
                self.handle_response()
            logger.debug("historic done")
            with self.cond_consumable:
                logger.debug("cond_consumable.notify (done=True)")
                self.done = True
                self.cond_consumable.notify()
        except:
            e = sys.exc_info()[1]
            logger.critical(type(e).__name__ + ':\n' + traceback.format_exc())
            with self.cond_consumable:
                logger.debug("cond_consumable.notify (exception)")
                self.done = "exception in historical reader"
                self.cond_consumable.notify()

