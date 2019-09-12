import logging
import requests
import SentryModule
from sources._Datasource import Datasource

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "expression":    {"type": "string"},
        "starttime":     {"type": "string"},
        "endtime":       {"type": "string"},
        "url":           {"type": "string"},
        "batchduration": {"type": "integer", "exclusiveMinimum": 0},
        "ignorenull":    {"type": "boolean"},
        "queryparams":   {"type": "object"},
    },
    "required": ["expression", "starttime", "endtime", "url", "batchduration"]
}

class Historical(Datasource):

    def __init__(self, config, gen):
        logger.debug("Historical.__init__")
        super().__init__(config, logger, gen)
        self.loop = None
        self.client = None
        self.expression = config['expression']
        self.start_time = SentryModule.strtimegm(config['starttime'])
        self.end_time = SentryModule.strtimegm(config['endtime'])
        self.batch_duration = config['batchduration']
        self.queryparams = config.get('queryparams', None)
        self.end_batch = self.start_time
        self.url = config['url']
        self.request = None

    def make_next_request(self):
        start_batch = self.end_batch
        if start_batch >= self.end_time:
            return False
        self.end_batch += self.batch_duration
        if self.end_batch >= self.end_time:
            self.end_batch = self.end_time
        post_data = {
            'from': start_batch,
            'until': self.end_batch,
            'expression': self.expression
        }
        if self.queryparams:
            post_data.update(self.queryparams)
        logger.debug("request: %d - %d", start_batch, self.end_batch)
        self.request = requests.post(self.url, data=post_data, timeout=60)
        return True

    def handle_response(self):
        logger.debug("response code: %d", self.request.status_code)
        self.request.raise_for_status()
        result = self.request.json()
        logger.debug("response: %s - %s", result['queryParameters']['from'],
            result['queryParameters']['until'])

        # wait for self.incoming to be empty
        with self.cond_producable:
            logger.debug("cond_producable check")
            while not self.producable and not self.done:
                logger.debug("cond_producable.wait")
                self.cond_producable.wait()
            if self.done:
                return
            self.incoming = []
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

    def reader_body(self):
        logger.debug("historic.run_reader()")
        try:
            while not self.done and self.make_next_request():
                self.handle_response()
        except requests.ConnectionError as e:
            # strip excess information about guts of requests module
            raise ConnectionError(str(e)) from None
        logger.debug("historic done")
