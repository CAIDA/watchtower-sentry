import sys
import logging
import threading
import time
import traceback
import SentryModule

logger = logging.getLogger(__name__)


class Datasource(SentryModule.SentryModule):
    def __init__(self, config, add_cfg_schema, modlogger, gen):
        logger.debug("Datasource.__init__")
        super().__init__(config, add_cfg_schema, modlogger, gen,
            isSource=True)
        self.done = False
        self.incoming = []
        self.producable = True
        self.consumable = False
        # The reader thread produces data by reading it from its source and
        # appending it to self.incoming.
        self.reader = threading.Thread(target=self.reader_thread,
            daemon=True, # program need not join() this thread to exit
            name="\x1b[31mDS.reader")
        lock = threading.Lock()
        self.cond_producable = threading.Condition(lock)
        self.cond_consumable = threading.Condition(lock)

    def reader_thread(self):
        try:
            self.reader_body()
            if not self.done:
                with self.cond_consumable:
                    logger.debug("cond_consumable.notify (done=True)")
                    self.done = True
                    self.cond_consumable.notify()
        except:
            e = sys.exc_info()[1]
            logger.critical("%s:\n%s", type(e).__name__, traceback.format_exc())
            with self.cond_consumable:
                logger.debug("cond_consumable.notify (exception)")
                self.done = "exception in %s reader thread" % self.modname
                self.cond_consumable.notify()


    def reader_body(self):
        raise NotImplementedError() # abstract method

    # Consume data produced by the reader thread, and yield it as a generator
    def run(self):
        logger.debug("Datasource.run()")
        self.reader.start()
        try:
            while True:
                # wait for reader thread to fill self.incoming
                data = None
                with self.cond_consumable:
                    logger.debug("cond_consumable check")
                    while not self.consumable and not self.done:
                        logger.debug("cond_consumable.wait")
                        self.cond_consumable.wait()
                    if self.consumable:
                        data = self.incoming
                        self.incoming = None
                    elif isinstance(self.done, str):
                        logger.debug("Datasource.run(): %s", self.done)
                        break
                    else: # if self.done:
                        logger.debug("Datasource.run(): end-of-stream")
                        break
                    self.consumable = False
                    logger.debug("cond_consumable.wait DONE (%d items)",
                        len(data))
                # Tell reader thread that self.incoming is ready to be refilled
                with self.cond_producable:
                    logger.debug("cond_producable.notify")
                    self.producable = True
                    self.cond_producable.notify()
                # Give up control to the reader so it can request the next set
                # of data; then while it waits for the response it will return
                # control to this thread.
                time.sleep(0)
                # Process the data.
                for entry in data:
                    yield entry
            self.reader.join()
        except:
            e = sys.exc_info()[1]
            logger.critical("%s:\n%s", type(e).__name__, traceback.format_exc())
            with self.cond_producable:
                logger.debug("cond_producable.notify")
                self.done = "exception in Datasource.run"
                self.cond_producable.notify()