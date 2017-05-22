import json
import logging
import time
from datetime import datetime

from kafka import KafkaProducer as _KafkaProducer
from kafka import KafkaConsumer as _KafkaConsumer
from kafka.errors import KafkaError
#from waiter import wait

from ..internals.dag import DAGNode


class KafkaProducer(DAGNode):
    def __init__(self, server, topic, request_timeout_ms=5):
        super().__init__()
        self._server = server
        self._topic = topic

        self._retries = 5 #default 1
        self._request_timeout_ms = 5000 #default 30000
        self._reconnect_backoff_ms = 50 #default 50
        self._producer = None
        #self._waiter = wait(1) * 2 <= 30
        self._wait_time_sec = request_timeout_ms
        self._bootstrap()

    def _bootstrap(self):
        log = logging.getLogger(__name__)
        while True:
            try:
                self._producer = _KafkaProducer(
                    bootstrap_servers=self._server, \
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'), \
                    retries=self._retries, \
                    request_timeout_ms=self._request_timeout_ms, \
                    reconnect_backoff_ms=self._reconnect_backoff_ms
                )
                if not self._producer._closed:
                    log.info("Connected to Kafka on " + str(self._server))
                    #connection is open , exit function
                    break
                #todo: # waiter = wait(1) * 2   # exponential backoff 1, 2, 4, 8, ...
                time.sleep(self._wait_time_sec) 
            except Exception as ex:
                log.warning("Cannot connect to kafka due to: " + str(ex) + \
                    ". Wait " + str(self._wait_time_sec) + " sec before reconnect to kafka")

    def on_data(self, data):
        log = logging.getLogger(__name__)
        #here we keep sending (producer.send) until success (break the loop)
        while True:
            try:
                msg = data
                msg["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                log.debug("Try to send message to kafka " + str(msg))
                self._producer.send(self._topic, msg)
                log.debug("Message sent")
                self.forward_data(msg)
                break
            except Exception as ex:
                log.exception("Cannot send msg to kafka")
                self._close()
                self._bootstrap()
            log.warning("After message sent failure, try again to send")

    def _close(self):
        log = logging.getLogger(__name__)
        try:
            if self._producer is not None:
                self._producer.close() #breaks on windows
                log.info("Closed connection to kafka")
        except Exception as ex:
            log.warning("Cannot close connection to kafka, connection will be re-opened")
        finally:
            self._producer = None

    def on_close(self, data=None):
        self._close()
        self.forward_completed(data)


class KafkaConsumer(DAGNode):
    def __init__(self, server, topic, group_id=None, start_from_latest_msg=False):
        super().__init__()
        self._server = server
        self._topic = topic
        self._consumer = None
        self._enable_auto_commit = True
        self._group_id = group_id
        self._start_from_latest_msg = start_from_latest_msg
        self._wait_time_sec = 5

    def _bootstrap(self):
        log = logging.getLogger(__name__)
        while True:
            try:
                self._consumer = _KafkaConsumer( \
                    self._topic, \
                    bootstrap_servers=self._server, \
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')), \
                    enable_auto_commit=self._enable_auto_commit, \
                    group_id=self._group_id \
                )
                if not self._consumer._closed:
                    log.info("Connection to " + str(self._server) + \
                        " opened for topic " + str(self._topic))
                    #connection is open, success, exit function
                    break
                #todo: # waiter = wait(1) * 2   # exponential backoff 1, 2, 4, 8, ...
                time.sleep(self._wait_time_sec)
            except Exception as ex:
                log.warning("Cannot connect to kafka due to: " + str(ex) + \
                    ". Wait " + str(self._wait_time_sec) + " sec before reconnect to kafka")


    def produce(self):
        log = logging.getLogger(__name__)
        while True:
            self._bootstrap()
            try:
                if self._start_from_latest_msg:
                    #move to end og the topic
                    self._consumer.poll()
                    self._consumer.seek_to_end()
                #start read from stream
                for message in self._consumer:
                    log.debug("Received message: " + str(message.value))
                    self.forward_data(message.value)
            except Exception as ex:
                log.exception("cannot consume from kafka topic " + str(self._topic))
        #will never get here!
        self.forward_completed()


    def on_close(self, data=None):
        self._close()
        self.forward_completed(data)

    def _close(self):
        log = logging.getLogger(__name__)
        try:
            if self._consumer is not None:
                self._consumer.close() #breaks on windows
                log.info("Closed connection to kafka")
        except Exception as ex:
            log.warning("Cannot close connection to kafka, connection will be re-opened")
        finally:
            self._consumer = None


    # def produce(self):
    #     log = logging.getLogger(__name__)
    #     while True:
    #         self._open_connection()
    #         try:
    #             print("Wait for messages...")
    #             for msg in self._producer:
    #                 self.forward_data(msg.value)
    #         except Exception as ex:
    #             log.exception(ex, exc_info=True)
    #     #will never get here!
    #     self.forward_completed()



# class KafkaConsumer(DAGNode):
#     def __init__(self, server, topic):
#         super().__init__()
#         self._server = server
#         self._topic = topic
#         self._producer = None
#         self._enable_auto_commit = True


#     def _open_connection(self):
#         if self._producer is None:
#             log = logging.getLogger(__name__)
#             self._producer = _KafkaConsumer( \
#                 bootstrap_servers=self._server, \
#                 value_deserializer=lambda v: json.loads(v.decode('utf-8')), \
#                 enable_auto_commit=self._enable_auto_commit \
#             )
#             self._producer.subscribe([self._topic])
#             print("Connection to " + str(self._server) + " opened")

#     def produce(self):
#         log = logging.getLogger(__name__)
#         while True:
#             self._open_connection()
#             try:
#                 print("Wait for messages...")
#                 for msg in self._producer:
#                     self.forward_data(msg.value)
#             except Exception as ex:
#                 log.exception(ex, exc_info=True)
#         #will never get here!
#         self.forward_completed()