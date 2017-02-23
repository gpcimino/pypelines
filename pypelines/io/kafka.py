import json
import logging

from ..internals.dag import DAGNode

from kafka import KafkaProducer as _KafkaProducer
from kafka import KafkaConsumer as _KafkaConsumer


class KafkaProducer(DAGNode):
    def __init__(self, server, topic, msg_type):
        super().__init__()
        self._producer = _KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self._topic = topic
        self._msg_type = msg_type

    def on_data(self, data):
        log = logging.getLogger(__name__)
        msg = {"msg" : self._msg_type, "data" : data}
        print("Try to send message to kafka " + str(msg))
        self._producer.send(self._topic, msg)
        print("Message sent")
        self.forward_data(msg)


class KafkaConsumer(DAGNode):
    def __init__(self, server, topic):
        super().__init__()
        self._server = server
        self._topic = topic
        self._producer = None


    def _open_connection(self):
        if self._producer is None:
            log = logging.getLogger(__name__)
            self._producer = _KafkaConsumer(bootstrap_servers=self._server, value_deserializer=lambda v: json.loads(v.decode('utf-8')))
            self._producer.subscribe([self._topic])
            print("Connection to " + str(self._server) + " opened")

    def produce(self):
        log = logging.getLogger(__name__)
        while True:
            self._open_connection()
            try:
                print("Wait for messages...")
                for msg in self._producer:
                    self.forward_data(msg.value)
            except Exception as ex:
                log.exception(ex, exc_info=True)
        #will never get here!
        self.forward_completed()