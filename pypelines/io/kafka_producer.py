import json

from ..internals.dag import DAGNode

from kafka import KafkaProducer as _KafkaProducer


class KafkaProducer(DAGNode):
    # def __init__(self, filepath="", header=None):
    #     super().__init__()
    #     self._filepath = filepath
    #     self._header = header

    # def on_data(self, data):
    #     print("data")


# class XXX(DAGNode):
    def __init__(self, server, topic, msg_type):
        super().__init__()
        self._producer = _KafkaProducer(bootstrap_servers=server)
        self._topic = topic
        self._msg_type = msg_type

    def on_data(self, data):
        msg = {"msg" : self._msg_type, "data" : data}
        self._producer.send(self._topic, json.dumps(msg).encode('utf-8'))
        self.forward_data(msg)

