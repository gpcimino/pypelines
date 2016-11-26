from ..internals.dag import DAGNode

from kafka import KafkaProducer


class Kafka(DAGNode):
    def __init__(self, filepath="", header=None):
        super().__init__()
        self._filepath = filepath
        self._header = header

    def on_data(self, data):
        print("data")


# class XXX(DAGNode):
#     def __init__(self, server, topic):
#         super().__init__()
#         self._producer = KafkaProducer(bootstrap_servers=server)
#         self._topic = topic

#     def on_data(self, data):
#         self._producer.send(self._topic, data)

