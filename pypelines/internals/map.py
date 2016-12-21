from .dag import DAGNode

#from pickablelambda import LambdaProxy 

class Map(DAGNode):
    def __init__(self, func):
        super().__init__()
        self._func = func

    def on_data(self, data):
        res = self._func(data)
        self.forward_data(res)

    def name(self):
        return super().name() #+ "; func=" + inspect.getsource(self._func)

