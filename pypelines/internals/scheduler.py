from .dag import DAGNode

class Scheduler(DAGNode):
    def __init__(self, cron_string):
        super().__init__()
        self._cron_string = cron_string

    def run(self):
        pass
        #set_cron(self._cron_string, self.execute, self.completed):

    def completed(self):
        self.forward_completed()

    def execute(self):
        data = None
        self.forward_data(data)