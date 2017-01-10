import copy
from pickleablelambda import pickleable

from .dag import DAGNode
from .spawn_process import SpawnProcess

class Parallelize(DAGNode):

    def __init__(self, split_func):
        super().__init__()
        self._split_func = pickleable(split_func)
        self._template = None #pypeline.root()
        self._parallel_jobs = {}
        self._collector = None

    def __or__(self, other):
        template_root = other.root()
        self._template = copy.deepcopy(template_root)
        super().add_child(template_root)
        return other

    #deprecated
    def __truediv__(self, other):
        import sys
        import traceback
        print("Division operator is deprecated, use pipe |", file=sys.stderr)
        traceback.print_stack()
        template_root = other.root()
        self._template = copy.deepcopy(template_root)
        super().add_child(template_root)
        return other

    # def add_childs(self, c):
    #     super().add_child(c)

    def collector(self):
        if not self._collector:
            #todo: use find node that inherit from Join
            self._collector = self.find("Join")
        return self._collector
        #return self._childs[0]

    def on_data(self, d):

        k = self._split_func(d)
        if k not in self._parallel_jobs:
            cloned = copy.deepcopy(self._template)
            cloned.leafs()[0].add_child(self.collector())
            async_node = SpawnProcess()
            async_node.add_child(cloned)
            async_node.join_on_child_process = False
            self._parallel_jobs[k] = async_node
        self._parallel_jobs[k].on_data(d)


    def on_completed(self, data=None):
        #call forward_complted in all parallel jobs
        for parallel_jobs in self._parallel_jobs.values():
            parallel_jobs.on_completed(data)
        for parallel_jobs in self._parallel_jobs.values():
            parallel_jobs.join()

    def name(self):
        return self._father.name() + "->" + super().name()