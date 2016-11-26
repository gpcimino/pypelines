from .dag import DAGNode

class StoreAndAssert(DAGNode):

    def __init__(self, expected, test_case, assert_method="assertEqual", \
    ignore_on_completed_data=True, assert_params=None):
        super().__init__()
        self._expected = expected
        self._test_case = test_case
        self._assert_method = assert_method
        self._ignore_on_completed_data = ignore_on_completed_data
        self._assert_params = {} if assert_params is None else assert_params
        self._actual = []

    def on_data(self, data):
        self._actual.append(data)


    def on_completed(self, data=None):
        if not self._ignore_on_completed_data:
            self._actual.append(data)
        _assert = getattr(self._test_case, self._assert_method)
        _assert(self._expected, self._actual, **self._assert_params)
