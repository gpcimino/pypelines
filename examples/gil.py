import time
import os
import json

from pypelines import Map, StdOut, Repeat, SpawnThread
from pypelines.io import HTTPClient
from fib import fib

def compute_fib(tup):
    [n, fib_of_n] = json.loads(tup.strip())
    new_fib_of_n = fib(n)
    return "Executed slow HTTP get and computed fibonacci(" + str(n) + ")=" + str(new_fib_of_n)

def main():
    """Demostrate that for I/O-bound operation python interpreter releases the GIL.
    The idea is to implement two operations using pypelines: one slow I/O-bound communication
    followed by one slow CPU-bound computation.

    To emulate an inefficient I/O-bound communication we will use an HTTP GET to an external
    web server which will compute very inefficiently Fibonacci value fib(n):
    HTTPClient("http://127.0.0.1:12345/fib/32").
    To run the web server execute 'python fib_web.py' in a second shell.

    To emulate a CPU-bound long running calculation we will re-compute fib(n) with the same
    value of n: Map(compute_fib).

    Clearly this is a useless example, but it is easy to note that:

    1. if we run this pypeline synchronously, we get as total time approximately
    the sum of the duration of the two operations.

    2. if we run this pypeline asynchronously using asymmetric parallelism  with one thread,
    we can observe that the time is almost half of the synchronous case.

    This means that while the HTTP client is waiting for the results (the web server takes a while
    to compute fib(n)), the Python interpreter release the GIL and the thread running
    compute_fib start to execute."""

    #Execute: python fib_web.py

    print("Run Pypelines synchronously...")
    workflow = Repeat(lambda x: x > 10) | HTTPClient("http://127.0.0.1:12345/fib/32") | Map(compute_fib) | StdOut()
    t1 = time.time()
    workflow.run()
    t2 = time.time()
    print("Took " + str(t2-t1) + " seconds.")

    print("Now, run Pypelines asynchronously using asymmetric parallelims with one thread, should be faster...")
    workflow = Repeat(lambda x: x > 10) | HTTPClient("http://127.0.0.1:12345/fib/32") | SpawnThread() |  Map(compute_fib) | StdOut()
    t1 = time.time()
    workflow.run()
    t2 = time.time()
    print("Took " + str(t2-t1) + " seconds.")

if __name__ == '__main__':
    main()



