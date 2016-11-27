Quick Start
===========

Pypelines is a simple Python framework to model processing as a pipelines. 

Say you want to execute the classic words count problem grabbing a file from a Web server and send the number of words on stdout. 
You can solve it with pypelines with *two lines* (if we exclude imports):


```python
from pypelines.internals.map import Map
from pypelines.internals.filter import Filter
from pypelines.internals.sum import Sum
from pypelines.internals.stdout import StdOut
from pypelines.io.http_client import HTTPClient

workflow = HTTPClient('http://www.gutenberg.org/cache/epub/1232/pg1232.txt', readlines=True) | Filter(lambda line: line != "") | Map(lambda line: line.split(' ')) | Map(lambda words: len(words))  | Sum() | StdOut()
workflow.run()
```

This example is in the examples folder in wordcount.py file. Execute as:

```
python wordcount.py
```

One producer with multiple consumers
====================================
You can model the pypeline as Direct Acyclic Graph (DAG), using one data source (HTTPClient in the example above) with multiple cascading nodes.
For instance let's say we want to execute the word count as above, but we want also save the file locally for future use and list the first ten words that occur more frequently.
Pypelines allow us to do it reading the file only once:

```python
from pypelines.internals.map import Map
from pypelines.internals.filter import Filter
from pypelines.internals.flat_map import FlatMap
from pypelines.internals.sum import Sum
from pypelines.internals.stdout import StdOut
from pypelines.internals.sort import Sort
from pypelines.internals.head import Head
from pypelines.internals.count_by_key import CountByKey
from pypelines.io.http_client import HTTPClient
from pypelines.io.textfile import TextFile


workflow = HTTPClient('http://www.gutenberg.org/cache/epub/1232/pg1232.txt', readlines=True) | Filter(lambda line: line != "")
savefile = workflow | TextFile("macchiavelli.txt")
wordcount = workflow | Map(lambda line: line.split(' ')) | Map(lambda words: len(words))  | Sum() | StdOut()
histogram = workflow | FlatMap(lambda line: line.split(' ')) | Filter(lambda word: word != "") | Map(lambda word: (word, 1)) | CountByKey() | Sort(key_func=lambda data: data[1], reverse=True) | Head(10) | StdOut()
workflow.run()
```

This example is in the examples folder in wordcount2.py file. Execute as:

```
python wordcount2.py
```

Symmetric Parallelism
=====================
Pypelines offers symmetric parallelism capabilities: is possible to execute one or more blocks of
the pipeline in different threads or processes. 
For example let's try to demostrate that the Python GIL is released during I/O-bound operations.
To do so, we will execute a long HTTP get (I/O-bound) followed by a slow fibonacci serie computation (CPU-bound).
First run a web server in separate terminal with the command (the fib_web.py file is in the examples folder):

```
python fib_web.py
```

Than execute the pypeline synchronously (note that imports and compute_fib function is missing, see gil.py in examples folder for complete code):

```
workflow = Repeat(lambda x: x > 10) | HTTPClient("http://127.0.0.1:12345/fib/32") | Map(compute_fib) | StdOut()
workflow.run()
```

The total excution time is 10 times the sum of the HTTP get plus the fibonacci computations.
We can easily execute the HTTP get and the fibonacci computation in 2 threads using SpawnThread():

```
workflow = Repeat(lambda x: x > 10) | HTTPClient("http://127.0.0.1:12345/fib/32") | SpawnThread() |  Map(compute_fib) | StdOut()
workflow.run()
```

In this case the duration is almost half of the synchronous case, this means that during the long running I/O operation the Python interpreter released the GIL, letting the CPU-bound operation execute.



Install
========

Install from github:

Run `pip install git+https://github.com/gpcimino/pypelines.git@master`

Install in development mode:

Download code and run `python setup.py develop`

