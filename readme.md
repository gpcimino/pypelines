Quick Start
===========

Pypelines is a simple Python framework to model processing as a pipelines. 
Say you want to execute the classic words count problem grabbing a file from an HTTP server and send the number of word on stdout. 
You can solve it with pypelines with *two lines* (if we exclude imports):


```python
from pypelines.internals.map import Map
from pypelines.internals.sum import Sum
from pypelines.internals.stdout import StdOut
from pypelines.io.http_client import HTTP_Client

workflow = HTTP_Client('https://en.wikipedia.org/wiki/Italy') | Map(lambda line: line.split(' ')) | Map(lambda words: len(words))  | Sum() | StdOut()
workflow.run()
```

save the file above as wordcount.py and then execute:

`python wordcount.py`

One producer with multiple consumers
====================================
You can model the pypeline as Direct Acyclic Graph (DAG), using one data source (HTTP_Clinet in the example above) with multiple cascading nodes.
For instance let' s say we want to exevute the word count above, but we want also save the file locally for future use and list the first ten words that occur more frequently.
Pypelines allow us to do it reading the file only once:

file = HTTP_Client('https://en.wikipedia.org/wiki/Italy')
savefile = file | TextFile("italy.txt")
split = file | Map(lambda line: line.split(' '))
wordcount = file | Map(lambda line: line.split(' ')) | Map(lambda words: len(words))  | Sum() | StdOut()
histogram = file | FlatMap(lambda line: line.split(' ')) |  Map(lambda word: (word,0)) | CountByKey()
file.run()


Parallelism
===========
Of course pypelines offers more this simple operation. 
It is particulary useful when you want to parallelize tasks. 
It handles both symmetric and asymmetric parallelism, based on threads and processes.




Install
========

Install from github:

Run `pip install git+https://github.com/gpcimino/pypelines.git@master`

Install in development mode:

Download code and run `python setup.py develop`

