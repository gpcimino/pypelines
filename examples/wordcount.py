from pypelines.internals.map import Map
from pypelines.internals.filter import Filter
from pypelines.internals.sum import Sum
from pypelines.internals.stdout import StdOut
from pypelines.io.http_client import HTTPClient

workflow = HTTPClient('http://www.gutenberg.org/cache/epub/1232/pg1232.txt', readlines=True) | Filter(lambda line: line != "") | Map(lambda line: line.split(' ')) | Map(lambda words: len(words))  | Sum() | StdOut()
workflow.run()