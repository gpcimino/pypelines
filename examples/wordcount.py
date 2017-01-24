from pypelines import Map, Filter, Sum, StdOut
from pypelines.io import HTTPClient

workflow = HTTPClient('http://www.gutenberg.org/cache/epub/1232/pg1232.txt', readlines=True) | Filter(lambda line: line != "") | Map(lambda line: line.split(' ')) | Map(lambda words: len(words))  | Sum() | StdOut()
workflow.run()