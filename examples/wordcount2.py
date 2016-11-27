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