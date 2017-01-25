from pypelines import Map, Filter, Sum, StdOut, CountByKey, FlatMap, Sort, Head
from pypelines.io import HTTPClient, TextFile


workflow = HTTPClient('http://www.gutenberg.org/cache/epub/1232/pg1232.txt', readlines=True) | Filter(lambda line: line != "")
savefile = workflow | TextFile("macchiavelli.txt")
wordcount = workflow | Map(lambda line: line.split(' ')) | Map(lambda words: len(words))  | Sum() | StdOut()
histogram = workflow | FlatMap(lambda line: line.split(' ')) | Filter(lambda word: word != "") | Map(lambda word: (word, 1)) | CountByKey() | Sort(key_func=lambda data: data[1], reverse=True) | Head(10) | StdOut()
workflow.run()