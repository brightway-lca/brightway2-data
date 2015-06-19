import psutil
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import QueryParser


def open_files():
    proc = psutil.Process()
    return len(proc.open_files())


schema = Schema(
    title=TEXT(stored=True),
    path=ID(stored=True),
    content=TEXT
)

ix = create_in("t", schema)
writer = ix.writer()
writer.add_document(
    title=u"First document",
    path=u"/a",
    content=u"This is the first document we've added!"
)
writer.add_document(
    title=u"Second document",
    path=u"/b",
    content=u"The second one is even more interesting!"
)
writer.commit()

print(open_files())

with ix.searcher() as searcher:
    query = QueryParser("content", ix.schema).parse("first")
    results = searcher.search(query)
    results[0]

print(open_files())
