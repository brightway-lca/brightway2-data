from whoosh.fields import TEXT, ID, Schema

bw2_schema = Schema(
    name=TEXT(stored=True, sortable=True),
    comment=TEXT(stored=True),
    product=TEXT(stored=True, sortable=True),
    categories=TEXT(stored=True),
    location=ID(stored=True, sortable=True),
    database=ID(stored=True, sortable=True),
    code=ID(stored=True, sortable=True),
    key=ID(unique=True),
)
