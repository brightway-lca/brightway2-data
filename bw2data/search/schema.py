from playhouse.sqlite_ext import FTS5Model, RowIDField, SearchField


class BW2Schema(FTS5Model):
    rowid = RowIDField()
    name = SearchField()
    comment = SearchField()
    product = SearchField()
    categories = SearchField()
    synonyms = SearchField()
    location = SearchField()
    database = SearchField()
    code = SearchField()

    class Meta:
        options = {"tokenize": "unicode61 tokenchars '''&:'"}
