from playhouse.sqlite_ext import FTSModel, SearchField, RowIDField


class BW2Schema(FTSModel):
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
        # Use the porter stemming algorithm to tokenize content.
        options = {'tokenize': 'porter'}
