from .schema import bw2_schema
from .. import config, Database
from whoosh import index


class IndexManager(object):
    def __init__(self, dir_name=u"whoosh"):
        self.path = config.request_dir(u"whoosh")
        self.writer = self.get()

    def get(self):
        try:
            return index.open_dir(self.path)
        except index.EmptyIndexError:
            return self.create()

    def create(self):
        return index.create_in(self.path, bw2_schema)

    def reset(self):
        return self.create()

    def add_database(self, name):
        db = Database(name).load()

        for key, value in db.items():
            self.add_dataset(key, value, False)
        self.writer.commit()

    def add_dataset(self, key, value, commit=True):
        self.writer.add_document(
            name=value.get(u"name", u""),
            comment=value.get(u"comment", u""),
            product=value.get(u"reference product", u""),
            categories=u", ".join(value.get(u"categories", [])),
            location=value.get(u"location", u""),
            database=key[0],
            code=key[1],
            key="".join(key),
        )
        if commit:
            self.writer.commit()
