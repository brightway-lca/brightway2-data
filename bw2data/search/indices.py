from .. import projects
from .schema import bw2_schema
from whoosh import index
import os
import shutil


class IndexManager(object):
    def __init__(self, database_path, dir_name="whoosh"):
        self.path = os.path.join(projects.request_directory("whoosh"), database_path)
        if not os.path.exists(self.path):
            os.mkdir(self.path)

    def get(self):
        try:
            return index.open_dir(self.path)
        except index.EmptyIndexError:
            return self.create()

    def create(self):
        self.delete_database()
        os.mkdir(self.path)
        return index.create_in(self.path, bw2_schema)

    def _format_dataset(self, ds):
        fl = lambda o: o[1].lower() if isinstance(o, tuple) else o.lower()
        return dict(
            name=ds.get("name", "").lower(),
            comment=ds.get("comment", "").lower(),
            product=ds.get("reference product", "").lower(),
            categories=u", ".join(ds.get("categories", [])).lower(),
            synonyms=u", ".join(ds.get("synonyms", [])).lower(),
            location=fl(ds.get("location", "")),
            database=ds["database"],
            code=ds["code"],
        )

    def add_dataset(self, ds):
        self.add_datasets([ds])

    def add_datasets(self, datasets):
        writer = self.get().writer()
        for ds in datasets:
            writer.add_document(**self._format_dataset(ds))
        writer.commit()

    def update_dataset(self, ds):
        writer = self.get().writer()
        writer.update_document(**self._format_dataset(ds))
        writer.commit()

    def delete_dataset(self, ds):
        index = self.get()
        index.delete_by_term("code", ds["code"])

    def delete_database(self):
        shutil.rmtree(self.path)
