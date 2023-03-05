import os
import shutil

from whoosh import index

from .. import projects
from .schema import bw2_schema


class IndexManager:
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
        def _fix_location(string):
            if isinstance(string, tuple):
                string = string[1]
            if isinstance(string, str):
                if string.lower() == "none":
                    return ""
                else:
                    return string.lower().strip()
            else:
                return ""

        return dict(

            name=(ds.get("name") or "").lower(),
            comment=(ds.get("comment") or "").lower(),
            product=(ds.get("reference product") or "").lower(),
            categories=", ".join(ds.get("categories") or []).lower(),
            synonyms=", ".join(ds.get("synonyms") or []).lower(),
            location=_fix_location(ds.get("location") or ""),
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
