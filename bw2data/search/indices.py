import os
import warnings

from playhouse.sqlite_ext import SqliteExtDatabase

from bw2data import projects
from bw2data.search.schema import BW2Schema

MODELS = (BW2Schema,)


class IndexManager:
    def __init__(self, database_path):
        self.path = os.path.join(projects.request_directory("search"), database_path)
        self.db = SqliteExtDatabase(self.path)
        if not os.path.exists(self.path):
            self.create()

    def get(self):
        return self

    def create(self):
        self.delete_database()
        with self.db.bind_ctx(MODELS):
            self.db.create_tables(MODELS)

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
        all_dataset = list(datasets)
        with self.db.bind_ctx(MODELS):
            for chunk_range in range(0, len(datasets), 100):
                for model in MODELS:
                    model.insert_many(
                        [
                            self._format_dataset(ds)
                            for ds in all_dataset[chunk_range : chunk_range + 100]
                        ]
                    ).execute()

    def update_dataset(self, ds):
        with self.db.bind_ctx(MODELS):
            for model in MODELS:
                model.delete().where(
                    model.code == ds["code"], model.database == ds["database"]
                ).execute()
                model.insert(**self._format_dataset(ds)).execute()

    def delete_dataset(self, ds):
        with self.db.bind_ctx(MODELS):
            for model in MODELS:
                model.delete().where(
                    model.code == ds["code"], model.database == ds["database"]
                ).execute()

    def delete_database(self):
        with self.db.bind_ctx(MODELS):
            self.db.drop_tables(MODELS)

    def close(self):
        self.db.close()

    def search(self, string, limit=None, weights=None, mask=None, filter=None):
        if mask:
            warnings.warn(
                "`mask` functionality has been deleted, and now does nothing. This input argument will be removed in the future",
                DeprecationWarning,
            )
        if filter:
            warnings.warn(
                "`filter` functionality has been deleted, and now does nothing. This input argument will be removed in the future",
                DeprecationWarning,
            )

        with self.db.bind_ctx(MODELS):
            if string == "*":
                query = BW2Schema
            else:
                query = BW2Schema.search_bm25(string.replace(",", ""), weights=weights)
            return list(
                query.select(
                    BW2Schema.name,
                    BW2Schema.comment,
                    BW2Schema.product,
                    BW2Schema.categories,
                    BW2Schema.synonyms,
                    BW2Schema.location,
                    BW2Schema.database,
                    BW2Schema.code,
                )
                .limit(limit)
                .dicts()
                .execute()
            )
