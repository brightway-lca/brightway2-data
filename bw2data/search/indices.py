import operator
import os

from peewee import OP, Expression
from playhouse.sqlite_ext import SqliteExtDatabase

from .schema import BW2Schema
from .. import projects

MODELS = (BW2Schema,)


class ProxyExpression:
    """
    allows exposing a custom expression in the search api
    without leaking the underlying peewee implementation
    """

    def __init__(self, attribute):
        self._attribute = attribute

    def _e(op, inv=False):
        """
        Lightweight factory which returns a method that builds an Expression
        consisting of the left-hand and right-hand operands, using `op`.
        """

        def inner(self, rhs):
            if inv:
                return Expression(rhs, op, self._attribute)
            return Expression(self._attribute, op, rhs)

        return inner

    __and__ = operator.and_
    __or__ = _e(OP.OR)

    __add__ = _e(OP.ADD)
    __sub__ = _e(OP.SUB)
    __mul__ = _e(OP.MUL)
    __div__ = __truediv__ = _e(OP.DIV)
    __xor__ = _e(OP.XOR)
    __radd__ = _e(OP.ADD, inv=True)
    __rsub__ = _e(OP.SUB, inv=True)
    __rmul__ = _e(OP.MUL, inv=True)
    __rdiv__ = __rtruediv__ = _e(OP.DIV, inv=True)
    __rand__ = _e(OP.AND, inv=True)
    __ror__ = _e(OP.OR, inv=True)
    __rxor__ = _e(OP.XOR, inv=True)

    def __eq__(self, rhs):
        op = OP.IS if rhs is None else OP.EQ
        return Expression(self._attribute, op, rhs)

    def __ne__(self, rhs):
        op = OP.IS_NOT if rhs is None else OP.NE
        return Expression(self._attribute, op, rhs)

    __lt__ = _e(OP.LT)
    __le__ = _e(OP.LTE)
    __gt__ = _e(OP.GT)
    __ge__ = _e(OP.GTE)
    __lshift__ = _e(OP.IN)
    __rshift__ = _e(OP.IS)
    __mod__ = _e(OP.LIKE)
    __pow__ = _e(OP.ILIKE)

    like = _e(OP.LIKE)
    ilike = _e(OP.ILIKE)

    bin_and = _e(OP.BIN_AND)
    bin_or = _e(OP.BIN_OR)
    in_ = _e(OP.IN)
    not_in = _e(OP.NOT_IN)
    regexp = _e(OP.REGEXP)
    iregexp = _e(OP.IREGEXP)


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
                            for ds in all_dataset[chunk_range: chunk_range + 100]
                        ]
                    ).execute()

    def update_dataset(self, ds):
        with self.db.bind_ctx(MODELS):
            for model in MODELS:
                model.delete().where(model.code == ds["code"], model.database == ds["database"]).execute()
                model.insert(**self._format_dataset(ds)).execute()

    def delete_dataset(self, ds):
        with self.db.bind_ctx(MODELS):
            for model in MODELS:
                model.delete().where(model.code == ds["code"], model.database == ds["database"]).execute()

    def delete_database(self):
        with self.db.bind_ctx(MODELS):
            self.db.drop_tables(MODELS)

    def close(self):
        self.db.close()

    def _process_condition(self, conditions, entity):
        statements = []
        string_keys = set([k for k, v in conditions.items() if type(v) is str])
        if string_keys:
            statements.append(
                entity.match(" AND ".join("{}:'{}'".format(k, v) for k, v in conditions.items() if k in string_keys))
            )
        for k, v in [(k, v) for k, v in conditions.items() if k not in string_keys]:
            attribute = getattr(entity, k)
            if callable(v):
                statements.append(v(ProxyExpression(attribute)))
            else:
                raise ValueError("unsupported argument type: {}".format(type(v)))
        return statements

    def search(self, string, limit=None, weights=None, mask=None, filter=None):
        conditions = []
        if mask:
            mask_entity = BW2Schema.alias("mask")
            conditions.append(
                BW2Schema.rowid.not_in(
                    mask_entity.select(mask_entity.rowid).where(
                        *self._process_condition(mask, mask_entity)
                    )
                )
            )
        if filter:
            filter_entity = BW2Schema.alias("filter")
            conditions.append(
                BW2Schema.rowid.in_(
                    filter_entity.select(filter_entity.rowid).where(
                        *self._process_condition(filter, filter_entity)
                    )
                )
            )

        with self.db.bind_ctx(MODELS):
            if string == '*':
                query = BW2Schema
            else:
                query = BW2Schema.search_bm25(" ".join([term.replace(",", "") for term in string.split(" ")]), weights=weights)
            if conditions:
                query = query.where(*conditions)
            return list(
                query.
                select(BW2Schema.name, BW2Schema.comment, BW2Schema.product, BW2Schema.categories,
                       BW2Schema.synonyms, BW2Schema.location, BW2Schema.database, BW2Schema.code).
                limit(limit).
                dicts().
                execute()
            )
