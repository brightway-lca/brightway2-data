import copy
import uuid
from collections.abc import Iterable
from typing import Callable, List, Optional

import pandas as pd

from bw2data import databases, geomapping, projects
from bw2data.backends import sqlite3_lci_db
from bw2data.backends.schema import ActivityDataset, ExchangeDataset
from bw2data.backends.typos import (
    check_activity_keys,
    check_activity_type,
    check_exchange_keys,
    check_exchange_type,
)
from bw2data.backends.utils import dict_as_activitydataset, dict_as_exchangedataset
from bw2data.configuration import labels
from bw2data.errors import ValidityError
from bw2data.logs import stdout_feedback_logger
from bw2data.proxies import ActivityProxyBase, ExchangeProxyBase
from bw2data.search import IndexManager
from bw2data.signals import on_activity_code_change, on_activity_database_change


class Exchanges(Iterable):
    """Iterator for exchanges with some additional methods.

    This is not a generator; ``next()`` is not supported. Everything time you start to iterate over the object you get a new list starting from the beginning. However, to get a single item you can do ``next(iter(foo))``.

    Ordering is by database row id.

    Supports the following:

    .. code-block:: python

        exchanges = activity.exchanges()

        # Iterate
        for exc in exchanges:
            pass

        # Length
        len(exchanges)

        # Delete all
        exchanges.delete()

    """

    def __init__(self, key, kinds=None, reverse=False):
        self._key = key
        if reverse:
            self._args = [
                ExchangeDataset.input_database == self._key[0],
                ExchangeDataset.input_code == self._key[1],
                # No production exchanges - these two clauses have to be together,
                # not individually. Note: DO NOT wrap these two clauses in
                # parentheses, this somehow breaks the functionality!
                ExchangeDataset.output_database
                != self._key[0] & ExchangeDataset.output_code
                != self._key[1],
            ]
        else:
            self._args = [
                ExchangeDataset.output_database == self._key[0],
                ExchangeDataset.output_code == self._key[1],
            ]
        if kinds:
            self._args.append(ExchangeDataset.type << kinds)

    def filter(self, expr):
        self._args.append(expr)

    def delete(self, allow_in_sourced_project: bool = False):
        if projects.dataset.is_sourced and not allow_in_sourced_project:
            raise NotImplementedError("Mass exchange deletion not supported in sourced projects")
        databases.set_dirty(self._key[0])
        ExchangeDataset.delete().where(*self._args).execute()

    def _get_queryset(self):
        return ExchangeDataset.select().where(*self._args).order_by(ExchangeDataset.id)

    def __iter__(self):
        for obj in self._get_queryset():
            yield Exchange(obj)

    def __len__(self):
        return self._get_queryset().count()

    def to_dataframe(
        self, categorical: bool = True, formatters: Optional[List[Callable]] = None
    ) -> pd.DataFrame:
        """Return a pandas DataFrame with all node exchanges. Standard DataFrame columns are:

            target_id: int,
            target_database: str,
            target_code: str,
            target_name: Optional[str],
            target_reference_product: Optional[str],
            target_location: Optional[str],
            target_unit: Optional[str],
            target_type: Optional[str]
            source_id: int,
            source_database: str,
            source_code: str,
            source_name: Optional[str],
            source_product: Optional[str],  # Note different label
            source_location: Optional[str],
            source_unit: Optional[str],
            source_categories: Optional[str]  # Tuple concatenated with "::" as in `bw2io`
            edge_amount: float,
            edge_type: str,

        Target is the node consuming the edge, source is the node or flow being consumed. The terms target and source were chosen because they also work well for biosphere edges.

        Args:

        ``categorical`` will turn each string column in a `pandas Categorical Series <https://pandas.pydata.org/docs/reference/api/pandas.Categorical.html>`__. This takes 1-2 extra seconds, but saves around 50% of the memory consumption.

        ``formatters`` is a list of callables that modify each row. These functions must take the following keyword arguments, and use the `Wurst internal data format <https://wurst.readthedocs.io/#internal-data-format>`__:

            * ``node``: The target node, as a dict
            * ``edge``: The edge, including attributes of the source node
            * ``row``: The current row dict being modified.

        The functions in ``formatters`` don't need to return anything, they modify ``row`` in place.

        Returns a pandas ``DataFrame``.

        """
        result = []

        for edge in self:
            row = {
                "target_id": edge.output["id"],
                "target_database": edge.output["database"],
                "target_code": edge.output["code"],
                "target_name": edge.output.get("name"),
                "target_reference_product": edge.output.get("reference product"),
                "target_location": edge.output.get("location"),
                "target_unit": edge.output.get("unit"),
                "target_type": edge.output.get("type", labels.process_node_default),
                "source_id": edge.input["id"],
                "source_database": edge.input["database"],
                "source_code": edge.input["code"],
                "source_name": edge.input.get("name"),
                "source_product": edge.input.get("product"),
                "source_location": edge.input.get("location"),
                "source_unit": edge.input.get("unit"),
                "source_categories": (
                    "::".join(edge.input["categories"]) if edge.input.get("categories") else None
                ),
                "edge_amount": edge["amount"],
                "edge_type": edge["type"],
            }
            if formatters is not None:
                for func in formatters:
                    func(node=edge.output, edge=edge, row=row)
            result.append(row)

        df = pd.DataFrame(result)

        if categorical:
            categorical_columns = [
                "target_database",
                "target_name",
                "target_reference_product",
                "target_location",
                "target_unit",
                "target_type",
                "source_database",
                "source_code",
                "source_name",
                "source_product",
                "source_location",
                "source_unit",
                "source_categories",
                "edge_type",
            ]
            for column in categorical_columns:
                if column in df.columns:
                    df[column] = df[column].astype("category")

        return df


class Activity(ActivityProxyBase):
    ORMDataset = ActivityDataset

    def __init__(self, document=None, **kwargs):
        """Create an `Activity` proxy object.

        If this is a new activity, can pass `kwargs`.

        If the activity exists in the database, `document` should be an `ActivityDataset`.
        """
        if document is None:
            self._document = self.ORMDataset()
            self._data = kwargs
        else:
            self._document = document
            self._data = self._document.data
            self._data["code"] = self._document.code
            self._data["database"] = self._document.database
            self._data["id"] = self._document.id

    @property
    def id(self):
        return self._document.id

    def __getitem__(self, key):
        if key == 0:
            return self["database"]
        elif key == 1:
            return self["code"]
        elif key in self._data:
            return self._data[key]

        for section in ("classifications", "properties"):
            if section in self._data:
                if isinstance(self._data[section], list):
                    try:
                        return {k: v for k, v in self._data[section]}[key]
                    except:
                        pass
                elif key in self._data[section]:
                    return self._data[section][key]

        try:
            rp = self.rp_exchange()
        except ValueError:
            raise KeyError

        if key in rp.get("classifications", []):
            return rp["classifications"][key]
        if key in rp.get("properties", []):
            return rp["properties"][key]

        raise KeyError

    def __setitem__(self, key, value):
        if key == "id":
            raise ValueError("`id` is read-only")
        elif key == "code" and "code" in self._data:
            self._change_code(value)
            stdout_feedback_logger.info(
                "Successfully switched activity dataset to new code `%s`", value
            )
        elif key == "database" and "database" in self._data:
            self._change_database(value)
            stdout_feedback_logger.info(
                "Successfully switch activity dataset to database `%s`", value
            )
        else:
            super(Activity, self).__setitem__(key, value)

    @property
    def key(self):
        return (self.get("database"), self.get("code"))

    def delete(self, signal: bool = True):
        from bw2data import Database, calculation_setups
        from bw2data.parameters import ActivityParameter, ParameterizedExchange

        def purge(obj: Activity, dct: dict) -> dict:
            return {
                key: value
                for key, value in dct.items()
                if key != obj._data["id"] and key != (obj._data["database"], obj._data["code"])
            }

        try:
            ap = ActivityParameter.get(database=self[0], code=self[1])
            ParameterizedExchange.delete().where(ParameterizedExchange.group == ap.group).execute()
            ActivityParameter.delete().where(
                ActivityParameter.database == self[0], ActivityParameter.code == self[1]
            ).execute()
        except ActivityParameter.DoesNotExist:
            pass
        IndexManager(Database(self["database"]).filename).delete_dataset(self._data)
        self.exchanges().delete(allow_in_sourced_project=True)

        for name, setup in calculation_setups.items():
            if any(
                (key == self._data["id"] or key == (self._data["database"], self._data["code"]))
                for func_unit in setup["inv"]
                for key in func_unit
            ):
                stdout_feedback_logger.warning("Removing this node from calculation setup %s", name)
                setup["inv"] = [purge(self, dct) for dct in setup["inv"] if purge(self, dct)]
        calculation_setups.flush()

        self._document.delete_instance(signal=signal)
        self = None

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        """
        Saves the current activity to the database after performing various checks.
        This method validates the activity, updates the database status, and handles
        geographical and indexing updates. It raises an error if the activity is
        not valid and updates relevant data in the database.

        Raises
        ------
        ValidityError
            If the activity is not valid, an error is raised detailing the reasons.

        Notes
        -----
        The method performs the following operations:
        - Checks if the activity is valid.
        - Marks the database as 'dirty', indicating changes.
        - Checks for type and key validity of the activity.
        - Updates the activity's associated document in the database.
        - Updates the geographical mapping if needed.
        - Updates the index if the database is searchable.

        Examples
        --------
        >>> activity.save()

        Saves the activity if it's valid, otherwise raises ValidityError.
        """
        from bw2data import Database

        if not data_already_set and not self.valid():
            raise ValidityError(
                "This activity can't be saved for the "
                + "following reasons\n\t* "
                + "\n\t* ".join(self.valid(why=True)[1])
            )

        databases.set_dirty(self["database"])

        if not data_already_set:
            check_activity_type(self._data.get("type"))
            check_activity_keys(self)

            for key, value in dict_as_activitydataset(self._data).items():
                # ID value is either already in `._document` (update) or will be created by
                # `SnowflakeIDBaseClass.save()`.
                if key != "id":
                    setattr(self._document, key, value)

        self._document.save(signal=signal, force_insert=force_insert)

        if self.get("location") and self["location"] not in geomapping:
            geomapping.add([self["location"]])

        if databases[self["database"]].get("searchable", True):
            IndexManager(Database(self["database"]).filename).update_dataset(self._data)

    def _change_code(self, new_code: str, signal: bool = True):
        if self["code"] == new_code:
            return

        previous = self["code"]

        if (
            self.ORMDataset.select()
            .where(
                self.ORMDataset.database == self["database"],
                self.ORMDataset.code == new_code,
            )
            .count()
        ):
            raise ValueError("Activity database with code `{}` already exists".format(new_code))

        with sqlite3_lci_db.atomic() as txn:
            self.ORMDataset.update(code=new_code).where(self.ORMDataset.id == self.id).execute()
            ExchangeDataset.update(output_code=new_code).where(
                ExchangeDataset.output_database == self["database"],
                ExchangeDataset.output_code == self["code"],
            ).execute()
            ExchangeDataset.update(input_code=new_code).where(
                ExchangeDataset.input_database == self["database"],
                ExchangeDataset.input_code == self["code"],
            ).execute()

        if databases[self["database"]].get("searchable"):
            from bw2data import Database

            IndexManager(Database(self["database"]).filename).delete_dataset(self)
            self._data["code"] = new_code
            IndexManager(Database(self["database"]).filename).add_datasets([self])
        else:
            self._data["code"] = new_code

        if signal:
            on_activity_code_change.send(
                old={"id": self.id, "code": previous},
                new={"id": self.id, "code": new_code},
            )

    def _change_database(self, new_database: str, signal: bool = True):
        if self["database"] == new_database:
            return

        previous = self["database"]

        if new_database not in databases:
            raise ValueError("Database {} does not exist".format(new_database))

        with sqlite3_lci_db.atomic() as txn:
            self.ORMDataset.update(database=new_database).where(
                self.ORMDataset.id == self.id
            ).execute()
            ExchangeDataset.update(output_database=new_database).where(
                ExchangeDataset.output_database == self["database"],
                ExchangeDataset.output_code == self["code"],
            ).execute()
            ExchangeDataset.update(input_database=new_database).where(
                ExchangeDataset.input_database == self["database"],
                ExchangeDataset.input_code == self["code"],
            ).execute()

        if databases[self["database"]].get("searchable"):
            from bw2data import Database

            IndexManager(Database(self["database"]).filename).delete_dataset(self)
            self._data["database"] = new_database
            IndexManager(Database(self["database"]).filename).add_datasets([self])
        else:
            self._data["database"] = new_database

        if signal:
            on_activity_database_change.send(
                old={"id": self.id, "database": previous},
                new={"id": self.id, "database": new_database},
            )

    def exchanges(self, exchanges_class=Exchanges):
        return exchanges_class(self.key)

    def edges(self):
        return self.exchanges()

    def technosphere(self, exchanges_class=Exchanges):
        return exchanges_class(self.key, kinds=labels.technosphere_negative_edge_types)

    def biosphere(self, exchanges_class=Exchanges):
        return exchanges_class(
            self.key,
            kinds=labels.biosphere_edge_types,
        )

    def production(self, include_substitution=False, exchanges_class=Exchanges):
        kinds = labels.technosphere_positive_edge_types
        if not include_substitution:
            kinds = [obj for obj in kinds if obj not in labels.substitution_edge_types]
        return exchanges_class(self.key, kinds=kinds)

    def rp_exchange(self):
        """Return an ``Exchange`` object corresponding to the reference production. Uses the following in order:

        * The ``production`` exchange, if only one is present
        * The ``production`` exchange with the same name as the activity ``reference product``.

        Raises ``ValueError`` if no suitable exchange is found."""
        candidates = list(self.production())
        if len(candidates) == 1:
            return candidates[0]
        candidates2 = [
            exc
            for exc in candidates
            if exc.input._data.get("name") == self._data.get("reference product")
        ]
        if len(candidates2) == 1:
            return candidates2[0]
        else:
            raise ValueError(
                "Can't find a single reference product exchange (found {} candidates)".format(
                    len(candidates)
                )
            )

    def producers(self):
        return self.production()

    def substitution(self, exchanges_class=Exchanges):
        return exchanges_class(
            self.key,
            kinds=labels.substitution_edge_types,
        )

    def upstream(self, kinds=labels.technosphere_negative_edge_types, exchanges_class=Exchanges):
        return exchanges_class(self.key, kinds=kinds, reverse=True)

    def consumers(self, kinds=labels.technosphere_negative_edge_types):
        return self.upstream(kinds=kinds)

    def new_exchange(self, **kwargs):
        return self.new_edge(**kwargs)

    def new_edge(self, **kwargs):
        """Create a new exchange linked to this activity"""
        exc = Exchange()
        exc.output = self.key
        for key, value in kwargs.items():
            if key == "id":
                raise ValueError(f"`id` must be created automatically, but `id={value}` given.")
            exc[key] = value
        return exc

    def copy(self, code: Optional[str] = None, signal: bool = True, **kwargs):
        """Copy the activity. Returns a new `Activity`.

        `code` is the new activity code; if not given, a UUID is used.

        `kwargs` are additional new fields and field values, e.g. name='foo'

        """
        activity = Activity()
        for key, value in self.items():
            if key != "id":
                activity[key] = value
        for key, value in kwargs.items():
            if key == "id":
                raise ValueError(f"`id` must be created automatically, but `id={value}` given.")
            activity._data[key] = value
        activity._data["code"] = str(code or uuid.uuid4().hex)
        activity.save(signal=signal)

        for exc in self.exchanges():
            data = copy.deepcopy(exc._data)
            if "id" in data:
                # New snowflake ID will be inserted by `.save()`; shouldn't be copied over
                # or specified manually
                del data["id"]
            if data["output"] == self.key:
                data["output"] = activity.key
            if exc["input"] == self.key:
                data["input"] = activity.key
            ExchangeDataset(**dict_as_exchangedataset(data)).save(signal=signal)
        return activity


class Exchange(ExchangeProxyBase):
    ORMDataset = ExchangeDataset

    def __init__(self, document=None, **kwargs):
        """Create an `Exchange` proxy object.

        If this is a new exchange, can pass `kwargs`.

        If the exchange exists in the database, `document` should be an `ExchangeDataset`.
        """
        if document is None:
            self._document = self.ORMDataset()
            self._data = kwargs
        else:
            self._document = document
            self._data = self._document.data
            self._data["input"] = (
                self._document.input_database,
                self._document.input_code,
            )
            self._data["output"] = (
                self._document.output_database,
                self._document.output_code,
            )

    @property
    def id(self):
        return self._document.id

    def save(self, signal: bool = True, data_already_set: bool = False, force_insert: bool = False):
        if not data_already_set and not self.valid():
            raise ValidityError(
                "This exchange can't be saved for the "
                "following reasons\n\t* " + "\n\t* ".join(self.valid(why=True)[1])
            )

        databases.set_dirty(self["output"][0])

        if not data_already_set:
            check_exchange_type(self._data.get("type"))
            check_exchange_keys(self)

            for key, value in dict_as_exchangedataset(self._data).items():
                setattr(self._document, key, value)

        self._document.save(signal=signal, force_insert=force_insert)

    def delete(self, signal: bool = True):
        from bw2data.parameters import ParameterizedExchange

        ParameterizedExchange.delete().where(
            ParameterizedExchange.exchange == self._document.id
        ).execute()
        self._document.delete_instance(signal=signal)
        databases.set_dirty(self["output"][0])
        self = None
