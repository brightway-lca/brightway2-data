import itertools
from collections.abc import Iterable, Mapping
from types import GeneratorType
from typing import Optional
from warnings import warn

import numpy as np
import pandas as pd
from bw_processing import Datapackage

from ...utils import get_node
from ..proxies import Activity, Exchange


class ReadOnlyExchange(Mapping):
    """Dictionary which mimics ``bw2data.proxies.Exchange``, but is read-only and doesn't link to a SQLite database row."""

    __lt__ = Exchange.__lt__
    __contains__ = Exchange.__contains__
    __iter__ = Exchange.__iter__
    __len__ = Exchange.__len__
    __getitem__ = Exchange.__getitem__
    __eq__ = Exchange.__eq__
    __hash__ = Exchange.__hash__

    # Get unit from input node
    unit = Exchange.unit

    lca = Exchange.lca
    as_dict = Exchange.as_dict

    def __str__(self):
        return "Exchange: {} {} {} to {}>".format(
            self.amount, self.input['unit'], self.input, self.output
        )

    def __init__(self, **kwargs):
        self.valid(dct=kwargs)
        input_id = kwargs.pop("input")
        output_id = kwargs.pop("output")
        self.input = get_node(id=input_id)
        self.output = get_node(id=output_id)
        self.amount = kwargs["amount"]
        self._data = kwargs
        self._data["input"] = self.input.key
        self._data["output"] = self.output.key

    def valid(self, dct: dict = None) -> None:
        if dct is None:
            dct = self._data
        REQUIRED = {"input", "output", "amount", "type"}
        for key in REQUIRED:
            if key not in dct:
                raise ValueError(f"Missing required input key {key}")
                if key in {"input", "output"}:
                    if not isinstance(dct[key], int):
                        raise ValueError(f"{key} must be integer")


class IOTableExchanges(Iterable):
    def __init__(
        self,
        datapackage: Datapackage,
        target: Optional[Activity] = None,
        biosphere: bool = True,
        technosphere: bool = True,
        production: bool = True,
    ):
        """Iterable of ``ReadOnlyExchange`` objects drawn from Datapackage arrays.

        In the *technosphere*, all positive exchanges are considered *production*, and all negative exchanges are *technosphere*, i.e. consumption.

        The order of returned edges are production, technosphere, biosphere.

        This function will draw from all resources with the correct matrix types (i.e. ``'biosphere_matrix'``, ``'technosphere_matrix'``). Normally each IO Table database is stored in only one datapackage, and each datapackage only has one such database.

        * ``datapackage``: The datapackage object.
        * ``target``: Limit exchanges to those with the column index ``target``. Target must be an instance of ``IOTableActivity``.
        * ``biosphere``, ``technosphere``, ``production``: Return these types of edges.

        """
        if not any((technosphere, production, biosphere)):
            raise ValueError(
                "Must include some edges from `technosphere`, `production`, and `biosphere`"
            )
        INCLUDE = [
            "technosphere_matrix" if any((technosphere, production)) else None,
            "biosphere_matrix" if biosphere else None,
        ]
        resources = [
            {
                obj["kind"]: obj
                for obj in group
                if obj["matrix"] in INCLUDE and obj["category"] == "vector"
            }
            for _, group in itertools.groupby(
                datapackage.resources, lambda x: x["group"]
            )
        ]
        resources = [obj for obj in resources if obj]

        # Adjust signs using `flip`
        for resource in resources:
            if "flip" in resource:
                flip_arr = datapackage.get_resource(resource["flip"]["name"])[0]
                flip_int_arr = np.ones_like(flip_arr)
                flip_int_arr[flip_arr] = -1

                data_ind = datapackage._get_index(resource["data"]["name"])
                datapackage.data[data_ind] = datapackage.data[data_ind] * flip_int_arr

        if target is not None:
            for resource in resources:
                indices_ind = datapackage._get_index(resource["indices"]["name"])
                data_ind = datapackage._get_index(resource["data"]["name"])

                indices_arr = datapackage.data[indices_ind]
                mask = indices_arr["col"] == target.id

                datapackage.data[indices_ind] = indices_arr[mask]
                datapackage.data[data_ind] = datapackage.data[data_ind][mask]

        for resource in resources:
            resource["data"]["array"] = datapackage.get_resource(
                resource["data"]["name"]
            )[0]
            resource["indices"]["array"] = datapackage.get_resource(
                resource["indices"]["name"]
            )[0]

        self.resources = resources
        self.datapackage = datapackage
        self.technosphere = technosphere
        self.production = production
        self.biosphere = biosphere

    def __iter__(self):
        for row, col, value in self._raw_technosphere_iterator(negative=False):
            yield ReadOnlyExchange(
                input=row,
                output=col,
                amount=value,
                uncertainty_type=0,
                type="production",
            )
        for row, col, value in self._raw_technosphere_iterator(negative=True):
            yield ReadOnlyExchange(
                input=row,
                output=col,
                amount=value,
                uncertainty_type=0,
                type="technosphere",
            )
        for row, col, value in self._raw_biosphere_iterator():
            yield ReadOnlyExchange(
                input=row,
                output=col,
                amount=value,
                uncertainty_type=0,
                type="biosphere",
            )

    def _raw_technosphere_iterator(self, negative=True):
        tm = lambda x: any(obj["matrix"] == "technosphere_matrixmatrix" for obj in x.values())
        for resource in filter(tm, self.resources):
            for (row, col), value in zip(
                resource["indices"]["array"], resource["data"]["array"]
            ):
                if (value < 0) == negative:
                    yield (row, col, value)

    def _raw_biosphere_iterator(self):
        bm = lambda x: any(obj["matrix"] == "biosphere_matrix" for obj in x.values())
        for resource in filter(bm, self.resources):
            for (row, col), value in zip(
                resource["indices"]["array"], resource["data"]["array"]
            ):
                yield (row, col, value)

    def __next__(self):
        raise NotImplementedError

    def __len__(self):
        return sum(len(resource["data"]["array"]) for resource in self.resources)

    # def to_dataframe(self, ascending=False, fields=None):

    #     # default columns to include
    #     if fields is None:
    #         fields = [
    #             "name",
    #             "location/category/compartment",
    #             "amount",
    #             "unit",
    #             "exchange type",
    #             "reference product",
    #         ]

    #     # load exchange data
    #     df = pd.DataFrame(
    #         {
    #             "database": e["input"][0],
    #             "code": e["input"][1],
    #             "amount": e["amount"],
    #             "exchange type": e["type"],
    #         }
    #         for e in self.data
    #     ).set_index(["database", "code"])

    #     # load activity metadata
    #     df_meta = Storage.construct_or_load_metadata(
    #         df.index.get_level_values("database").unique()
    #     )

    #     # join both into one dataframe
    #     # sort values
    #     df = df.join(df_meta, how="left").sort_values("amount", ascending=ascending)
    #     # merge location, categories and compartments into one column
    #     df["location/category/compartment"] = df["location"]
    #     if "categories" in df:
    #         df["location/category/compartment"].fillna(df["categories"], inplace=True)
    #     if "compartment" in df:
    #         df["location/category/compartment"].fillna(df["compartment"], inplace=True)

    #     # filter fields and return
    #     return df[fields]


class IOTableActivity(Activity):
    def delete(self) -> None:
        # TBD; needs to rewrite arrays so not so simple...
        raise NotImplementedError

    def rp_exchange(self):
        # Need to raise `ValueError` so that lookups for `classification`,
        # `properties` etc. will raise correct error.
        # See super.__getitem__ code for details
        raise ValueError("Not defined for IO Table activities")

    def _get_correct_db_backend(self):
        from ...database import DatabaseChooser

        db = DatabaseChooser(self["database"])
        if db.backend != "iotable":
            raise ValueError(
                "`IOTableActivity` must be used with IO Table backend activities"
            )
        return db

    def technosphere(self) -> IOTableExchanges:
        db = self._get_correct_db_backend()
        return IOTableExchanges(
            technosphere=True,
            biosphere=False,
            production=False,
            target=self,
            datapackage=db.datapackage(),
        )

    def biosphere(self):
        db = self._get_correct_db_backend()
        return IOTableExchanges(
            technosphere=False,
            biosphere=True,
            production=False,
            target=self,
            datapackage=db.datapackage(),
        )

    def production(self):
        db = self._get_correct_db_backend()
        return IOTableExchanges(
            technosphere=False,
            biosphere=False,
            production=True,
            target=self,
            datapackage=db.datapackage(),
        )

    def exchanges(self):
        # Order is production, technosphere, biosphere
        db = self._get_correct_db_backend()
        return IOTableExchanges(
            technosphere=True,
            biosphere=True,
            production=True,
            target=self,
            datapackage=db.datapackage(),
        )

    def substitution(self):
        warn(
            "IO Table doesn't store exchange types, only numeric data. All positive technosphere edges are `production`, all negative technosphere edges are `technosphere`. Returning an empty iterator."
        )
        return iter([])
