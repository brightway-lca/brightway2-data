import itertools
from collections.abc import Iterable, Mapping
from typing import Optional
from warnings import warn

import numpy as np
from bw_processing import Datapackage

from bw2data.backends.proxies import Activity, Exchange, Exchanges
from bw2data.configuration import labels
from bw2data.errors import InvalidDatapackage
from bw2data.utils import get_node


class ReadOnlyExchange(Mapping):
    """Non-mutable dictionary which mimics ``bw2data.proxies.Exchange``, but is read-only and doesn't link to a SQLite database row."""

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

    def __lt__(self, other):
        if not isinstance(other, ReadOnlyExchange):
            raise TypeError
        else:
            return (self.input.key, self.output.key) < (
                other.input.key,
                other.output.key,
            )

    def __str__(self):
        return "Exchange: {} {} {} to {}>".format(
            self.amount, self.input.get("unit"), self.input, self.output
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
    to_dataframe = Exchanges.to_dataframe

    def __init__(
        self,
        datapackage: Datapackage,
        target: Optional[Activity] = None,
        biosphere: bool = True,
        technosphere: bool = True,
        production: bool = True,
    ):
        """Iterable of ``ReadOnlyExchange`` objects drawn from Datapackage arrays.

        In the *technosphere matrix*, all positive exchanges are considered *production*, and all negative exchanges are *technosphere*, i.e. consumption, and we use this convention to label the edges. However, to be consistent with SQLite database results, we don't flip signs in the returned dataframe.

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

        if hasattr(datapackage, "filtered") and (
            (target is None) or (datapackage.filtered != target.id)
        ):
            raise InvalidDatapackage(
                "This datapackage was already filtered to a different node. Please load it again."
            )

        resources = self._group_and_filter_resources(datapackage)
        self._add_arrays_to_resources(resources, datapackage)
        resources = self._reduce_arrays_to_selected_types(
            resources, technosphere, production, biosphere
        )

        if target is not None:
            datapackage.filtered = target.id
            for resource in resources:
                mask = resource["indices"]["array"]["col"] == target.id
                self._mask_resource_arrays(resource, mask)

        self.resources = resources
        self.datapackage = datapackage
        self.technosphere = technosphere
        self.production = production
        self.biosphere = biosphere

    def _group_and_filter_resources(self, datapackage):
        resources = [
            {obj["kind"]: obj for obj in group if obj["category"] == "vector"}
            for _, group in itertools.groupby(datapackage.resources, lambda x: x["group"])
        ]
        return [obj for obj in resources if obj]

    def _add_arrays_to_resources(self, resources, datapackage):
        for resource in resources:
            resource["data"]["array"] = datapackage.get_resource(resource["data"]["name"])[0]
            resource["indices"]["array"] = datapackage.get_resource(resource["indices"]["name"])[0]
            if "flip" in resource:
                resource["flip"]["array"] = datapackage.get_resource(resource["flip"]["name"])[0]
            else:
                resource["flip"] = {"array": np.zeros_like(resource["data"]["array"], dtype=bool)}

            # Add array indicating if values are positive after combining data and flip
            positive_arr = np.ones_like(resource["flip"]["array"], dtype=int)
            positive_arr[resource["flip"]["array"]] = -1
            resource["flip"]["positive"] = (resource["data"]["array"] * positive_arr) >= 0

    def _reduce_arrays_to_selected_types(self, resources, technosphere, production, biosphere):
        if not biosphere:
            resources = [
                resource
                for resource in resources
                if resource["data"]["matrix"] == "technosphere_matrix"
            ]
        elif not (technosphere or production):
            resources = [
                resource
                for resource in resources
                if resource["data"]["matrix"] == "biosphere_matrix"
            ]
        else:
            resources = [
                resource
                for resource in resources
                if resource["data"]["matrix"] in ("biosphere_matrix", "technosphere_matrix")
            ]

        if technosphere != production:
            for resource in resources:
                if resource["data"]["matrix"] != "technosphere_matrix":
                    continue
                elif technosphere:
                    self._mask_resource_arrays(resource, ~resource["flip"]["positive"])
                else:
                    self._mask_resource_arrays(resource, resource["flip"]["positive"])
        return resources

    def _mask_resource_arrays(self, resource, mask):
        resource["data"]["array"] = resource["data"]["array"][mask]
        resource["indices"]["array"] = resource["indices"]["array"][mask]
        resource["flip"]["array"] = resource["flip"]["array"][mask]
        resource["flip"]["positive"] = resource["flip"]["positive"][mask]

    def __iter__(self):
        for row, col, value in self._raw_technosphere_iterator(negative=False):
            yield ReadOnlyExchange(
                input=row,
                output=col,
                amount=value,
                uncertainty_type=0,
                type=labels.production_edge_default,
            )
        for row, col, value in self._raw_technosphere_iterator(negative=True):
            yield ReadOnlyExchange(
                input=row,
                output=col,
                amount=value,
                uncertainty_type=0,
                type=labels.consumption_edge_default,
            )
        for row, col, value in self._raw_biosphere_iterator():
            yield ReadOnlyExchange(
                input=row,
                output=col,
                amount=value,
                uncertainty_type=0,
                type=labels.biosphere_edge_default,
            )

    def _raw_technosphere_iterator(self, negative=True):
        tm = lambda x: any(obj.get("matrix") == "technosphere_matrix" for obj in x.values())
        for resource in filter(tm, self.resources):
            for (row, col), value, positive_flag in zip(
                resource["indices"]["array"],
                resource["data"]["array"],
                resource["flip"]["positive"],
            ):
                if positive_flag != negative:
                    yield (row, col, value)

    def _raw_biosphere_iterator(self):
        bm = lambda x: any(obj.get("matrix") == "biosphere_matrix" for obj in x.values())
        for resource in filter(bm, self.resources):
            for (row, col), value in zip(resource["indices"]["array"], resource["data"]["array"]):
                yield (row, col, value)

    def __next__(self):
        raise NotImplementedError

    def __len__(self):
        return sum([len(resource["data"]["array"]) for resource in self.resources])


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
        from bw2data.database import DatabaseChooser

        db = DatabaseChooser(self["database"])
        if db.backend != "iotable":
            raise ValueError("`IOTableActivity` must be used with IO Table backend activities")
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
