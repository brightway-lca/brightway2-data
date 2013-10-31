# -*- coding: utf-8 -*
from __future__ import division
from .. import Database, mapping, config
from ..logs import get_io_logger
from ..utils import activity_hash
from ..units import normalize_units
from lxml import objectify
try:
    from stats_arrays.distributions import *
except ImportError:
    LognormalUncertainty = None
import copy
import math
import numpy as np
import os
import pprint
import progressbar
import warnings

BIOSPHERE = ("air", "water", "soil", "resource")


class Ecospold1DataExtractor(object):
    def extract(self, path, log):
        data = []
        if os.path.isdir(path):
            files = [os.path.join(path, y) for y in filter(
                lambda x: x[-4:].lower() == ".xml", os.listdir(path))]
        else:
            files = [path]
        widgets = ['Extracting data: ', progressbar.Percentage(), ' ',
            progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
            progressbar.ETA()]
        pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(files)
            ).start()

        for index, filename in enumerate(files):
            root = objectify.parse(open(filename)).getroot()

            if root.tag not in (
                    '{http://www.EcoInvent.org/EcoSpold01}ecoSpold',
                    'ecoSpold'):
                # Unrecognized file type
                log.critical(u"skipping %s - no ecoSpold element" % filename)
                continue

            for dataset in root.iterchildren():
                data.append(self.process_dataset(dataset))

            pbar.update(index)
        pbar.finish()
        return data

    def process_dataset(self, dataset):
        ref_func = dataset.metaInformation.processInformation.\
            referenceFunction
        data = {
            "name": ref_func.get("name"),
            "type": "process",  # True for all ecospold?
            "categories": [ref_func.get("category"), ref_func.get(
                "subCategory")],
            "location": dataset.metaInformation.processInformation.\
                geography.get("location"),
            "code": int(dataset.get("number")),
            "unit": normalize_units(ref_func.get("unit")),
            "exchanges": self.process_exchanges(dataset)
            }
        # Convert ("foo", "unspecified") to ("foo",)
        while data["categories"] and data["categories"][-1] in (
                "unspecified", None):
            data["categories"] = data["categories"][:-1]
        return data

    def process_exchanges(self, dataset):
        data = []
        # Skip definitional exchange - we assume this already
        for exc in dataset.flowData.iterchildren():
            if exc.tag in (
                    "{http://www.EcoInvent.org/EcoSpold01}exchange",
                    "exchange"):
                data.append(self.process_exchange(exc, dataset))
            elif exc.tag in (
                    "{http://www.EcoInvent.org/EcoSpold01}allocation",
                    "allocation"):
                data.append(self.process_allocation(exc, dataset))
            else:
                raise ValueError("Flow data type %s not understood" % exc.tag)
        return data

    def process_allocation(self, exc, dataset):
        return {
            "reference": int(exc.get("referenceToCoProduct")),
            "fraction": float(exc.get("fraction")),
            "exchanges": [int(c.text) for c in exc.iterchildren()]
        }

    def process_exchange(self, exc, dataset):
        # if exc.get("name") == dataset.metaInformation.processInformation.\
        #         referenceFunction.get("name") != None and float(
        #         exc.get("meanValue", 0.)) == 1.0:
        #     continue

        data = {
            "code": int(exc.get("number")),
            "matching": {
                "categories": (exc.get("category"), exc.get("subCategory")),
                "location": exc.get("location"),
                "unit": normalize_units(exc.get("unit")),
                "name": exc.get("name")
                }
            }

        try:
            data["group"] = int(exc.getchildren()[0].text)
        except:
            pass

        # Convert ("foo", "unspecified") to ("foo",)
        while data["matching"]["categories"] and \
                data["matching"]["categories"][-1] in ("unspecified", None):
            data["matching"]["categories"] = \
                data["matching"]["categories"][:-1]

        if exc.get("generalComment"):
            data["comment"] = exc.get("generalComment")
        return self.process_uncertainty_fields(exc, data)

    def process_uncertainty_fields(self, exc, data):
        uncertainty = int(exc.get("uncertaintyType", 0))

        def floatish(x):
            try:
                return float(x)
            except:
                return np.NaN

        mean = floatish(exc.get("meanValue"))
        min_ = floatish(exc.get("minValue"))
        max_ = floatish(exc.get("maxValue"))
        sigma = floatish(exc.get("standardDeviation95"))

        if uncertainty == 1 and sigma in (0, 1):
            # Bad data
            uncertainty = 0

        if uncertainty == 1:
            # Lognormal
            data.update({
                'uncertainty type': LognormalUncertainty.id,
                'amount': float(mean),
                'loc': np.log(np.abs(mean)),
                'scale': math.log(math.sqrt(float(sigma))),
                'negative': mean < 0,
            })
            if np.isnan(data['scale']):
                # Bad data
                data['uncertainty type'] = UndefinedUncertainty.id
                data['loc'] = data['amount']
                del data["scale"]
        elif uncertainty == 2:
            # Normal
            data.update({
                'uncertainty type': NormalUncertainty.id,
                'amount': float(mean),
                'loc': float(mean),
                'scale': float(sigma) / 2
            })
        elif uncertainty == 3:
            # Triangular
            data.update({
                'uncertainty type': TriangularUncertainty.id,
                'minimum': float(min_),
                'maximum': float(max_)
            })
            # Sometimes this isn't included (though it SHOULD BE)
            if exc.get("mostLikelyValue"):
                mode = floatish(exc.get("mostLikelyValue"))
                data['amount'] = data['loc'] = mode
            else:
                data['amount'] = data['loc'] = float(mean)
        elif uncertainty == 4:
            # Uniform
            data.update({
                'uncertainty type': UniformUncertainty.id,
                'amount': float(mean),
                'minimum': float(min_),
                'maximum': float(max_)
                })
        else:
            # None
            data.update({
                'uncertainty type': UndefinedUncertainty.id,
                'amount': float(mean),
                'loc': float(mean),
            })
        return data


class Ecospold1Importer(object):
    """Import inventory datasets from ecospold XML format.

    Does not have any arguments; instead, instantiate the class, and then import using the ``importer`` method, i.e. ``Ecospold1Importer().importer(filepath)``.

    """
    def importer(self, path, name, depends=[config.biosphere]):
        """Import an inventory dataset, or a directory of inventory datasets.

        .. image:: images/import-method.png
            :align: center

        Args:
            *path* (str): A filepath or directory.

        """

        if LognormalUncertainty is None:
            print "``stats_array`` not installed!"
            return

        self.log, self.logfile = get_io_logger("lci-import")
        self.new_activities = []
        self.new_biosphere = []

        data = Ecospold1DataExtractor().extract(path, self.log)
        data = self.allocate_datasets(data)
        data = self.apply_transforms(data)
        data = self.add_hashes(data)

        if not data:
            self.log.critical("No data found in XML file %s" % path)
            warnings.warn("No data found in XML file %s" % path)
            return

        widgets = ['Linking exchanges:', progressbar.Percentage(), ' ',
            progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
            progressbar.ETA()]
        pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(data)
            ).start()

        linked_data = []
        for index, ds in enumerate(data):
            linked_data.append(self.link_exchanges(ds, data, depends, name))
            pbar.update(index)
        pbar.finish()

        data = linked_data + self.new_activities

        if self.new_biosphere:
            self.new_biosphere = dict([((config.biosphere, o.pop("hash")), o) \
                for o in self.new_biosphere])
            biosphere = Database(config.biosphere)
            biosphere_data = biosphere.load()
            biosphere_data.update(self.new_biosphere)
            biosphere.write(biosphere_data)
            # biosphere.process()

        data = self.set_exchange_types(data)
        data = self.clean_exchanges(data)
        # Dictionary constructor eliminates duplicate activities
        data = dict([((name, o.pop("hash")), o) for o in data])
        self.write_database(name, data, depends)

    def allocate_datasets(self, data):
        activities = []
        for ds in data:
            multi_output = [exc for exc in ds["exchanges"] \
                if "reference" in exc]
            if multi_output:
                for activity in self.allocate_exchanges(ds):
                    activities.append(activity)
            else:
                activities.append(ds)
        return activities

    def allocate_exchanges(self, ds):
        """
Take a dataset, which has multiple outputs, and return a list of allocated datasets.

Two things change in the allocated datasets. First, the name changes to the names of the individual outputs. Second, the list of exchanges is rewritten, and only the allocated exchanges are used.

The list of ``exchanges`` looks like:

.. code-block:: python

    [
        {
        "code": 2,
        "matching": {
            "categories": data,
            "location": data,
            "unit": data,
            "name": name
            },
        "uncertainty type": data
        "amount": reference amount,
        "group": 2
        }, {
        "code": 4,
        "matching": {data},
        "uncertainty type": data
        "amount": 1,
        "group": 5
        }, {
        "reference": 2,
        "fraction": 0.5,
        "exchanges": [4,]
        }
    ]

It should be changed to:

.. code-block:: python

    [
        {
        "code": 2,
        "matching": {
            "categories": data,
            "location": data,
            "unit": data,
            "name": name
            },
        "uncertainty type": data
        "amount": number,
        "group": 2
        }, {
        "code": 4,
        "matching": {data},
        "uncertainty type": data
        "amount": 0.5,
        "group": 5
        }
    ]

Exchanges should also be copied and allocated for any other co-products.

        """
        coproduct_codes = [exc["code"] for exc in ds["exchanges"] if exc.get(
            "group", None) == 2]
        coproducts = dict([(x, copy.deepcopy(ds)) for x in coproduct_codes])
        exchanges = dict([(exc["code"], exc) for exc in ds["exchanges"
            ] if "code" in exc])
        allocations = [a for a in ds["exchanges"] if "fraction" in a]
        # First, get production amounts for each coproduct.
        # these aren't included in the allocations
        for key, product in coproducts.iteritems():
            product["exchanges"] = [exc for exc in product["exchanges"] if exc.get("code", None) == key]
        # Next, correct names, location, and unit
        for key, product in coproducts.iteritems():
            for label in ("unit", "name", "location"):
                if exchanges[key]["matching"].get(label, None):
                    product[label] = exchanges[key]["matching"][label]
        # Finally, add the allocated exchanges
        for allocation in allocations:
            if allocation["fraction"] == 0:
                continue
            product = coproducts[allocation["reference"]]
            for exc_code in allocation["exchanges"]:
                copied = copy.deepcopy(exchanges[exc_code])
                copied["amount"] = copied["amount"] * allocation["fraction"]
                product["exchanges"].append(copied)
        return coproducts.values()

    def apply_transforms(self, data):
        # Reserved for sublcasses, e.g. SimaPro import
        # where some cleaning is necessary...
        return data

    def add_hashes(self, ds):
        for o in ds:
            o["hash"] = activity_hash(o)
        return ds

    def link_exchanges(self, ds, data, depends, name):
        if self.sequential_exchanges(ds):
            del ds["code"]
        ds["exchanges"] = [self.link_exchange(exc, ds, data, depends, name
            ) for exc in ds["exchanges"]]
        return ds

    def sequential_exchanges(self, ds):
        codes = np.array([x["code"] for x in ds["exchanges"]])
        return np.allclose(np.diff(codes), np.ones(np.diff(codes).shape))

    def link_exchange(self, exc, ds, data, depends, name):
        # Has to happen before others because US LCI doesn't define categories
        # for product definitions...
        if exc.get("group", None) == 0:
            # Activity dataset production
            exc["input"] = (name, activity_hash(ds))
            return exc
        # Hack for US LCI-specific bug - both "Energy recovered"
        # and "Energy, recovered" are present
        elif exc["matching"]["categories"] == () and \
                exc["matching"]["name"] == "Recovered energy":
            exc["matching"].update(
                name="Energy, recovered",
                categories=("resource",),
                )
        elif not exc["matching"]["categories"]:
            # US LCI doesn't list categories, subcategories for
            # technosphere inputs. Try to find based on name. Need to lowercase
            # because US LCI is not consistent within itself (!!!)
            for other_ds in data:
                if other_ds["name"].lower() == \
                        exc["matching"]["name"].lower():
                    exc["input"] = (name, other_ds["hash"])
                    return exc
            # Can't find matching process - but could be a US LCI "dummy"
            # activity
            if exc["matching"]["name"][:5].lower() == "dummy":
                self.log.warning(u"New activity created by:\n%s" % \
                    pprint.pformat(exc))
                exc["input"] = (name, self.create_activity(exc["matching"]))
                return exc
            else:
                raise ValueError("Exchange can't be matched:\n%s" % \
                    pprint.pformat(exc))
        exc["hash"] = activity_hash(exc["matching"])
        if exc["matching"].get("categories", [None])[0] in BIOSPHERE:
            return self.link_biosphere(exc)
        else:
            return self.link_activity(exc, ds, data, depends, name)

    def link_biosphere(self, exc):
        exc["input"] = ("biosphere", exc["hash"])
        if (u"biosphere", exc["hash"]) in Database("biosphere").load():
            return exc
        else:
            new_flow = copy.deepcopy(exc["matching"])
            new_flow.update({
                "hash": activity_hash(exc["matching"]),
                "type": "resource" if new_flow["categories"][0] == "resource" \
                    else "emission",
                "exchanges": []
                })
            # Biosphere flows don't have locations
            del new_flow["location"]
            self.new_biosphere.append(new_flow)
            return exc

    def link_activity(self, exc, ds, data, depends, name):
        if exc["hash"] in [o["hash"] for o in data]:
            exc["input"] = (name, exc["hash"])
            return exc
        else:
            return self.link_activity_dependent_database(exc, depends, name)

    def link_activity_dependent_database(self, exc, depends, name):
        for database in depends:
            if (database, exc["hash"]) in mapping:
                exc["input"] = (database, exc["hash"])
                return exc
        # Create new activity in this database and log
        self.log.warning(u"New activity created by:\n%s" % pprint.pformat(exc))
        exc["input"] = (name, self.create_activity(exc["matching"]))
        return exc

    def create_activity(self, exc):
        exc = copy.deepcopy(exc)
        exc.update({
            "exchanges": [],
            "type": "process",
            "hash": activity_hash(exc),
            })
        self.new_activities.append(exc)
        return exc["hash"]

    def set_exchange_types(self, data):
        """Set the ``type`` attribute for each exchange, one of either (``production``, ``technosphere``, ``biosphere``). ``production`` defines the amount produced by the activity dataset (default is 1)."""
        for ds in data:
            for exc in ds["exchanges"]:
                if exc["input"][0] == config.biosphere:
                    exc["type"] = "biosphere"
                elif exc["input"][1] == ds["hash"]:
                    exc["type"] = "production"
                else:
                    exc["type"] = "technosphere"
        return data

    def clean_exchanges(self, data):
        for ds in data:
            for exc in ds["exchanges"]:
                if "matching" in exc:
                    del exc["matching"]
                if "hash" in exc:
                    del exc["hash"]
        return data

    def write_database(self, name, data, depends):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            manager = Database(name)
            manager.register(("Ecospold", 1), depends, len(data))
            manager.write(data)
            manager.process()
