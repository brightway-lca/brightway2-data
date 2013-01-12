# -*- coding: utf-8 -*
from .. import Database, Method, methods
from ..logs import get_io_logger
from lxml import objectify
import numpy as np
import os
import pprint
import progressbar
import warnings
try:
    import cPickle as pickle
except:
    import pickle


class EcospoldImpactAssessmentImporter(object):
    """Import impact assessment methods and weightings from ecospold XML format.

Does not have any arguments; instead, instantiate the class, and then import using the ``importer`` method, i.e. ``EcospoldImpactAssessmentImporter().importer(filepath)``."""
    def importer(self, path):
        """Import an impact assessment method, or a directory of impact assessment methods.

        The flow logic is relatively complex, because:
            #. We have to make sure the ``number`` attribute is not just a sequential list
            #. Even if valid biosphere ``number``s are provided, we can't believe them.

        Here is the flow logic graphic:

        .. image:: images/import-method.png
            :align: center

        Args:
            *path* (str): A filepath or directory.

        """
        if os.path.isdir(path):
            files = [os.path.join(path, name) for name in \
                filter(lambda x: x[-4:].lower() == ".xml", os.listdir(path))]
        else:
            files = [path]

        self.new_flows = False
        self.log, self.logfile = get_io_logger("lcia-import")

        try:
            self.biosphere_data = Database("biosphere").load()
        except:
            # Biosphere not loaded
            raise ValueError("Can't find biosphere database; check configuration.")

        self.max_code = max(50000, max([x[1] for x in self.biosphere_data]))

        if progressbar:
            widgets = ['Files: ', progressbar.Percentage(), ' ',
                progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
                progressbar.ETA()]
            pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(files)
                ).start()

        for index, filepath in enumerate(files):
            # Note that this is only used for the first root method found in
            # the file
            root = objectify.parse(open(filepath)).getroot()
            for dataset in root.iterchildren():
                self.add_method(dataset)
            pbar.update(index)

        pbar.finish()

    def add_method(self, ds):
        ref_func = ds.metaInformation.processInformation.referenceFunction
        name = (ref_func.get("category"), ref_func.get("subCategory"),
            ref_func.get("name"))
        assert name not in methods
        description = ref_func.get("generalComment") or ""
        unit = ref_func.get("unit") or ""

        # Check if codes are sequential
        codes = np.array([int(cf.get("number")) for cf in ds.flowData.iterchildren()])
        sequential = np.allclose(np.diff(codes), np.ones(np.diff(codes).shape))

        if sequential:
            data = self.add_sequential_cfs(ds)
        else:
            data = self.add_nonsequential_cfs(ds)

        if self.new_flows:
            biosphere = Database("biosphere")
            biosphere.write(self.biosphere_data)
            biosphere.process()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            method = Method(name)
            method.register(unit, description, len(data))
            method.write(data)
            method.process()

    def add_sequential_cfs(self, ds):
        data = {}
        for cf in ds.flowData.iterchildren():
            cf_data = self.get_cf_data(cf, ignore_code=True)
            cf_data = self.match_biosphere_by_attrs(cf_data)
            data[("biosphere", cf_data["code"])] = float(cf.get("meanValue"))
        return data

    def add_nonsequential_cfs(self, ds):
        data = {}
        for cf in ds.flowData.iterchildren():
            cf_data = self.get_cf_data(cf)
            cf_data = self.code_in_biosphere(cf_data)
            data[("biosphere", cf_data["code"])] = float(cf.get("meanValue"))
        return data

    def match_biosphere_by_attrs(self, cf):
        found = False
        for key, value in self.biosphere_data.iteritems():
            if self.verify_attrs(cf, value):
                found = key[1]
                break
        if found:
            cf["code"] = found
            return cf
        else:
            cf["code"] = self.get_new_code()
            self.log_info(cf)
            self.add_flow(cf)
            return cf

    def get_new_code(self):
        self.max_code += 1
        return self.max_code

    def verify_attrs(self, cf, bio):
        return bio["name"].lower() == cf["name"].lower() and \
            list(bio["categories"]) == cf["categories"]

    def code_in_biosphere(self, cf):
        key = ("biosphere", cf["code"])
        if key in self.biosphere_data:
            if self.verify_attrs(cf, self.biosphere_data[key]):
                return cf
            else:
                self.log_warning(cf)
        return self.match_biosphere_by_attrs(cf)

    def log_warning(self, cf):
        error_message = "Found biosphere flow with same code but conflicting attributes:\n"
        error_message += "\tExisting version:\n"
        error_message += pprint.pformat(self.biosphere_data[("biosphere", cf["code"])])
        error_message += "\tNew version:\n"
        error_message += pprint.pformat(cf)
        self.log.warning(error_message)

    def log_info(self, cf):
        log_message = "Adding new biosphere flow:\n"
        log_message += pprint.pformat(cf)
        self.log.info(log_message)

    def get_int_code(self, cf):
        try:
            return int(cf.get("number"))
        except:
            raise ValueError("Can't convert `number` attribute to number")

    def get_cf_data(self, cf, ignore_code=False):
        data = {
            "name": cf.get("name"),
            "categories": [cf.get("category"),
                cf.get("subCategory") or "unspecified"],
            "unit": cf.get("unit"),
            "exchanges": []
        }
        # Convert ("foo", "unspecified") to ("foo",)
        while data["categories"][-1] == "unspecified":
            data["categories"] = data["categories"][:-1]
        if not ignore_code:
            data["code"] = self.get_int_code(cf)
        return data

    def add_flow(self, cf):
        """Add new biosphere flow"""
        # Emission or resource
        resource = cf["categories"][0] == "resource"
        cf["type"] = "resource" if resource else "emission"
        self.new_flows = True
        self.biosphere_data[("biosphere", cf["code"])] = cf
