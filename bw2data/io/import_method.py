# -*- coding: utf-8 -*
from .. import Database, mapping, Method, methods, databases
from lxml import objectify
import hashlib
import os
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

        Args:
            *path* (str): A filepath or directory.

        """
        if os.path.isdir(path):
            files = [os.path.join(path, name) for name in \
                filter(lambda x: x[-4:].lower() == ".xml", os.listdir(path))]
        else:
            files = [path]

        self.new_flows = {}

        try:
            self.biosphere_data = Database("biosphere").load()
        except:
            # Biosphere not loaded
            raise ValueError("Can't find biosphere database; check configuration.")
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
        description = ref_func.get("generalComment") or ""
        unit = ref_func.get("unit") or ""
        data = {}
        for cf in ds.flowData.iterchildren():
            code = self.get_code(cf)
            if ("biosphere", code) not in mapping:
                self.add_flow(cf, code)
            data[("biosphere", code)] = float(cf.get("meanValue"))
        assert name not in methods

        if self.new_flows:
            biosphere = Database("biosphere")
            bio_data = biosphere.load()
            bio_data.update(self.new_flows)
            biosphere.write(bio_data)
            biosphere.process()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            method = Method(name)
            method.register(unit, description, len(data))
            method.write(data)
            method.process()

    def get_code(self, cf):
        try:
            int_code = int(cf.get("number"))
            assert int_code
            return int_code
        except:
            hasher = hashlib.md5()
            cat_string = "-".join([cf.get("category"), cf.get("subCategory")])
            hasher.update(cat_string)
            return hasher.hexdigest()[-8:]

    def add_flow(self, cf, code):
        """Add new biosphere flow"""
        new_flow = {
            "name": cf.get("name"),
            "categories": (cf.get("category"),
                cf.get("subCategory") or "unspecified"),
            "code": code,
            "unit": cf.get("unit"),
            "exchanges": []
        }

        # Convert ("foo", "unspecified") to ("foo",)
        while new_flow["categories"][-1] == "unspecified":
            new_flow["categories"] = new_flow["categories"][:-1]

        # Emission or resource
        resource = new_flow["categories"][0] == "resource"
        new_flow["type"] = "resource" if resource else "emission"
        self.new_flows[("biosphere", code)] = new_flow
