# -*- coding: utf-8 -*
from __future__ import division, print_function
from .. import Database, databases, mapping
from ..logs import get_io_logger, close_log
from ..units import normalize_units
from ..utils import recursive_str_to_unicode
from lxml import objectify
from stats_arrays.distributions import *
import copy
import hashlib
import os
import pprint
import progressbar
import warnings

EMISSIONS = (u"air", u"water", u"soil")
PM_MAPPING = {
    u'reliability': u'reliability',
    u'completeness': u'completeness',
    u'temporalCorrelation': u'temporal correlation',
    u'geographicalCorrelation': u'geographical correlation',
    u'furtherTechnologyCorrelation': u'further technological correlation'
}
KNOWN_ISSUES = {
    u"waste packaging glass, unsorted",
    u"heat, future",
    u"heat, central or small-scale, other than natural gas",
    u"natural gas, low pressure",
    u"roundwood, azobe from sustainable forest management, CM, debarked",
}


class Ecospold2DataExtractor(object):

    @classmethod
    def extract_metadata(cls, dirpath):
        for filename in (
            u"IntermediateExchanges.xml",
            u"ElementaryExchanges.xml"
        ):
            assert os.path.exists(os.path.join(dirpath, filename))
        biosphere = cls.extract_biosphere_metadata(dirpath)
        technosphere = cls.extract_technosphere_metadata(dirpath)
        return biosphere, technosphere

    @classmethod
    def extract_technosphere_metadata(cls, dirpath):
        def extract_metadata(o):
            return {
                u'name': o.name.text,
                u'unit': normalize_units(o.unitName.text),
                u'id': o.get(u'id')
            }

        root = objectify.parse(open(os.path.join(
            dirpath, u"IntermediateExchanges.xml"))
        ).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    @classmethod
    def extract_biosphere_metadata(cls, dirpath):
        def extract_metadata(o):
            return {
                u'name': o.name.text,
                u'unit': normalize_units(o.unitName.text),
                u'id': o.get(u'id'),
                u'categories': (
                    o.compartment.compartment.text,
                    o.compartment.subcompartment.text
                )
            }

        root = objectify.parse(open(os.path.join(
            dirpath, u"ElementaryExchanges.xml"))
        ).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    @classmethod
    def extract_activities(cls, dirpath, multioutput=False):
        assert os.path.exists(dirpath)
        filelist = [filename for filename in os.listdir(dirpath)
                    if os.path.isfile(os.path.join(dirpath, filename))
                    and filename.split(".")[-1].lower() == u"spold"
                    ]

        widgets = [
            progressbar.SimpleProgress(sep="/"), " (",
            progressbar.Percentage(), ') ',
            progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
            progressbar.ETA()
        ]
        pbar = progressbar.ProgressBar(
            widgets=widgets,
            maxval=len(filelist)
        ).start()

        data = []
        for index, filename in enumerate(filelist):
            data.append(cls.extract_activity(dirpath, filename, multioutput))
            pbar.update(index)
        pbar.finish()

        return data

    @classmethod
    def extract_activity(cls, dirpath, filename, multioutput=False):
        root = objectify.parse(open(os.path.join(dirpath, filename))).getroot()
        if hasattr(root, u"activityDataset"):
            stem = root.activityDataset
        else:
            stem = root.childActivityDataset
        data = {
            u'name':      stem.activityDescription.activity.activityName.text,
            u'location':  stem.activityDescription.geography.shortname.text,
            u'exchanges': [cls.extract_exchange(exc, multioutput)
                           for exc in stem.flowData.iterchildren()],
            u'linking': {
                u'activity':  stem.activityDescription.activity.get('id'),
                u'filename':  filename,
            },
        }
        data[u'products'] = [copy.deepcopy(exc)
            for exc in data[u'exchanges']
            if exc.get(u'product', 0)
        ]

        # Multi-output datasets, when allocated, keep all product exchanges,
        # but set some amounts to zero...
        ref_product_candidates = [exc for exc in data[u'products'] if exc[u'amount']]
        # Allocation datasets only have one product actually produced
        assert len(ref_product_candidates) == 1
        data[u"linking"][u'flow'] = ref_product_candidates[0][u'flow']

        # Despite using a million UUIDs, there is actually no unique ID in
        # an ecospold2 dataset. Datasets are uniquely identified by the
        # combination of activity and flow UUIDs.
        data[u'id'] = hashlib.md5(data[u"linking"][u'activity'] +
            data[u"linking"][u'flow']).hexdigest()
        data[u'reference product'] = ref_product_candidates[0][u'name']

        # Purge parameters (where `extract_exchange` returns `{}`)
        # and exchanges with `amount` of zero
        # and byproducts (amount of zero),
        # and non-allocated reference products (amount of zero)
        # Multioutput products are kept in 'products'
        data[u'exchanges'] = [
            x for x in data[u'exchanges']
            if x
            and x[u'amount'] != 0
        ]
        return data

    @classmethod
    def extract_exchange(cls, exc, multioutput=False):
        if exc.tag == u"{http://www.EcoInvent.org/EcoSpold02}intermediateExchange":
            flow = "intermediateExchangeId"
            is_biosphere = False
        elif exc.tag == u"{http://www.EcoInvent.org/EcoSpold02}elementaryExchange":
            flow = "elementaryExchangeId"
            is_biosphere = True
        elif exc.tag == u"{http://www.EcoInvent.org/EcoSpold02}parameter":
            return {}
        else:
            print(exc.tag)
            raise ValueError

        # Output group 0 is reference product
        #              2 is by-product
        is_product = (not is_biosphere
                      and hasattr(exc, u"outputGroup")
                      and exc.outputGroup.text == u"0")
        amount = float(exc.get(u'amount'))

        if is_product and amount == 0. and not multioutput:
            # This is system modeled multi-output dataset
            # and a "fake" exchange. It represents a possible
            # output which isn't actualized in this allocation
            # and can therefore be ignored. This shouldn't exist
            # at all, but the data format is not perfect.
            return {}

        data = {
            u'flow': exc.get(flow),
            u'amount': amount,
            u'biosphere': is_biosphere,
            u'product': is_product,
            u'name': exc.name.text,
            u'production volume': float(exc.get("productionVolumeAmount") or 0)
            # 'xml': etree.tostring(exc, pretty_print=True)
        }
        if not is_biosphere:
            # Biosphere flows not produced by an activity
            data[u"activity"] = exc.get(u"activityLinkId")
        if hasattr(exc, u"unitName"):
            data[u'unit'] = normalize_units(exc.unitName.text)

        # Uncertainty fields
        if hasattr(exc, u"uncertainty"):
            unc = exc.uncertainty
            if hasattr(unc, u"pedigreeMatrix"):
                data[u'pedigree'] = dict([(
                    PM_MAPPING[key], int(unc.pedigreeMatrix.get(key)))
                    for key in PM_MAPPING
                ])

            if hasattr(unc, "lognormal"):
                data.update(**{
                    u'uncertainty type': LognormalUncertainty.id,
                    u"loc": float(unc.lognormal.get('mu')),
                    u"scale": float(unc.lognormal.get("varianceWithPedigreeUncertainty")),
                })
                if unc.lognormal.get('variance'):
                    data[u"scale without pedigree"] = float(unc.lognormal.get('variance'))
            elif hasattr(unc, u'normal'):
                data.update(**{
                    u"uncertainty type": NormalUncertainty.id,
                    u"loc": float(unc.normal.get('meanValue')),
                    u"scale": float(unc.normal.get('varianceWithPedigreeUncertainty')),
                })
                if unc.normal.get('variance'):
                    data[u"scale without pedigree"] = float(unc.normal.get('variance'))
            elif hasattr(unc, u'triangular'):
                data.update(**{
                    u'uncertainty type': TriangularUncertainty.id,
                    u'minimum': float(unc.triangular.get('minValue')),
                    u'loc': float(unc.triangular.get('mostLikelyValue')),
                    u'maximum': float(unc.triangular.get('maxValue'))
                })
            elif hasattr(unc, u'uniform'):
                data.update(**{
                    u"uncertainty type": UniformUncertainty.id,
                    u"loc": data[u'amount'],
                    u'minimum': float(unc.uniform.get('minValue')),
                    u'maximum': float(unc.uniform.get('maxValue')),
                })
            elif hasattr(unc, u'undefined'):
                data.update(**{
                    u"uncertainty type": UndefinedUncertainty.id,
                    u"loc": data[u'amount'],
                })
            else:
                raise ValueError(u"Unknown uncertainty type")
        else:
            data.update(**{
                u"uncertainty type": UndefinedUncertainty.id,
                u"loc": data[u'amount'],
            })

        return data

    @classmethod
    def extract(cls, files_dir, meta_dir, multioutput=False):
        biosphere, technosphere = cls.extract_metadata(meta_dir)
        activities = cls.extract_activities(files_dir, multioutput)
        return activities, biosphere, technosphere


class Ecospold2Importer(object):

    def __init__(self, datapath, metadatapath, name, multioutput=False, debug=False):
        """Create a new ecospold2 importer object.

        .. warning:: You should always check the import log after an ecospold 2 import, because the background database could have missing links that will produce incorrect LCI results.

        Usage: ``Ecospold2Importer(**args).importer()``

        Args:
            * *datapath*: Absolute filepath to directory containing the datasets.
            * *metadatapath*: Absolute filepath to the *"MasterData"* directory.
            * *name*: Name of the created database.
            * *multioutput*: Boolean. When importing allocated datasets, include the other outputs in a special *"products"* list.
            * *debug*: Boolean. Include additional debugging information.

        The data schema for ecospold2 databases is slightly different from ecospold1 databases, as there is some additional data included (only additional data shown here):

        .. code-block:: python

            {
                'linking': {
                    'activity': uuid,  # System model-specific activity UUID (location/time specific)
                    'flow': uuid,  # System model-specific UUID of the reference product flow (location/time specific)
                    'filename': str  # Dataset filename
                },
                'production amount': float,  # Not all activities in ecoinvent 3 are scaled to produce one unit of the reference product
                'products': [
                    {exchange_dict},  # List of products. Only has length > 1 if *multioutput* is True. Products which aren't the reference product will have amounts of zero.
                ],
                'reference product': str  # Name of the reference product. Ecospold2 distinguishes between activity and product names.
            }


        Where an exchange in the list of exchanges includes the following additional fields:

        .. code-block:: python

            {
                'production amount': float,  # Yearly production amount in this location and time
                'pedigree matrix': {  # Pedigree matrix values in a structured format
                    'completeness': int,
                    'further technological correlation': int,
                    'geographical correlation': int,
                    'reliability': int,
                    'temporal correlation': int
                }
            }

        """
        self.datapath = unicode(datapath)
        self.metadatapath = unicode(metadatapath)
        self.multioutput = multioutput
        self.debug = debug
        if name in databases:
            raise AttributeError(u"Database {} already registered".format(name))
        self.name = unicode(name)

    def importer(self):
        self.log, self.logfile = get_io_logger("es3-import")

        self.log.info(u"Starting ecospold2 import." + \
            u"\n\tDatabase name: %s" % self.name    + \
            u"\n\tDatapath: %s" % self.datapath     + \
            u"\n\tMetadatapath: %s" % self.metadatapath)

        try:
            activities, biosphere, technosphere = Ecospold2DataExtractor.extract(
                self.datapath,
                self.metadatapath,
                self.multioutput,
            )

            # XML is encoded in UTF-8, but we want unicode strings
            activities = recursive_str_to_unicode(activities)
            biosphere = recursive_str_to_unicode(biosphere)
            technosphere = recursive_str_to_unicode(technosphere)

            self.create_biosphere3_database(biosphere)
            self.create_database(biosphere, technosphere, activities)

            print(u"Ecospold2 database imported successfully. "
                  u"Please check the logfile:\n\t" + self.logfile)
            close_log(self.log)

        except:
            self.log.critical(u"ERROR: Aborting import.")
            close_log(self.log)
            print(u"ERROR: Please check the logfile:\n\t" + self.logfile)
            raise

    def create_biosphere3_database(self, data):
        for elem in data:
            elem[u"type"] = "emission" if elem[u'categories'][0] in EMISSIONS \
                else elem[u'categories'][0]
            elem[u"exchanges"] = []

        data = dict([((u"biosphere3", x[u"id"]), x) for x in data])

        if u"biosphere3" in databases:
            del databases[u"biosphere3"]

        print(u"Writing new biosphere database")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            db = Database(u"biosphere3")
            db.register(
                format=u"Ecospold2",
                depends=[],
                num_processes=len(data)
            )
            db.write(data)
            db.process()

    def create_database(self, biosphere, technosphere, activities):
        print(u"Processing database")
        for elem in activities:
            for exc in elem[u"exchanges"]:
                if exc[u'product']:
                    exc[u'type'] = u'production'
                    exc[u'input'] = (self.name, elem[u'id'])
                    # Activities do not have units, per se - products have units. However,
                    # it is nicer to give the unit of the reference product than nothing.
                    assert "unit" not in elem
                    elem[u"unit"] = exc[u'unit']
                elif exc[u'biosphere']:
                    exc[u'type'] = 'biosphere'
                    exc[u'input'] = (u'biosphere3', exc[u'flow'])
                elif exc[u'activity'] is None:
                    # This exchange wasn't linked correctly by ecoinvent
                    # It is missing the "activityLinkId" attribute
                    # See http://www.ecoinvent.org/database/ecoinvent-version-3/reports-of-changes/known-data-issues/
                    # We ignore it for now, but add attributes to log it later
                    exc[u'input'] = None
                    exc[u'activity filename'] = elem[u'filename']
                    exc[u'activity name'] = elem[u'name']
                    exc[u'type'] = u'unknown'
                    exc[u'unlinked'] = True
                else:
                    # Normal input from technosphere
                    exc[u'type'] = u'technosphere'
                    exc[u'input'] = (
                        self.name,
                        hashlib.md5(exc[u'activity'] + exc[u'flow']).hexdigest()
                    )

            assert "unit" in elem

        # Drop "missing" exchanges
        for elem in activities:
            for exc in [
                    x for x in elem[u"exchanges"]
                    if not x[u'input']
            ]:
                if exc[u'name'] in KNOWN_ISSUES:
                    self.log.info(u"Dropped known missing exchange: {}".format(
                        exc[u'name']))
                else:
                    self.log.warning(u"Dropped missing exchange: %s" %
                                     pprint.pformat(exc, indent=2))

            elem[u"exchanges"] = [
                x for x in elem[u"exchanges"]
                if x[u'input']
            ]

        # Set production amount
        for elem in activities:
            if len(elem[u'products']) > 1:
                elem[u'production amount'] = None
            else:
                elem[u'production amount'] = elem[u'products'][0][u'amount']

        data = dict([((self.name, elem[u'id']), elem) for elem in activities])

        if not self.debug:
            # Remove 'product' and 'biosphere' keys from exchange dictionaries
            for ds in data.values():
                del ds[u'id']
                for exc in ds.get(u"exchanges", []):
                    del exc[u"product"]
                    del exc[u"biosphere"]
                    del exc[u"flow"]
                    if u"activity" in exc:
                        del exc[u"activity"]
                for product in ds.get(u"products", []):
                    del product[u"product"]
                    del product[u"biosphere"]
                    del product[u"activity"]

        print(u"Writing new database")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            db = Database(self.name)
            db.register(
                format=u"Ecospold2",
                depends=[u"biosphere3"],
                num_processes=len(data)
            )
            db.write(data)

            # Purge any exchanges which link to ghost activities
            # i.e. those not created by the import
            rewrite = False
            for value in data.values():
                for exc in [x for x in value[u'exchanges']
                            if x[u'input'] not in mapping]:
                    rewrite = True
                    self.log.critical(
                        u"Purging unlinked exchange:\nFilename: %s\n%s" % \
                        (value[u'filename'], pprint.pformat(exc, indent=2))
                    )
                value[u'exchanges'] = [x for x in value[u'exchanges'] if
                                      x[u'input'] in mapping]

            if rewrite:
                # Rewrite with correct data
                db.write(data)
            db.process()
