# -*- coding: utf-8 -*
from __future__ import division
from .. import Database, databases, mapping
from ..logs import get_io_logger
from ..units import normalize_units
from lxml import objectify, etree
from stats_arrays.distributions import *
import hashlib
import os
import pprint
import progressbar
import warnings

EMISSIONS = ("air", "water", "soil")
PM_MAPPING = {
    'reliability': 'reliability',
    'completeness': 'completeness',
    'temporalCorrelation': 'temporal correlation',
    'geographicalCorrelation': 'geographical correlation',
    'furtherTechnologyCorrelation': 'further technological correlation'
}


class Ecospold2DataExtractor(object):
    def extract_metadata(self, dirpath):
        for filename in (
            "IntermediateExchanges.xml",
            "ElementaryExchanges.xml"
        ):
            assert os.path.exists(os.path.join(dirpath, filename))
        biosphere = self.extract_biosphere_metadata(dirpath)
        technosphere = self.extract_technosphere_metadata(dirpath)
        return biosphere, technosphere

    def extract_technosphere_metadata(self, dirpath):
        def extract_metadata(o):
            return {
                'name': o.name.text,
                'unit': o.unitName.text,
                'id': o.get('id')
            }

        root = objectify.parse(open(os.path.join(
            dirpath, "IntermediateExchanges.xml"))
        ).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    def extract_biosphere_metadata(self, dirpath):
        def extract_metadata(o):
            return {
                'name': o.name.text,
                'unit': o.unitName.text,
                'id': o.get('id'),
                'categories': (
                    o.compartment.compartment.text,
                    o.compartment.subcompartment.text
                )
            }

        root = objectify.parse(open(os.path.join(
            dirpath, "ElementaryExchanges.xml"))
        ).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    def extract_activities(self, dirpath):
        assert os.path.exists(dirpath)
        filelist = [filename for filename in os.listdir(dirpath)
                    if os.path.isfile(os.path.join(dirpath, filename))
                    and filename.split(".")[-1].lower() == "spold"
                    ]

        widgets = [
            'Extracting activities: ',
            progressbar.Percentage(),
            ' ',
            progressbar.Bar(marker=progressbar.RotatingMarker()),
            ' ',
            progressbar.ETA()]
        pbar = progressbar.ProgressBar(
            widgets=widgets,
            maxval=len(filelist)
        ).start()

        data = []
        for index, filename in enumerate(filelist):
            data.append(self.extract_activity(dirpath, filename))
            pbar.update(index)
        pbar.finish()

        return data

    def extract_activity(self, dirpath, filename):
        root = objectify.parse(open(os.path.join(dirpath, filename))).getroot()
        if hasattr(root, "activityDataset"):
            stem = root.activityDataset
        else:
            stem = root.childActivityDataset
        data = {
            'name': stem.activityDescription.activity.activityName.text,
            'location': stem.activityDescription.geography.shortname.text,
            'exchanges': [self.extract_exchange(exc) for exc in stem.flowData.iterchildren()],
            'filename': filename,
            'activity': stem.activityDescription.activity.get('id')
        }

        candidates = [exc for exc in data['exchanges'] if exc.get('product', False) and exc['amount']]
        assert len(candidates) == 1
        flow = candidates[0]['flow']

        data['id'] = hashlib.md5(data['activity'] + flow).hexdigest()
        data['id_from'] = {
            'activity': data['activity'],
            'flow': flow
        }

        # Purge empties and exchanges with `amount` of zero
        # Excludes parameters, by products (amount of zero),
        # non-allocated reference products (amount of zero)
        data['exchanges'] = [x for x in data['exchanges'] if x and x['amount'] != 0]
        return data

    def extract_exchange(self, exc):
        if exc.tag == u"{http://www.EcoInvent.org/EcoSpold02}intermediateExchange":
            flow = "intermediateExchangeId"
            is_biosphere = False
        elif exc.tag == u"{http://www.EcoInvent.org/EcoSpold02}elementaryExchange":
            flow = "elementaryExchangeId"
            is_biosphere = True
        elif exc.tag == u"{http://www.EcoInvent.org/EcoSpold02}parameter":
            return {}
        else:
            print exc.tag
            raise ValueError

        # Output group 0 is reference product
        # 2 is by-product
        is_product = (not is_biosphere
            and hasattr(exc, "outputGroup")
            and exc.outputGroup.text == "0")
        amount = float(exc.get('amount'))

        if is_product and amount == 0.:
            # This is system modeled multi-output dataset
            # and a "fake" exchange. It represents a possible
            # output which isn't actualized in this allocation
            # and can therefore be ignored. This shouldn't exist
            # at all, but the data format is not perfect.
            return {}

        data = {
            'flow': exc.get(flow),
            'amount': amount,
            'biosphere': is_biosphere,
            'product': is_product,
            'name': exc.name.text,
            # 'xml': etree.tostring(exc, pretty_print=True)
        }
        if not is_biosphere:
            data["activity"] = exc.get("activityLinkId")
        if hasattr(exc, "unitName"):
            data['unit'] = exc.unitName.text

        # Uncertainty fields
        if hasattr(exc, "uncertainty"):
            unc = exc.uncertainty
            if hasattr(unc, "pedigreeMatrix"):
                data['pedigree'] = dict([(
                    PM_MAPPING[key], int(unc.pedigreeMatrix.get(key)))
                    for key in PM_MAPPING
                ])

            if hasattr(unc, "lognormal"):
                if unc.lognormal.get("variance") is not None:
                    variance = float(unc.lognormal.get("variance"))
                elif unc.lognormal.get("varianceWithPedigreeUncertainty"
                                       ) is not None:
                    variance = float(unc.lognormal.get(
                        "varianceWithPedigreeUncertainty"))
                else:
                    variance = None

                data['uncertainty'] = {
                    'type': 'lognormal',
                    'mu': float(unc.lognormal.get('mu')),
                    'sigma': variance
                }
            elif hasattr(unc, 'normal'):
                if unc.normal.get('variance') is not None:
                    variance = float(unc.normal.get('variance'))
                elif unc.normal.get('varianceWithPedigreeUncertainty'
                                    ) is not None:
                    variance = float(unc.normal.get(
                        'varianceWithPedigreeUncertainty'))
                else:
                    variance = None

                data['uncertainty'] = {
                    'type': 'normal',
                    'mu': float(unc.normal.get('meanValue')),
                    'sigma': variance
                }
            elif hasattr(unc, 'triangular'):
                data['uncertainty'] = {
                    'type': 'triangular',
                    'min': float(unc.triangular.get('minValue')),
                    'mode': float(unc.triangular.get('mostLikelyValue')),
                    'mean': float(unc.triangular.get('maxValue'))
                }
            elif hasattr(unc, 'uniform'):
                data['uncertainty'] = {
                    'type': 'uniform',
                    'min': float(unc.uniform.get('minValue')),
                    'max': float(unc.uniform.get('maxValue'))
                }
            elif hasattr(unc, 'undefined'):
                data['uncertainty'] = {'type': 'undefined'}
            else:
                raise ValueError("Unknown uncertainty type")
        else:
            data['uncertainty'] = {'type': 'unknown'}

        return data

    def extract(self, files_dir, meta_dir):
        biosphere, technosphere = self.extract_metadata(meta_dir)
        activities = self.extract_activities(files_dir)
        return activities, biosphere, technosphere


class Ecospold2Importer(object):
    def __init__(self, datapath, metadatapath, name):
        self.datapath = datapath
        self.metadatapath = metadatapath
        self.name = name

    def importer(self):
        self.log, self.logfile = get_io_logger("es3-import")
        # Note: Creates biosphere3 database
        activities, biosphere, technosphere = Ecospold2DataExtractor().extract(
            self.datapath,
            self.metadatapath
        )
        self.create_biosphere3_database(biosphere)
        self.create_database(biosphere, technosphere, activities)

    def create_biosphere3_database(self, data):
        for elem in data:
            elem["unit"] = normalize_units(elem["unit"])
            elem["type"] = "emission" if elem['categories'][0] in EMISSIONS \
                else elem['categories'][0]
            elem["exchanges"] = []

        data = dict([(("biosphere3", x["id"]), x) for x in data])

        if "biosphere3" in databases:
            del databases["biosphere3"]

        print "Writing new biosphere database"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            db = Database("biosphere3")
            db.register("Ecospold2", [], len(data))
            db.write(data)
            db.process()

    def create_database(self, biosphere, technosphere, activities):
        print "Processing database"
        for elem in activities:
            elem["unit"] = ""
            elem["type"] = "product"
            for exc in elem["exchanges"]:
                exc['uncertainty type'] = 0
                if exc['product']:
                    exc['type'] = 'production'
                    exc['input'] = (self.name, elem['id'])
                elif exc['biosphere']:
                    exc['type'] = 'biosphere'
                    exc['input'] = ('biosphere3', exc['flow'])
                elif exc['activity'] is None:
                    # This exchange wasn't linked correctly by ecoinvent
                    # It is missing the "activityLinkId" attribute
                    # See http://www.ecoinvent.org/database/ecoinvent-version-3/reports-of-changes/known-data-issues/
                    # We ignore it for now, but add attributes to log it later
                    exc['input'] = None
                    exc['activity filename'] = elem['filename']
                    exc['activity name'] = elem['name']
                else:
                    exc['type'] = 'technosphere'
                    exc['input'] = (
                        self.name,
                        hashlib.md5(exc['activity'] + exc['flow']).hexdigest()
                    )

        # Drop "missing" exchanges
        for elem in activities:
            for exc in [x for x in elem["exchanges"] if not x['input']]:
                self.log.warning(u"Dropped missing exchange: %s" % \
                    pprint.pformat(exc, indent=2))
            elem["exchanges"] = [x for x in elem["exchanges"] if x['input']]

        data = dict([((self.name, elem['id']), elem) for elem in activities])

        if self.name in databases:
            del databases[self.name]

        print "Writing new database"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            db = Database(self.name)
            db.register("Ecospold2", ["biosphere3"], len(data))
            db.write(data)

            # Purge any exchanges without valid activities
            rewrite = False
            for value in data.values():
                for exc in [x for x in value['exchanges'] \
                        if x['input'] not in mapping]:
                    rewrite = True
                    self.log.critical(u"Purging unlinked exchange:\n%s" % \
                        pprint.pformat(exc, indent=2))
                value['exchanges'] = [x for x in value['exchanges'] if
                                      x['input'] in mapping]

            if rewrite:
                # Rewrite with correct data
                db.write(data)
            db.process()


