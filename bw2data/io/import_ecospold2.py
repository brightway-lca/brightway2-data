# -*- coding: utf-8 -*
from __future__ import division
from .. import Database, databases, mapping
# from ..logs import get_io_logger
from ..units import normalize_units
from lxml import objectify, etree
from stats_arrays.distributions import *
import os
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
            'id': stem.activityDescription.activity.get('id')
        }
        # Purge empties and exchanges with `amount` of zero
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

        is_product = hasattr(exc, "outputGroup") and not is_biosphere

        data = {
            'flow': exc.get(flow),
            'amount': float(exc.get('amount')),
            'biosphere': is_biosphere,
            'product': is_product,
            'name': exc.name.text,
            # 'xml': etree.tostring(exc, pretty_print=True)
        }
        if not is_biosphere and not is_product:
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
        # Note: Creates biosphere3 database
        activities, biosphere, technosphere = Ecospold2DataExtractor().extract(
            self.datapath,
            self.metadatapath
        )
        self.file = open("exchange-weirdness.txt", "w")
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

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            db = Database("biosphere3")
            db.register("Ecospold2", [], len(data))
            db.write(data)
            db.process()

    def create_database(self, biosphere, technosphere, activities):
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
                else:
                    exc['type'] = 'technosphere'
                    exc['input'] = (self.name, exc['activity'])
                if exc['input'][1] is None:
                    exc['input'] = None
                    continue
                    # self.file.write("Activity name: %s\n" % elem['name'])
                    # self.file.write('Flow name: %s\n' % exc['name'])
                    # self.file.write('Filename: %s\n' % elem['filename'])
                    # self.file.write('XML:\n%s\n' % exc['xml'])

        # Drop "missing" exchanges
        for elem in activities:
            elem["exchanges"] = [x for x in elem["exchanges"] if x['input']]

        data = dict([((self.name, elem['id']), elem) for elem in activities])

        if self.name in databases:
            del databases[self.name]

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            db = Database(self.name)
            db.register("Ecospold2", ["biosphere3"], len(data))
            db.write(data)

            # Purge weird exchanges without valid activities
            for value in data.values():
                value['exchanges'] = [x for x in value['exchanges'] if
                                      x['input'] in mapping]

            # Rewrite with correct data
            db.write(data)
            db.process()


