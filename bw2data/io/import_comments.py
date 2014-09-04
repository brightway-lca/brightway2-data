# -*- coding: utf-8 -*
from .. import Database, databases
from ..errors import UnknownObject
from ..units import normalize_units
from ..utils import activity_hash, recursive_str_to_unicode
from lxml import objectify
import hashlib
import os
import progressbar

widgets = [
    progressbar.SimpleProgress(sep="/"), " (",
    progressbar.Percentage(), ') ',
    progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
    progressbar.ETA()
]

def getattr2(obj, attr):
    try:
        return getattr(obj, attr)
    except:
        return {}


class Ecospold1CommentExtractor(object):
    """Extract comments from a directory of ecospold 1 XML files.

    Usage: `Ecospold1CommentExtractor().extract(filepath)`.

    Returns the a list of `(dataset id, comment, dataset data)` tuples."""
    def extract(self, path):
        data = []
        if os.path.isdir(path):
            files = [os.path.join(path, y) for y in filter(
                lambda x: x[-4:].lower() == ".xml", os.listdir(path))]
        else:
            files = [path]

        if not files:
            raise OSError("Provided path doesn't appear to have any XML files")

        pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(files)
            ).start()

        for index, filename in enumerate(files):
            root = objectify.parse(open(filename)).getroot()

            if root.tag not in (
                    '{http://www.EcoInvent.org/EcoSpold01}ecoSpold',
                    'ecoSpold'):
                continue

            for dataset in root.iterchildren():
                data.append(self.process_dataset(dataset))

            pbar.update(index)
        pbar.finish()
        return data

    def process_dataset(self, dataset):
        ref_func = dataset.metaInformation.processInformation.\
            referenceFunction
        comments = [
            ref_func.get("generalComment"),
            ref_func.get("includedProcesses"),
            (u"Location: ", dataset.metaInformation.processInformation.geography.get("text")),
            (u"Technology: ", dataset.metaInformation.processInformation.technology.get("text")),
            (u"Time period: ", getattr2(dataset.metaInformation.processInformation, "timePeriod").get("text")),
            (u"Production volume: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("productionVolume")),
            (u"Sampling: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("samplingProcedure")),
            (u"Extrapolations: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("extrapolations")),
            (u"Uncertainty: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("uncertaintyAdjustments")),
        ]
        comment = "\n".join([
            (" ".join(x) if isinstance(x, tuple) else x)
            for x in comments
            if (x[1] if isinstance(x, tuple) else x)
        ])

        hash_data = {
            "name": ref_func.get("name").strip(),
            "categories": [ref_func.get("category"), ref_func.get(
                "subCategory")],
            "location": dataset.metaInformation.processInformation.\
                geography.get("location"),
            "unit": normalize_units(ref_func.get("unit")),
        }

        return (activity_hash(hash_data), recursive_str_to_unicode(comment), hash_data)


class Ecospold2CommentExtractor(object):
    """Extract comments from a directory of ecospold 2 SPOLD files.

    Usage: `Ecospold2CommentExtractor().extract(filepath)`.

    Returns the a list of `(filename, comment)` tuples."""
    def extract(self, dirpath):
        data = []
        filelist = [filename for filename in os.listdir(dirpath)
                    if os.path.isfile(os.path.join(dirpath, filename))
                    and filename.split(".")[-1].lower() == u"spold"
                    ]

        pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(filelist)
            ).start()

        data = []
        for index, filename in enumerate(filelist):
            root = objectify.parse(open(os.path.join(dirpath, filename))).getroot()
            data.append((filename, self.process_dataset(root)))
            pbar.update(index)

        pbar.finish()
        return data

    def condense_multiline_comment(self, element):
        try:
            return u"\n".join([
                child.text for child in element.iterchildren()
                if child.tag == u"{http://www.EcoInvent.org/EcoSpold02}text"]
            )
        except:
            return u""

    def process_dataset(self, root):
        if hasattr(root, u"activityDataset"):
            stem = root.activityDataset
        else:
            stem = root.childActivityDataset

        comments = [
            self.condense_multiline_comment(getattr2(stem.activityDescription.activity, u"generalComment")),
            (u"Included activities start: ", getattr2(stem.activityDescription.activity, u"includedActivitiesStart").get(u"text")),
            (u"Included activities end: ", getattr2(stem.activityDescription.activity, u"includedActivitiesEnd").get(u"text")),
            (u"Geography: ", self.condense_multiline_comment(getattr2(stem.activityDescription.geography, u"comment"))),
            (u"Technology: ", self.condense_multiline_comment(getattr2(stem.activityDescription.technology, u"comment"))),
            (u"Time period: ", self.condense_multiline_comment(getattr2(stem.activityDescription.timePeriod, u"comment"))),
        ]
        comment = "\n".join([
            (" ".join(x) if isinstance(x, tuple) else x)
            for x in comments
            if (x[1] if isinstance(x, tuple) else x)
        ])

        return recursive_str_to_unicode(comment)


def add_ecospold1_comments(name, filepath=None):
    """Add comments from ecospold version 1 XML files.

    Args:
        * `name` (unicode): Name of database to add comments.
        * `filepath` (unicode, optional): Filepath of XML files to extract comments from, if not specified in database metadata.

    Doesn't return anything.
    """
    if name not in databases:
        raise UnknownObject(u"Database %s not registered" % name)
    if not filepath and u"directory" not in databases[name]:
        raise ValueError(u"Specify dataset directory with `filepath` parameter.")
    extractor = Ecospold1CommentExtractor()
    db = Database(name)
    data = db.load()
    for key, comment, activity in extractor.extract(filepath or databases[name][u"directory"]):
        assert (name, key) in data
        data[(name, key)][u'comment'] = comment
    db.write(data)
    db.process()


def add_ecospold2_comments(name, filepath=None):
    """Add comments from ecospold version 2 SPOLD files.

    Args:
        * `name` (unicode): Name of database to add comments.
        * `filepath` (unicode, optional): Filepath of XML files to extract comments from, if not specified in database metadata.

    Doesn't return anything.
    """
    if name not in databases:
        raise UnknownObject(u"Database %s not registered" % name)
    if not filepath and u"directory" not in databases[name]:
        raise ValueError(u"Specify dataset directory with `filepath` parameter.")
    extractor = Ecospold2CommentExtractor()
    db = Database(name)
    data = db.load()
    filename_mapping = {value[u'linking'][u'filename']: key for key, value in data.items()}
    for filename, comment in extractor.extract(filepath or databases[name][u"directory"]):
        data[filename_mapping[filename]][u'comment'] = comment
    db.write(data)
    db.process()
