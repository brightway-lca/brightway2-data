# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import projects,mapping
from .filesystem import md5
from .utils import MAX_INT_32, TYPE_DICTIONARY, numpy_string
import json
import numpy as np
import os
import uuid


def create_presample_package(params, data, kind, id_=None, mapped=False):
    """Create a new subdirectory in the ``project`` folder that stores the presampled values.

    A presampled directory that holds three files:

    * ``{id_}.samples.npy``: Exchange information to locate the presampled values in LCA matrices.
    * ``{id_}.samples.npy``: Array of the presampled values.
    * ``datapackage.json``: Metadata about the two files above, including integrity checks.

    Input parameters:

    * ``params``: An iterable whose format depends on ``kind``. For inventory params, the format is a list of (input activity, output activity, string label for exchange type), e.g. ``[("my database", "an input"), ("my database", "an output"), "technosphere"]``. For charaterization factor params, the format is a list of flow identifiers, e.g. ``[('biosphere3', 'f9055607-c571-4903-85a8-7c20e3790c43')]``.
    * ``kind``: One of ``{"inventory", "cfs"}``.
    * ``id_``: Unique id for this collection of presamples. Optional, generated automatically if not set.
    * ``mapped``: Are the arrays in ``params`` already mapped to integers using ``mapping``. Boolean, default ``False``.

    Returns ``id_`` and the absolute path of the created directory.

    """
    inv_dtype = [
        (numpy_string('input'), np.uint32),
        (numpy_string('output'), np.uint32),
        (numpy_string('row'), np.uint32),
        (numpy_string('col'), np.uint32),
        (numpy_string('type'), np.uint8),
    ]
    ia_dtype = [
        (numpy_string('flow'), np.uint32),
        (numpy_string('row'), np.uint32),
    ]
    dtype_mapping = {
        'inventory': inv_dtype,
        'cfs': ia_dtype,
    }
    assert kind in dtype_mapping

    if id_ is None:
        id_ = uuid.uuid4().hex

    base_dir = os.path.join(projects.request_directory('presamples'), id_)
    if os.path.isdir(base_dir):
        raise ValueError("This presampled directory already exists")
    os.mkdir(base_dir)

    params_arr = np.zeros(len(data), dtype=dtype_mapping[kind])

    _ = lambda x: x if mapped else mapping[x]

    for i, row in enumerate(params):
        if kind == "inventory":
            processed = (_(row[0]), _(row[1]), MAX_INT_32, MAX_INT_32, TYPE_DICTIONARY[row[2]])
        elif kind == 'cfs':
            processed = (_(row), MAX_INT_32)
        params_arr[i] = processed

    params_fp = os.path.join(base_dir, "{}.params.npy".format(id_))
    data_fp = os.path.join(base_dir, "{}.samples.npy".format(id_))

    np.save(params_fp, params_arr, allow_pickle=False)
    np.save(data_fp, data.astype(np.float32), allow_pickle=False)

    datapackage = {
        "id": id_,
        "profile": "data-package",
        "type": kind,
        "resources": [{
            "name": "samples",
            "path": "{}.samples.npy".format(id_),
            "profile": "data-resource",
            "format": "npy",
            "mediatype": "application/octet-stream",
            "hash": md5(data_fp),
            "dtype": "float32",
            "shape": data.shape
        }, {
            "name": "parameters",
            "path": "{}.params.npy".format(id_),
            "profile": "data-resource",
            "format": "npy",
            "mediatype": "application/octet-stream",
            "hash": md5(params_fp),
        }]
    }
    with open(os.path.join(base_dir, "datapackage.json"), "w") as f:
        json.dump(datapackage, f)

    return id_, base_dir
