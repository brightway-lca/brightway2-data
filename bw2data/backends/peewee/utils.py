# -*- coding: utf-8 -*-
from __future__ import unicode_literals


def dict_as_activitydataset(ds):
    return {
        "data": ds,
        "database": ds["database"],
        "key": (ds["database"], ds["code"]),
        "location": ds.get("location"),
        "name": ds.get("name"),
        "product": ds.get("reference product"),
        "type": ds.get("type", "process"),
    }


def dict_as_exchangedataset(ds):
    return {
        "data": ds,
        "input": ds['input'],
        "output": ds['output'],
        "database": ds['output'][0],
        "type": ds['type']
    }
