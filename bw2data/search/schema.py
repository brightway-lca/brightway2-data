# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from whoosh.fields import TEXT, ID, Schema

bw2_schema = Schema(
    name=TEXT(stored=True, sortable=True),
    comment=TEXT(stored=True),
    product=TEXT(stored=True, sortable=True),
    categories=TEXT(stored=True),
    location=TEXT(stored=True, sortable=True),
    database=TEXT(stored=True),
    code=ID(unique=True, stored=True),
)
