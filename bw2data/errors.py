# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *


class BW2Exception(Exception):
    """Base class for exceptions in Brightway2"""
    pass


class InvalidExchange(BW2Exception):
    """Exchange is missing 'amount' or 'input'"""
    pass


class MissingIntermediateData(BW2Exception):
    pass


class UnknownObject(BW2Exception):
    pass


class UntypedExchange(BW2Exception):
    """Exchange doesn't have 'type' attribute"""
    pass


class WebUIError(BW2Exception):
    """Can't find running instance of bw2-web"""
    pass


class ValidityError(BW2Exception):
    """The activity or exchange dataset does not have all the required fields"""
    pass

class NotAllowed(BW2Exception):
    """This operation is not allowed"""
    pass

class WrongDatabase(BW2Exception):
    """Can't save activities from database `x` to database `y`."""
    pass


class ReadOnlyProject(BW2Exception):
    """Current project is read only"""
    pass
