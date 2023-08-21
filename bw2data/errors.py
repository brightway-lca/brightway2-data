"""Exceptions for bw2data."""


class BW2Exception(BaseException):
    """Base class for exceptions in Brightway2"""


class InvalidExchange(BW2Exception):
    """Exchange is missing 'amount' or 'input'"""


class DuplicateNode(BW2Exception):
    """Can't have nodes with same unique identifiers"""


class MissingIntermediateData(BW2Exception):
    """"""


class UnknownObject(BW2Exception):
    """"""


class MultipleResults(BW2Exception):
    """"""


class UntypedExchange(BW2Exception):
    """Exchange doesn't have 'type' attribute"""


class ValidityError(BW2Exception):
    """The activity or exchange dataset does not have all the required fields"""


class WrongDatabase(BW2Exception):
    """Can't save activities from database `x` to database `y`."""


class PickleError(BW2Exception):
    """Pickle file can't be loaded due to updated library file structure"""


class InvalidDatapackage(BW2Exception):
    """The given datapackage can't be used for the requested task."""
