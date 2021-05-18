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


class NotFound(BW2Exception):
    """Requested web resource not found"""

    pass


class PickleError(BW2Exception):
    """Pickle file can't be loaded due to updated library file structure"""

    pass


class Brightway2Project(BW2Exception):
    """This project is not yet migrated to Brightway 2.5"""

    pass
