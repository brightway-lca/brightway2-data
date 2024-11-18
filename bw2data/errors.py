class BW2Exception(Exception):
    """Base class for exceptions in Brightway2"""

    pass


class InvalidExchange(BW2Exception):
    """Exchange is missing 'amount' or 'input'"""

    pass


class DuplicateNode(BW2Exception):
    """Can't have nodes with same unique identifiers"""

    pass


class MissingIntermediateData(BW2Exception):
    pass


class UnknownObject(BW2Exception):
    pass


class MultipleResults(BW2Exception):
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


class NotFound(BW2Exception):
    """Requested web resource not found"""

    pass


class PickleError(BW2Exception):
    """Pickle file can't be loaded due to updated library file structure"""

    pass


class Brightway2Project(BW2Exception):
    """This project is not yet migrated to Brightway 2.5"""

    pass


class InvalidDatapackage(BW2Exception):
    """The given datapackage can't be used for the requested task."""

    pass


class IncompatibleClasses(BW2Exception):
    """Revision comparison across two different classes doesn't make sense and isn't allowed"""

    pass


class DifferentObjects(BW2Exception):
    """Revision comparison of two different objects doesn't make sense and isn't allowed"""

    pass


class InconsistentData(BW2Exception):
    """Attempted a change on data which was in an inconsistent state with the changeset."""

    pass


class PossibleInconsistentData(BW2Exception):
    """Attempted a change on data which was in an inconsistent state with the changeset."""

    pass


class NoRevisionNeeded(BW2Exception):
    """No revision needed given the presented previous and current data"""

    pass
