class InvalidExchange(Exception):
    """Exchange is missing 'amount' or 'input'"""
    pass


class MissingIntermediateData(Exception):
    pass


class UnknownObject(Exception):
    pass


class UntypedExchange(Exception):
    """Exchange doesn't have 'type' attribute"""
    pass


class WebUIError(Exception):
    """Can't find running instance of bw2-web"""
    pass


class ValidityError(Exception):
    """The activity or exchange dataset does not have all the required fields"""
    pass
