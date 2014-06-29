class InvalidExchange(StandardError):
    """Exchange is missing 'amount' or 'input'"""
    pass


class InvalidPackage(StandardError):
    """bw2package data doesn't validate"""
    pass


class MissingIntermediateData(StandardError):
    pass


class UnknownObject(StandardError):
    pass


class UnsafeData(StandardError):
    """bw2package data comes from a class that isn't recognized by Brightway2"""
    pass


class UntypedExchange(StandardError):
    """Exchange doesn't have 'type' attribute"""
    pass


class WebUIError(StandardError):
    """Can't find running instance of bw2-web"""
    pass
