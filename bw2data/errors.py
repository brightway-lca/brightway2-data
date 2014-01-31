class MissingIntermediateData(StandardError):
    pass


class UnknownObject(StandardError):
    pass


class UnsafeData(StandardError):
    """bw2package data comes from a class that isn't recognized by Brightway2"""
    pass


class InvalidPackage(StandardError):
    """bw2package data doesn't validate"""
    pass
