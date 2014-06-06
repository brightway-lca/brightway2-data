from ...serialization import PickledDict

cache = {}


class KeyMapping(PickledDict):
    filename = "keys-filenames.mapping"


def get_mapping(filepath):
    if filepath not in cache:
        cache[filepath] = KeyMapping(filepath)
    return cache[filepath]
