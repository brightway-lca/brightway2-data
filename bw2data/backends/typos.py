from typing import Iterable
import warnings
from functools import partial

try:
    import Levenshtein
except ImportError:
    Levenshtein = None


VALID_ACTIVITY_TYPES = (
    "process",
    "emission",
    "natural resource",
    "product",
    "economic",
    "inventory indicator",
)
VALID_EXCHANGE_TYPES = (
    'biosphere',
    'production', 'substitution', 'generic production',
    'technosphere', 'generic consumption',
)
VALID_ACTIVITY_KEYS = (
    'CAS number',
    'activity',
    'activity type',
    'authors',
    'categories',
    'classifications',
    'code',
    'comment',
    'database',
    'exchanges',
    'filename',
    'flow',
    'id',
    'location',
    'name',
    'parameters',
    'production amount',
    'reference product',
    'synonyms',
    'type',
    'unit',
)
VALID_EXCHANGE_KEYS = (
    'activity',
    'amount',
    'classifications',
    'comment',
    'flow',
    'input',
    'loc',
    'maximum',
    'minimum',
    'name',
    'output',
    'pedigree',
    'production volume',
    'properties',
    'scale',
    'scale without pedigree',
    "shape",
    "temporal_distribution",
    'type',
    'uncertainty type',
    'uncertainty_type',
    'unit',
)

# TBD: What is reasonable for uncertainty_| type?


def _check_type(type_value: str, kind: str, valid: Iterable[str]) -> None:
    if not Levenshtein:
        return

    if type_value and type_value not in valid and isinstance(type_value, str):
        possibles = sorted(
            [
                (Levenshtein.distance(type_value, possible), possible)
                for possible in valid
            ]
        )
        possibles = [(x, y) for x, y in possibles if x < 3]
        if possibles:
            warnings.warn(
                f"Possible typo found: Given {kind} type `{type_value}` but "
                + f"`{possibles[0][1]}` is more common"
            )


check_activity_type = partial(_check_type, valid=VALID_ACTIVITY_TYPES, kind="activity")
check_exchange_type = partial(_check_type, valid=VALID_EXCHANGE_TYPES, kind="exchange")


def _check_keys(obj: dict, kind: str, valid: Iterable[str]) -> None:
    if not Levenshtein:
        return

    for key in obj:
        if key not in valid and isinstance(key, str):
            possibles = [
                (Levenshtein.distance(key, possible), possible)
                for possible in valid
            ]
            possibles = sorted([(x, y) for x, y in possibles if x < 3 and len(y) > x + 1])
            if possibles:
                warnings.warn(
                    f"Possible incorrect {kind} key found: Given `{key}` but "
                    + f"`{possibles[0][1]}` is more common"
                )


check_activity_keys = partial(_check_keys, valid=VALID_ACTIVITY_KEYS, kind="activity")
check_exchange_keys = partial(_check_keys, valid=VALID_EXCHANGE_KEYS, kind="exchange")
