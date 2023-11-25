from typing import Iterable
import warnings
from functools import partial
from ..string_distance import damerau_levenshtein

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
    """
    Validates the `type_value against a set of valid types. If the `type_value`
    is a close match (based on Damerau-Levenshtein distance) to any of the valid types
    a warning is raised indicating a possible typo.

    Parameters
    ----------
    type_value : str
        The type value to be checked.
    kind : str
        The category of the type being checked (e.g., 'activity', 'exchange').
    valid : Iterable[str]
        An iterable of valid type values.

    Raises
    ------
    UserWarning
        Warns if `type_value` is not in `valid` but is close to a valid value.

    Examples
    --------
    >>> _check_type("actvty", "activity", ["activity", "process"])
    Possible typo found: Given activity type `actvty` but `activity` is more common
    """
    if type_value and type_value not in valid and isinstance(type_value, str):
        possibles = sorted(
            ((damerau_levenshtein(type_value, possible), possible) for possible in valid),
            key=lambda x: x[0]
        )

        if possibles and possibles[0][0] <= 2:
            warning_message = (
                f"Possible typo found: Given {kind} type `{type_value}` but "
                f"`{possibles[0][1]}` is more common"
            )
            warnings.warn(warning_message, UserWarning)


def _check_keys(obj: dict, kind: str, valid: Iterable[str]) -> None:
    """
    Checks keys of a dictionary `obj` against a set of valid keys. If a key
    is a close match to any of the valid keys, a warning is raised indicating
    a possible incorrect key.

    Parameters
    ----------
    obj : dict
        The dictionary whose keys are to be checked.
    kind : str
        The category of the keys being checked (e.g., 'activity', 'exchange').
    valid : Iterable[str]
        An iterable of valid key values.

    Raises
    ------
    UserWarning
        Warns if a key in `obj` is not in `valid` but is close to a valid key.

    Examples
    --------
    >>> _check_keys({"actvty": "value"}, "activity", ["activity", "process"])
    Possible incorrect activity key found: Given `actvty` but `activity` is more common
    """
    for key in obj:
        if key not in valid and isinstance(key, str):
            possibles = sorted(
                ((damerau_levenshtein(key, possible), possible) for possible in valid),
                key=lambda x: x[0]
            )
            if possibles and possibles[0][0] < 2 and len(possibles[0][1]) >= len(key):
                warnings.warn(
                    f"Possible incorrect {kind} key found: Given `{key}` but "
                    f"`{possibles[0][1]}` is more common"
                )


check_activity_type = partial(_check_type, valid=VALID_ACTIVITY_TYPES, kind="activity")
check_exchange_type = partial(_check_type, valid=VALID_EXCHANGE_TYPES, kind="exchange")
check_activity_keys = partial(_check_keys, valid=VALID_ACTIVITY_KEYS, kind="activity")
check_exchange_keys = partial(_check_keys, valid=VALID_EXCHANGE_KEYS, kind="exchange")
