import warnings
from functools import partial
from typing import Iterable

from bw2data.configuration import typo_settings

try:
    from rapidfuzz.distance import DamerauLevenshtein

    damerau_levenshtein = DamerauLevenshtein.distance
except ImportError:
    # Can happen on Windows, see
    # https://github.com/rapidfuzz/RapidFuzz/tree/main?tab=readme-ov-file#with-pip
    # Rapidfuzz is not currently available on Emscripten
    # https://github.com/brightway-lca/brightway-live/issues/59
    from bw2data.string_distance import damerau_levenshtein


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
            key=lambda x: x[0],
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
                key=lambda x: x[0],
            )
            if possibles and possibles[0][0] < 2 and len(possibles[0][1]) >= len(key):
                warnings.warn(
                    f"Possible incorrect {kind} key found: Given `{key}` but "
                    f"`{possibles[0][1]}` is more common"
                )


check_activity_type = partial(_check_type, valid=typo_settings.node_types, kind="activity")
check_exchange_type = partial(_check_type, valid=typo_settings.edge_types, kind="exchange")
check_activity_keys = partial(_check_keys, valid=typo_settings.node_keys, kind="activity")
check_exchange_keys = partial(_check_keys, valid=typo_settings.edge_keys, kind="exchange")
