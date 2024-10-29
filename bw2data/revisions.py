import json
import uuid
from typing import Any, Optional, Self, TypeVar

import deepdiff

from bw2data.backends import schema


SD = TypeVar("SD", bound=schema.SignaledDataset)


class Delta:
    """
    The difference between two versions of an object.

    Can be serialized, transfered, and applied to the same previous version to
    change it to the new state.
    """
    @classmethod
    def from_difference(
        cls: Self,
        obj_type: str,
        obj_id: int,
        diff: deepdiff.DeepDiff,
    ) -> Self:
        ret = cls()
        ret.type = obj_type
        ret.id = obj_id
        ret.delta = deepdiff.Delta(diff)
        return ret


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if not isinstance(obj, Delta):
            return super().default(obj)
        # XXX
        obj.delta.serializer = deepdiff.serialization.json_dumps
        return json.loads(obj.delta.dumps())


def generate_metadata(
    parent_revision: Optional[str] = None,
    revision: Optional[str] = None,
) -> dict[str, Any]:
    ret = {}
    ret["parent_revision"] = parent_revision
    ret["revision"] = revision or uuid.uuid4().hex
    ret["authors"] = ret.get("authors", "Anonymous")
    ret["title"] = ret.get("title", "Untitled revision")
    ret["description"] = ret.get("description", "No description")
    return ret


def generate_delta(old: Optional[SD], new: SD) -> Delta:
    """
    Generates a patch object from one version of an object to another.

    Both objects are assumeed to be of the same type, but `old` can be `None` to
    generate a creation event.
    """
    from bw2data.backends import utils

    obj_type = new.__class__
    assert old is None or old.__class__ == obj_type
    assert old is None or not old.id or old.id == new.id
    mapper = getattr(utils, f"dict_as_{obj_type.__name__.lower()}")
    return Delta.from_difference(
        obj_type.__name__.lower(),
        new.id,
        deepdiff.DeepDiff(
            mapper(old.data) if old else None,
            mapper(new.data),
            verbose_level=2,
        ),
    )
