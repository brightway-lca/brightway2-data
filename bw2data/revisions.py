import json
import uuid
from typing import Any, Optional, TypeVar

import deepdiff

from bw2data.backends import schema


SD = TypeVar("SD", bound=schema.SignaledDataset)


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if not isinstance(obj, deepdiff.Delta):
            return super().default(obj)
        # XXX
        obj.serializer = deepdiff.serialization.json_dumps
        return json.loads(obj.dumps())


def generate_metadata(
    metadata: dict[str, Any],
    parent_revision: Optional[str] = None,
    revision: Optional[str] = None,
) -> dict[str, Any]:
    metadata["parent_revision"] = parent_revision
    metadata["revision"] = revision or uuid.uuid4().hex
    metadata["authors"] = metadata.get("authors", "Anonymous")
    metadata["title"] = metadata.get("title", "Untitled revision")
    metadata["description"] = metadata.get("description", "No description")
    return metadata


def generate_delta(old: Optional[SD], new: SD) -> deepdiff.Delta:
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
    return deepdiff.Delta(
        deepdiff.DeepDiff(
            mapper(old.data) if old else None,
            mapper(new.data),
            verbose_level=2,
        ),
    )
