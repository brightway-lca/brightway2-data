import json
import uuid
from typing import Any, Optional


import deepdiff


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
