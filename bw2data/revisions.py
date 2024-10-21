import uuid
from typing import Any, Optional


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
