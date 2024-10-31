import json
from typing import Any, Optional, Sequence, TypeVar

import deepdiff
from snowflake import SnowflakeGenerator as sfg

from bw2data.backends.schema import SignaledDataset, ActivityDataset, ExchangeDataset
from bw2data.errors import IncompatibleClasses, DifferentObjects
from bw2data.backends.utils import dict_as_activitydataset, dict_as_exchangedataset

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


SD = TypeVar("SD", bound=SignaledDataset)
OBJECT_AS_DICT = {
    ActivityDataset: lambda x: x.data,
    ExchangeDataset: lambda x: dict_as_exchangedataset(x.data),
}
OBJECT_AS_LABEL = {
    ActivityDataset: "lci_node",
    ExchangeDataset: "lci_edge",
}
LABEL_AS_OBJECT = {
    v: k for k, v in OBJECT_AS_LABEL.items()
}


class RevisionGraph:
    """Graph of revisions, edges are based on `metadata.parent_revision`."""

    class Iterator:
        """Helper class implementing iteration from child to parent."""

        def __init__(self, g: "RevisionGraph"):
            self.head = g.head
            self.id_map = g.id_map

        def __next__(self) -> Optional[dict]:
            if self.head is None:
                raise StopIteration
            ret = self.id_map[self.head]
            self.head = ret["metadata"].get("parent_revision")
            return ret

    def __init__(self, head: int, revisions: Sequence[dict]):
        self.head = head
        self.revisions = revisions
        self.id_map = {r["metadata"]["revision"]: r for r in revisions}

    def __iter__(self):
        """Iterates the graph from head to root."""
        return self.Iterator(self)


class Delta:
    """
    The difference between two versions of an object.

    Can be serialized, transfered, and applied to the same previous version to
    change it to the new state.
    """
    def apply(self, obj):
        return obj + self.delta

    @classmethod
    def from_dict(cls: Self, d: dict) -> Self:
        ret = cls()
        ret.delta = deepdiff.Delta(
            JSONEncoder().encode(d), deserializer=deepdiff.serialization.json_loads
        )
        return ret

    @classmethod
    def from_difference(
        cls: Self,
        obj_type: str,
        obj_id: int,
        change_type: str,
        diff: deepdiff.DeepDiff,
    ) -> Self:
        ret = cls()
        ret.type = obj_type
        ret.id = obj_id
        ret.change_type = change_type
        ret.delta = deepdiff.Delta(diff)
        return ret

    @classmethod
    def generate(cls: Self, old: Optional[SD], new: Optional[SD]) -> Self:
        """
        Generates a patch object from one version of an object to another.

        Both `old` and `new` should be instances of `bw2data.backends.schema.SignaledDataset`.

        `old` can be `None` if an object is being created.

        `new` can be `None` is an object is being deleted.

        Raises `IncompatibleClasses` is `old` and `new` have different classes.
        """
        if old is None and new is None:
            raise ValueError("Both `new` and `old` are `None`")

        obj_type = new.__class__ if new is not None else old.__class__
        if old is not None and new is not None:
            if old.__class__ != new.__class__:
                raise IncompatibleClasses(f"Can't diff {old.__class__} and {new.__class__}")
            if old.id != new.id:
                raise DifferentObjects(f"Can't diff different objects (ids {old.id} & {new.id})")

        if old is not None and new is not None:
            change_type = "update"
        elif old is None and new is not None:
            change_type = "create"
        elif old is not None and new is None:
            change_type = "delete"

        return cls.from_difference(
            OBJECT_AS_LABEL[obj_type],
            new.id,
            change_type,
            deepdiff.DeepDiff(
                OBJECT_AS_DICT[obj_type](old) if old else None,
                OBJECT_AS_DICT[obj_type](new) if new else None,
                verbose_level=2,
            ),
        )


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if not isinstance(obj, Delta):
            return super().default(obj)
        # XXX
        obj.delta.serializer = deepdiff.serialization.json_dumps
        return json.loads(obj.delta.dumps())


def generate_metadata(
    parent_revision: Optional[int] = None,
    revision: Optional[int] = None,
) -> dict[str, Any]:
    ret = {}
    ret["parent_revision"] = parent_revision
    ret["revision"] = revision or next(sfg(0))
    ret["authors"] = ret.get("authors", "Anonymous")
    ret["title"] = ret.get("title", "Untitled revision")
    ret["description"] = ret.get("description", "No description")
    return ret


def generate_revision(metadata: dict, delta: Sequence[Delta]) -> dict:
    return {
        "metadata": metadata,
        "data": [{"type": d.type, "id": d.id, "change_type": d.change_type, "delta": d} for d in delta],
    }


generate_delta = Delta.generate
