import json
from typing import Any, Optional, Sequence, TypeVar

import deepdiff
from snowflake import SnowflakeGenerator as sfg

from bw2data.backends.schema import SignaledDataset, ActivityDataset, ExchangeDataset
from bw2data.backends.proxies import Activity, Exchange
from bw2data.errors import IncompatibleClasses, DifferentObjects
from bw2data.backends.utils import dict_as_exchangedataset, dict_as_activitydataset

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


SD = TypeVar("SD", bound=SignaledDataset)


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

    Can be serialized, transferred, and applied to the same previous version to
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

        label = SIGNALLEDOBJECT_TO_LABEL[obj_type]
        handler = REVISIONED_LABEL_AS_OBJECT[label]

        return cls.from_difference(
            label,
            old.id if old is not None else new.id,
            change_type,
            deepdiff.DeepDiff(
                handler.current_state_as_dict(old) if old else None,
                handler.current_state_as_dict(new) if new else None,
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


class RevisionedORMProxy:
    """
    Class that encapsulates logic around applying revisions. Used for `Activity` and `Exchange`.

    We need a separate class because we apply the changes to `ActivityDataset`, but need to save
    `Node` (and similar for edges).
    """
    @classmethod
    def previous_state_as_dict(cls: Self, revision_data: dict) -> dict:
        orm_object = cls.ORM_CLASS.get_by_id(revision_data['id'])
        return cls.orm_as_dict(orm_object)

    @classmethod
    def current_state_as_dict(cls: Self, obj: SignaledDataset) -> dict:
        return cls.orm_as_dict(obj)

    @classmethod
    def update(cls: Self, revision_data: dict) -> None:
        print("Calling update")
        previous = cls.previous_state_as_dict(revision_data)
        updated_data = Delta.from_dict(revision_data["delta"]).apply(previous)
        updated_orm_object = cls.ORM_CLASS(**updated_data)
        updated_orm_object.id = revision_data['id']
        cls.PROXY_CLASS(document=updated_orm_object).save(signal=False, data_already_set=True)

    @classmethod
    def create(cls: Self, revision_data: dict) -> None:
        data = Delta.from_dict(revision_data["delta"]).apply({})
        cls.PROXY_CLASS(document=None, **data).save(signal=False, data_already_set=False)

    @classmethod
    def delete(cls: Self, revision_data: dict) -> None:
        cls.PROXY_CLASS(cls.ORM_CLASS.get_by_id(revision_data['id'])).delete(signal=False)

    @classmethod
    def handle(cls: Self, revision_data: dict) -> None:
        getattr(cls, revision_data['change_type'])(revision_data)


class RevisionedNode(RevisionedORMProxy):
    PROXY_CLASS = Activity
    ORM_CLASS = Activity.ORMDataset

    @classmethod
    def orm_as_dict(cls: Self, orm_object: Activity.ORMDataset) -> dict:
        return orm_object.data

    @classmethod
    def update(cls: Self, revision_data: dict) -> None:
        previous = cls.previous_state_as_dict(revision_data)
        updated_data = Delta.from_dict(revision_data["delta"]).apply(previous)
        updated_orm_object = cls.ORM_CLASS(**dict_as_activitydataset(updated_data))
        updated_orm_object.id = revision_data['id']
        cls.PROXY_CLASS(document=updated_orm_object).save(signal=False, data_already_set=True)


class RevisionedEdge(RevisionedORMProxy):
    PROXY_CLASS = Exchange
    ORM_CLASS = Exchange.ORMDataset

    @classmethod
    def orm_as_dict(cls: Self, orm_object: Exchange.ORMDataset) -> dict:
        return dict_as_exchangedataset(orm_object.data)


SIGNALLEDOBJECT_TO_LABEL = {
    ActivityDataset: "lci_node",
    ExchangeDataset: "lci_edge",
}
REVISIONED_LABEL_AS_OBJECT = {
    "lci_node": RevisionedNode,
    "lci_edge": RevisionedEdge,
}
REVISIONS_OBJECT_AS_LABEL = {
    v: k for k, v in REVISIONED_LABEL_AS_OBJECT.items()
}
