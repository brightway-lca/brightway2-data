import json
from typing import Any, Optional, Sequence, Union

import deepdiff

from bw2data import databases
from bw2data.backends.proxies import Activity, Exchange
from bw2data.backends.schema import ActivityDataset, ExchangeDataset
from bw2data.backends.utils import dict_as_activitydataset, dict_as_exchangedataset
from bw2data.database import DatabaseChooser
from bw2data.errors import DifferentObjects, IncompatibleClasses
from bw2data.signals import SignaledDataset
from bw2data.snowflake_ids import snowflake_id_generator
from bw2data.utils import get_node

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


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

    def __init__(
        self,
        delta: Optional[Union[deepdiff.Delta, dict]],
        obj_type: Optional[str] = None,
        obj_id: Optional[Union[int, str]] = None,
        change_type: Optional[str] = None,
    ):
        """
        Private, exists only for type-checking.

        Use one of the class-method constructors to create objects.
        """
        self.type = obj_type
        self.id = obj_id
        self.change_type = change_type
        self.delta = delta

    def apply(self, obj):
        return obj + self.delta

    @classmethod
    def from_dict(cls, d: dict) -> Self:
        return cls(
            delta=deepdiff.Delta(
                JSONEncoder().encode(d), deserializer=deepdiff.serialization.json_loads
            ),
        )

    @classmethod
    def from_difference(
        cls,
        obj_type: str,
        obj_id: Optional[Union[int, str]],
        change_type: str,
        diff: deepdiff.DeepDiff,
    ) -> Self:
        return cls(
            obj_type=obj_type,
            obj_id=obj_id,
            change_type=change_type,
            delta=deepdiff.Delta(diff),
        )

    @classmethod
    def _direct_lci_node_difference(cls, old: dict, new: dict, change_type: str) -> Self:
        return cls.from_difference(
            "lci_node",
            old["id"],
            change_type,
            deepdiff.DeepDiff(
                old,
                new,
                verbose_level=2,
            ),
        )

    @classmethod
    def activity_code_change(cls, old: dict, new: dict) -> Self:
        """Special handling to change the `database` attribute of an activity node."""
        return cls._direct_lci_node_difference(old, new, "activity_code_change")

    @classmethod
    def activity_database_change(cls, old: dict, new: dict) -> Self:
        """Special handling to change the `database` attribute of an activity node."""
        return cls._direct_lci_node_difference(old, new, "activity_database_change")

    @classmethod
    def database_metadata_change(cls, old: dict, new: dict) -> Union[Self, None]:
        """Special handling to change the `database` attribute of an activity node."""
        for dct in (old, new):
            # Changes to these values are "noise" - they will either be captured by other events
            # (i.e. changing the graph will set database['dirty']) or are not worth propagating
            # so we can safely remove them from the diff generation.
            for value in dct.values():
                for forgotten in ("processed", "modified", "dirty", "number"):
                    if forgotten in value:
                        del value[forgotten]
        diff = deepdiff.DeepDiff(
            old,
            new,
            verbose_level=2,
        )
        if not diff:
            return None
        return cls.from_difference("lci_database", None, "database_metadata_change", diff)

    @classmethod
    def generate(
        cls, old: Optional[Any], new: Optional[Any], operation: Optional[str] = None
    ) -> Self:
        """
        Generates a patch object from one version of an object to another.

        Both `old` and `new` should be instances of `bw2data.backends.schema.SignaledDataset`.

        `old` can be `None` if an object is being created.

        `new` can be `None` is an object is being deleted.

        Raises `IncompatibleClasses` is `old` and `new` have different classes.
        """
        if operation is not None:
            return getattr(cls, operation)(old, new)

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

        diff = deepdiff.DeepDiff(
            handler.current_state_as_dict(old) if old else None,
            handler.current_state_as_dict(new) if new else None,
            verbose_level=2,
        )
        if not diff:
            return None

        return cls.from_difference(
            label,
            old.id if old is not None else new.id,
            change_type,
            diff,
        )


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if not isinstance(obj, Delta):
            return super().default(obj)
        # XXX
        obj.delta.serializer = deepdiff.serialization.json_dumps
        return json.loads(obj.delta.dumps())


def generate_metadata(
    metadata: Optional[dict[str, Any]] = None,
    parent_revision: Optional[int] = None,
    revision: Optional[int] = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    metadata["parent_revision"] = parent_revision
    metadata["revision"] = revision or next(snowflake_id_generator)
    metadata["authors"] = metadata.get("authors", "Anonymous")
    metadata["title"] = metadata.get("title", "Untitled revision")
    metadata["description"] = metadata.get("description", "No description")
    return metadata


def generate_revision(metadata: dict, delta: Sequence[Delta]) -> dict:
    return {
        "metadata": metadata,
        "data": [
            {"type": d.type, "id": d.id, "change_type": d.change_type, "delta": d} for d in delta
        ],
    }


generate_delta = Delta.generate


class RevisionedORMProxy:
    """
    Class that encapsulates logic around applying revisions. Used for `Activity` and `Exchange`.

    We need a separate class because we apply the changes to `ActivityDataset`, but need to save
    `Node` (and similar for edges).
    """

    @classmethod
    def handle(cls, revision_data: dict) -> None:
        getattr(cls, revision_data["change_type"])(revision_data)

    @classmethod
    def previous_state_as_dict(cls, revision_data: dict) -> dict:
        orm_object = cls.ORM_CLASS.get_by_id(revision_data["id"])
        return cls.orm_as_dict(orm_object)

    @classmethod
    def current_state_as_dict(cls, obj: SignaledDataset) -> dict:
        return cls.orm_as_dict(obj)

    @classmethod
    def update(cls, revision_data: dict) -> None:
        previous = cls.previous_state_as_dict(revision_data)
        updated_data = Delta.from_dict(revision_data["delta"]).apply(previous)
        updated_orm_object = cls.ORM_CLASS(**cls.prepare_data_dict_for_orm_class(updated_data))
        updated_orm_object.id = revision_data["id"]
        cls.PROXY_CLASS(document=updated_orm_object).save(signal=False, data_already_set=True)

    @classmethod
    def delete(cls, revision_data: dict) -> None:
        cls.PROXY_CLASS(cls.ORM_CLASS.get_by_id(revision_data["id"])).delete(signal=False)

    @classmethod
    def prepare_data_dict_for_orm_class(cls, data: dict) -> dict:
        return data

    @classmethod
    def create(cls, revision_data: dict) -> None:
        data = Delta.from_dict(revision_data["delta"]).apply({})
        orm_object = cls.ORM_CLASS(**cls.prepare_data_dict_for_orm_class(data))
        orm_object.id = revision_data["id"]
        # Force insert because we specify the primary key already but object not in database
        cls.PROXY_CLASS(document=orm_object).save(
            signal=False, data_already_set=True, force_insert=True
        )


class RevisionedNode(RevisionedORMProxy):
    PROXY_CLASS = Activity
    ORM_CLASS = Activity.ORMDataset

    @classmethod
    def orm_as_dict(cls, orm_object: Activity.ORMDataset) -> dict:
        return orm_object.data

    @classmethod
    def prepare_data_dict_for_orm_class(cls, data: dict) -> dict:
        return dict_as_activitydataset(data)

    @classmethod
    def activity_database_change(cls, revision_data: dict) -> None:
        """Special handling for changing activity `database` attributes"""
        node = get_node(id=revision_data["id"])
        node._change_database(
            new_database=revision_data["delta"]["values_changed"]["root['database']"]["new_value"],
            signal=False,
        )

    @classmethod
    def activity_code_change(cls, revision_data: dict) -> None:
        """Special handling for changing activity `code` attributes"""
        node = get_node(id=revision_data["id"])
        node._change_code(
            new_code=revision_data["delta"]["values_changed"]["root['code']"]["new_value"],
            signal=False,
        )


class RevisionedEdge(RevisionedORMProxy):
    PROXY_CLASS = Exchange
    ORM_CLASS = Exchange.ORMDataset

    @classmethod
    def orm_as_dict(cls, orm_object: Exchange.ORMDataset) -> dict:
        return dict_as_exchangedataset(orm_object.data)


class RevisionedDatabase:
    @classmethod
    def handle(cls, revision_data: dict) -> None:
        if revision_data["change_type"] == "database_metadata_change":
            new_data = Delta.from_dict(revision_data["delta"]).apply(databases.data)
            for name, value in new_data.items():
                # Need to call this method to create search index database file
                if value.get("searchable") and not databases.get(name, {}).get("searchable"):
                    DatabaseChooser(name).make_searchable(reset=False, signal=False)
                elif not value.get("searchable") and databases.get(name, {}).get("searchable"):
                    DatabaseChooser(name).make_unsearchable(reset=False, signal=False)
            databases.data = new_data
            databases.flush(signal=False)
        if revision_data["change_type"] == "database_reset":
            DatabaseChooser(revision_data["id"]).delete(warn=False, signal=False)
        if revision_data["change_type"] == "database_delete":
            databases.__delitem__(revision_data["id"], signal=False)


SIGNALLEDOBJECT_TO_LABEL = {
    ActivityDataset: "lci_node",
    ExchangeDataset: "lci_edge",
}
REVISIONED_LABEL_AS_OBJECT = {
    "lci_node": RevisionedNode,
    "lci_edge": RevisionedEdge,
    "lci_database": RevisionedDatabase,
}
REVISIONS_OBJECT_AS_LABEL = {v: k for k, v in REVISIONED_LABEL_AS_OBJECT.items()}
