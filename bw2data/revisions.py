from typing import Mapping, Optional, Sequence


class RevisionGraph(object):
    """Graph of revisions, edges are based on `metadata.parent_revision`."""

    class Iterator(object):
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

    def __init__(self, head: str, revisions: Sequence[dict]):
        self.head = head
        self.revisions = revisions
        self.id_map = {r["metadata"]["revision"]: r for r in revisions}

    def __iter__(self):
        """Iterates the graph from head to root."""
        return self.Iterator(self)
