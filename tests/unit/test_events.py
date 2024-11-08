import json

from bw2data import revisions
from bw2data.backends.schema import ActivityDataset
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.revisions import RevisionGraph
from bw2data.tests import bw2test


@bw2test
def test_setting_sourced():
    projects.set_current("sourced-project")
    # Default is not sourced
    assert not projects.dataset.is_sourced

    projects.dataset.set_sourced()
    assert projects.dataset.is_sourced


def test_iter_graph():
    r0 = {"metadata": {"revision": "r0"}}
    r1 = {"metadata": {"revision": "r1", "parent_revision": "r0"}}
    r2 = {"metadata": {"revision": "r2", "parent_revision": "r1"}}
    g = RevisionGraph("r2", (r1, r2, r0))
    assert list(g) == [r2, r1, r0]


@bw2test
def test_load_revisions():
    projects.set_current("test_load_revisions")
    projects.dataset.set_sourced()
    database = DatabaseChooser("db")
    database.register()
    d = projects.dataset.dir
    head = 2
    for r, p in ((0, None), (1, 0), (head, 1)):
        with open(f"{d}/revisions/{r}.rev", "w") as f:
            json.dump(
                {
                    "metadata": {"revision": r, "parent_revision": p},
                    "data": {},
                },
                f,
            )
    with open(f"{d}/revisions/head", "w") as f:
        f.write(str(head))
    projects.dataset.load_revisions()
    assert projects.dataset.revision == head


@bw2test
def test_load_revisions_partial():
    projects.set_current("test_load_revisions_partial")
    projects.dataset.set_sourced()
    database = DatabaseChooser("db")
    database.register()
    d = projects.dataset.dir
    head0, head1 = 1, 2
    for r, p in ((0, None), (head0, 0), (head1, head0)):
        with open(f"{d}/revisions/{r}.rev", "w") as f:
            json.dump(
                {
                    "metadata": {"revision": r, "parent_revision": p},
                    "data": {},
                },
                f,
            )
    with open(f"{d}/revisions/head", "w") as f:
        f.write(str(head1))
    projects.dataset.load_revisions(head0)
    assert projects.dataset.revision == head0
    projects.dataset.load_revisions()
    assert projects.dataset.revision == head1
