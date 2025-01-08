import json

import pytest

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


def test_rebase():
    r0 = {"metadata": {"revision": "r0"}}
    r01 = {"metadata": {"revision": "r01", "parent_revision": "r0"}}
    r02 = {"metadata": {"revision": "r02", "parent_revision": "r01"}}
    r11 = {"metadata": {"revision": "r11", "parent_revision": "r0"}}
    r12 = {"metadata": {"revision": "r12", "parent_revision": "r11"}}
    g = RevisionGraph("r02", (r0, r01, r02, r11, r12))
    assert list(g) == [r02, r01, r0]
    g.rebase("r02", "r0", "r12")
    g.set_head("r12")
    assert list(g) == [r12, r11, r02, r01, r0]


def test_rebase_invalid_range():
    r0 = {"metadata": {"revision": "r0"}}
    r1 = {"metadata": {"revision": "r1", "parent_revision": "r0"}}
    r2 = {"metadata": {"revision": "r2", "parent_revision": "r1"}}
    g = RevisionGraph("r02", (r0, r1, r2))
    with pytest.raises(AssertionError) as ex:
        g.rebase("r0", "r2", "r0")
    assert str(ex.value) == "invalid range r2..r0"


@bw2test
def test_load_revisions():
    projects.set_current("test_load_revisions")
    database = DatabaseChooser("db")
    database.register()
    projects.dataset.set_sourced()
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
    database = DatabaseChooser("db")
    database.register()
    projects.dataset.set_sourced()
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
