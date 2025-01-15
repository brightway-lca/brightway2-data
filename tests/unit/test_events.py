import json
from typing import Sequence, Tuple

import pytest

from bw2data import errors, revisions
from bw2data.backends.schema import ActivityDataset
from bw2data.database import DatabaseChooser
from bw2data.project import projects
from bw2data.revisions import RevisionGraph
from bw2data.tests import bw2test


def create_project(
    name: str,
    head: revisions.ID,
    branches: Sequence[Sequence[revisions.ID]],
):
    projects.set_current(name)
    database = DatabaseChooser("db")
    database.register()
    project = projects.dataset
    project.set_sourced()
    directory = project.dir
    for revisions in branches:
        parent = None
        for r in revisions:
            project._write_revision(
                {
                    "metadata": {"revision": r, "parent_revision": parent},
                    "data": {},
                }
            )
            parent = r
    project._write_head(head)
    return project


def revision_list(project):
    g = revisions.RevisionGraph(*project._load_revisions())
    return list(reversed([x["metadata"]["revision"] for x in g]))


@bw2test
def test_setting_sourced():
    projects.set_current("sourced-project")
    # Default is not sourced
    assert not projects.dataset.is_sourced

    projects.dataset.set_sourced()
    assert projects.dataset.is_sourced


@pytest.mark.parametrize(
    "r0,r1,expected",
    (
        ((0, 3), (0, 0), [0, 1, 2]),
        ((0, 0), (0, 3), [0, 1, 2]),
        ((0, 3), (3, 6), [0, 3, 1, 4, 2, 5]),
        ((0, 3), (3, 8), [0, 3, 1, 4, 2, 5, 6, 7]),
        ((3, 8), (0, 3), [3, 0, 4, 1, 5, 2, 6, 7]),
    ),
)
def test_interleave(r0, r1, expected):
    i0 = iter(range(*r0))
    i1 = iter(range(*r1))
    assert list(revisions._interleave(i0, i1)) == expected


def test_iter_graph():
    r0 = {"metadata": {"revision": "r0"}}
    r1 = {"metadata": {"revision": "r1", "parent_revision": "r0"}}
    r2 = {"metadata": {"revision": "r2", "parent_revision": "r1"}}
    g = RevisionGraph("r2", (r1, r2, r0))
    assert list(g) == [r2, r1, r0]


@pytest.mark.parametrize(
    "head0,head1,expected",
    (
        (None, None, None),
        (None, "r0", None),
        (None, "r01", None),
        (None, "r02", None),
        (None, "r11", None),
        (None, "r12", None),
        ("r0", None, None),
        ("r0", "r0", "r0"),
        ("r0", "r01", "r0"),
        ("r0", "r02", "r0"),
        ("r0", "r11", "r0"),
        ("r0", "r12", "r0"),
        ("r0", "r1", None),
        ("r01", None, None),
        ("r01", "r0", "r0"),
        ("r01", "r01", "r01"),
        ("r01", "r02", "r01"),
        ("r01", "r11", "r0"),
        ("r01", "r12", "r0"),
        ("r01", "r1", None),
        ("r02", None, None),
        ("r02", "r0", "r0"),
        ("r02", "r01", "r01"),
        ("r02", "r02", "r02"),
        ("r02", "r11", "r0"),
        ("r02", "r12", "r0"),
        ("r02", "r1", None),
        ("r11", None, None),
        ("r11", "r0", "r0"),
        ("r11", "r01", "r0"),
        ("r11", "r02", "r0"),
        ("r11", "r11", "r11"),
        ("r11", "r12", "r11"),
        ("r11", "r1", None),
        ("r12", None, None),
        ("r12", "r0", "r0"),
        ("r12", "r01", "r0"),
        ("r12", "r02", "r0"),
        ("r12", "r11", "r11"),
        ("r12", "r12", "r12"),
        ("r12", "r1", None),
        ("r1", "r1", "r1"),
    ),
)
def test_merge_base(head0, head1, expected):
    r0 = {"metadata": {"revision": "r0"}}
    r01 = {"metadata": {"revision": "r01", "parent_revision": "r0"}}
    r02 = {"metadata": {"revision": "r02", "parent_revision": "r01"}}
    r11 = {"metadata": {"revision": "r11", "parent_revision": "r0"}}
    r12 = {"metadata": {"revision": "r12", "parent_revision": "r11"}}
    r1 = {"metadata": {"revision": "r1"}}
    g = RevisionGraph("r02", (r0, r01, r02, r11, r12, r1))
    expected = expected and g.id_map[expected]["metadata"]["revision"]
    assert g.merge_base(head0, head1) == expected


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
    head = 2
    create_project("test_load_revisions", head, ((0, 1, head),))
    projects.dataset.load_revisions()
    assert projects.dataset.revision == head


@bw2test
def test_load_revisions_partial():
    head0, head1 = 1, 2
    create_project("test_load_revisions_partial", head1, ((0, head0, head1),))
    projects.dataset.load_revisions(head0)
    assert projects.dataset.revision == head0
    projects.dataset.load_revisions()
    assert projects.dataset.revision == head1


@bw2test
def test_load_revisions_rebase():
    head0, head1 = 2, 4
    project = create_project(
        "test_load_revisions_rebase",
        head0,
        ((0, 1, head0), (0, 3, head1)),
    )
    projects.dataset.load_revisions()
    assert project.revision == head0
    assert revision_list(project) == [0, 1, head0]
    project._write_head(head1)
    project.load_revisions()
    assert project.revision == head0
    assert revision_list(project) == [0, 3, head1, 1, head0]
    project.load_revisions()
    assert project.revision == head0
    assert revision_list(project) == [0, 3, head1, 1, head0]


@bw2test
def test_load_revisions_apply(monkeypatch):
    head0, head1 = 2, 4
    project = create_project(
        "test_load_revisions_rebase",
        head0,
        ((0, 1, head0), (0, 3, head1)),
    )
    applied, patch = [], lambda x: applied.append(x)
    with monkeypatch.context() as m:
        m.setattr(project, "apply_revision", patch)
        projects.dataset.load_revisions()
    ids = lambda l: [x["metadata"]["revision"] for x in l]
    assert ids(applied) == [0, 1, head0]
    project.revision = head0
    project.save()
    project._write_head(head1)
    with monkeypatch.context() as m:
        m.setattr(project, "apply_revision", patch)
        project.load_revisions()
    assert ids(applied) == [0, 1, head0, 3, head1]


@bw2test
def test_load_revisions_divergent():
    head0, head1 = 2, 5
    project = create_project(
        "test_load_revisions_rebase",
        head0,
        ((0, 1, head0), (3, 4, head1)),
    )
    projects.dataset.load_revisions()
    assert project.revision == head0
    assert revision_list(project) == [0, 1, head0]
    project._write_head(head1)
    with pytest.raises(errors.PossibleInconsistentData):
        project.load_revisions()
