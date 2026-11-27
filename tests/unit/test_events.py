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


def _head_file(project):
    return (project.dir / "revisions" / "head").read_text()


def _temporary_files(project):
    return [p for p in (project.dir / "revisions").iterdir() if p.suffix == ".tmp"]


def _fail(*args, **kwargs):
    raise OSError("crash")


@bw2test
def test_write_head_failure_keeps_previous_head(monkeypatch):
    # #316: a write which fails part way doesn't leave a partly written head file
    project = create_project("test_write_head_atomic", 2, ((0, 1, 2),))
    monkeypatch.setattr("bw2data.project.os.replace", _fail)
    with pytest.raises(OSError):
        project._write_head(1234567)
    assert _head_file(project) == "2"
    assert not _temporary_files(project)


@bw2test
def test_write_revision_failure_keeps_previous_file(monkeypatch):
    project = create_project("test_write_revision_atomic", 1, ((0, 1),))
    path = project.dir / "revisions" / "1.rev"
    before = path.read_text()
    monkeypatch.setattr("bw2data.project.os.replace", _fail)
    with pytest.raises(OSError):
        project._write_revision({"metadata": {"revision": 1, "parent_revision": 7}, "data": []})
    assert path.read_text() == before
    assert not _temporary_files(project)


@bw2test
def test_add_revision_crash_before_head_file(monkeypatch):
    # #316: the column is saved but the head file isn't written; `load_revisions` applies
    # nothing and brings the head file up to date
    project = create_project("test_crash_before_head", 1, ((0, 1),))
    project.load_revisions()
    assert project.revision == 1
    with monkeypatch.context() as m:
        m.setattr(project, "_write_head", _fail)
        with pytest.raises(OSError):
            project.add_revision([])
    new = project.revision
    assert new != 1
    assert _head_file(project) == "1"

    applied = []
    with monkeypatch.context() as m:
        m.setattr(project, "apply_revision", applied.append)
        project.load_revisions()
    assert applied == []
    assert project.revision == new
    assert _head_file(project) == str(new)
    assert revision_list(project) == [0, 1, new]


@bw2test
def test_add_revision_crash_before_column(monkeypatch):
    # #316: the revision file is written but nothing points to it; it is ignored
    from bw2data.project import ProjectDataset

    project = create_project("test_crash_before_column", 1, ((0, 1),))
    project.load_revisions()
    with monkeypatch.context() as m:
        m.setattr(project, "save", _fail)
        with pytest.raises(OSError):
            project.add_revision([])
    # What a new session reads
    project = ProjectDataset.get(ProjectDataset.name == project.name)
    assert project.revision == 1
    assert _head_file(project) == "1"

    applied = []
    with monkeypatch.context() as m:
        m.setattr(project, "apply_revision", applied.append)
        project.load_revisions()
    assert applied == []
    assert project.revision == 1
    assert _head_file(project) == "1"


@bw2test
def test_load_revisions_saves_column_before_head_file(monkeypatch):
    # The column was only changed in memory, as its save failed; the head file must not get
    # ahead of what is stored
    from bw2data.project import ProjectDataset

    project = create_project("test_column_before_head", 1, ((0, 1),))
    project.load_revisions()
    with monkeypatch.context() as m:
        m.setattr(project, "save", _fail)
        with pytest.raises(OSError):
            project.add_revision([])
    new = project.revision
    project.load_revisions()
    assert _head_file(project) == str(new)
    assert ProjectDataset.get(ProjectDataset.name == project.name).revision == new


@bw2test
@pytest.mark.parametrize("content", ["", "12", "None", "1\x00"])
def test_unreadable_head_file_replaced_by_column(content, monkeypatch):
    # #316: e.g. truncated by a crash of a version which wrote it in place
    project = create_project("test_unreadable_head", 2, ((0, 1, 2),))
    project.load_revisions()
    path = project.dir / "revisions" / "head"
    if content == "12":
        # A valid id is not touched
        path.write_text(content)
        assert project._read_head() == 12
        return
    path.write_text(content)
    applied = []
    monkeypatch.setattr(project, "apply_revision", applied.append)
    with pytest.warns(UserWarning, match="doesn't hold a revision id"):
        project.load_revisions()
    assert applied == []
    assert project.revision == 2
    assert _head_file(project) == "2"


@bw2test
def test_unreadable_head_file_without_revision():
    project = create_project("test_unreadable_head_no_revision", 1, ((0, 1),))
    path = project.dir / "revisions" / "head"
    path.write_text("")
    with pytest.warns(UserWarning, match="no revision to replace it with"):
        project.load_revisions()
    assert project.revision is None
    assert path.read_text() == ""


@bw2test
def test_write_head_without_revision_writes_nothing():
    projects.set_current("test_write_head_none")
    project = projects.dataset
    project.set_sourced()
    assert project.revision is None
    project._write_head()
    assert not (project.dir / "revisions" / "head").exists()
