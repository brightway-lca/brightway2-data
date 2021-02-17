from bw2data.tests import bw2test
from bw2data import (
    config,
    databases,
    geomapping,
    mapping,
    methods,
    preferences,
    projects,
)
from bw2data.project import ProjectDataset
from bw2data.errors import ReadOnlyProject
from pathlib import Path
import os
import pytest
import tempfile


###
### Basic setup
###


@bw2test
def test_project_directories():
    projects.set_current("foo")
    for dirname in projects._basic_directories:
        assert os.path.isdir(os.path.join(projects.dir, dirname))


@bw2test
def test_from_env_var():
    dirpath = tempfile.mkdtemp()
    os.environ["BRIGHTWAY2_DIR"] = dirpath
    base, logs = projects._get_base_directories()
    assert os.path.isdir(base)
    assert os.path.isdir(logs)
    del os.environ["BRIGHTWAY2_DIR"]


@bw2test
def test_invalid_env_var():
    os.environ["BRIGHTWAY2_DIR"] = "nothing special"
    with pytest.raises(OSError):
        projects._get_base_directories()
    del os.environ["BRIGHTWAY2_DIR"]


@bw2test
def test_invalid_output_env_dir():
    os.environ["BRIGHTWAY2_OUTPUT_DIR"] = "nothing special"
    assert str(projects.dir) in str(projects.output_dir)
    del os.environ["BRIGHTWAY2_OUTPUT_DIR"]


@bw2test
def test_output_env_dir():
    assert Path(os.getcwd()) != projects.output_dir
    os.environ["BRIGHTWAY2_OUTPUT_DIR"] = os.getcwd()
    assert Path(os.getcwd()) == projects.output_dir
    del os.environ["BRIGHTWAY2_OUTPUT_DIR"]


@bw2test
def test_output_dir_from_preferences():
    assert Path(os.getcwd()) != projects.output_dir
    preferences["output_dir"] = os.getcwd()
    assert Path(preferences["output_dir"]) == projects.output_dir


@bw2test
def test_invalid_output_dir_from_preferences():
    preferences["output_dir"] = "nope"
    assert str(projects.dir) in str(projects.output_dir)
    del preferences["output_dir"]


@bw2test
def test_directories():
    assert os.path.isdir(projects.dir)
    assert os.path.isdir(projects.logs_dir)


@bw2test
def test_default_project_created():
    assert "default" in projects


@bw2test
def test_repeatedly_set_name_same_value():
    projects.set_current("foo")
    projects.set_current("foo")
    projects.set_current("foo")
    assert len(projects) == 3
    assert "foo" in projects


@pytest.mark.skipif(config._windows, reason="Windows doesn't allow fun")
@bw2test
def test_funny_project_names():
    NAMES = [
        "Powerلُلُصّبُلُلصّبُررً ॣ ॣh ॣ ॣ冗",
        "Roses are [0;31mred[0m, violets are [0;34mblue. Hope you enjoy terminal hue",
        "True",
        "None",
        "1.0/0.0",
        "0xabad1dea",
        "!@#$%^&*()`~",
        "<>?:'{}|_+",
        r",./;'[]\-=",
        "Ω≈ç√∫˜µ≤≥÷",
        "田中さんにあげて下さい",
        "｀ｨ(´∀｀∩",
        "👾 🙇 💁 🙅 🙆 🙋 🙎 🙍 ",
        "הָיְתָהtestالصفحات التّحول",
        "　",
    ]
    error_found = False
    for name in NAMES:
        try:
            projects.set_current(name)
            assert [x for x in os.listdir(projects.dir)]
            print("This is OK:", name)
        except:
            print("This is not OK:", name)
            error_found = True
    if error_found:
        raise ValueError("Oops")


@bw2test
def test_report():
    assert projects.report


@bw2test
def test_request_directory():
    projects.request_directory("foo")
    assert "foo" in os.listdir(projects.dir)


###
### Project deletion
###


@bw2test
def test_delete_current_project_with_name():
    projects.set_current("foo")
    projects.delete_project("foo")
    assert projects.current != "foo"
    assert "foo" not in projects


@bw2test
def test_delete_project_remove_directory():
    projects.set_current("foo")
    foo_dir = projects.dir
    projects.set_current("bar")
    projects.delete_project("foo", delete_dir=True)
    assert not os.path.isdir(foo_dir)
    assert "foo" not in projects
    assert projects.current == "bar"


@bw2test
def test_delete_project_keep_directory():
    projects.set_current("foo")
    foo_dir = projects.dir
    projects.set_current("bar")
    projects.delete_project("foo")
    assert os.path.isdir(foo_dir)
    assert "foo" not in projects
    assert projects.current == "bar"


@bw2test
def test_delete_project():
    projects.set_current("foo")
    projects.set_current("bar")
    projects.delete_project("foo")
    assert "foo" not in projects
    assert projects.current == "bar"


@bw2test
def test_delete_last_project():
    assert len(projects) == 2
    with pytest.raises(ValueError):
        projects.delete_project()
        projects.delete_project()


@bw2test
def test_delete_current_project_no_name():
    projects.set_current("foo")
    projects.delete_project()
    assert "foo" not in projects
    assert projects.current != "foo"


###
### Set project
###


@bw2test
def test_error_outdated_set_project():
    assert projects.current
    with pytest.raises(AttributeError):
        projects.current = "Foo"


@bw2test
def test_set_project_creates_new_project():
    other_num = len(projects)
    projects.set_current("foo")
    assert len(projects) == other_num + 1


@bw2test
def test_set_project():
    projects.set_current("foo")
    assert projects.current == "foo"


@bw2test
def test_set_project_default_writable():
    pass


@bw2test
def test_set_project_writable_even_if_writable_false():
    pass


@bw2test
def test_set_readonly_project():
    projects.set_current("foo")
    assert not projects.read_only
    config.p["lockable"] = True
    projects.set_current("foo", writable=False)
    assert projects.read_only


@bw2test
def test_set_readonly_project_first_time():
    projects.set_current("foo", writable=False)
    assert not projects.read_only


@bw2test
def test_set_current_reset_metadata():
    databases["foo"] = "bar"
    assert "foo" in databases
    projects.set_current("foo")
    assert "foo" not in databases


###
### Test magic methods
###


@bw2test
def test_representation():
    assert repr(projects)
    assert str(projects)


@bw2test
def test_contains():
    assert "default" in projects
    projects.set_current("foo")
    assert "foo" in projects


@bw2test
def test_len():
    assert len(projects) == 2
    projects.set_current("foo")
    assert len(projects) == 3


@bw2test
def test_iterating_over_projects_no_error():
    projects.set_current("foo")
    projects.set_current("bar")
    projects.set_current("baz")
    for x in projects:
        projects.set_current(x.name)


###
### Read-only constraints
###


@bw2test
def test_create_lock_file():
    projects.set_current("foo")
    config.p["lockable"] = True
    projects.set_current("foo", writable=False)
    assert not os.path.isfile(os.path.join(projects.dir, "write-lock"))
    projects.set_current("bar")
    assert not os.path.isfile(os.path.join(projects.dir, "write-lock"))
    config.p["lockable"] = True
    projects.set_current("bar")
    assert os.path.isfile(os.path.join(projects.dir, "write-lock"))


@bw2test
def test_lockable_config_missing():
    assert "lockable" not in config.p


@bw2test
def test_read_only_cant_write():
    projects.set_current("foo")
    config.p["lockable"] = True
    projects.set_current("foo", writable=False)
    with pytest.raises(ReadOnlyProject):
        databases["foo"] = "bar"


###
### Copy project
###


@bw2test
def test_copy_project():
    ds = ProjectDataset.get(ProjectDataset.name == projects.current)
    ds.data["this"] = "that"
    ds.save()

    databases["foo"] = "bar"
    projects.copy_project("another one", False)
    assert "another one" in projects

    ds = ProjectDataset.get(ProjectDataset.name == "another one")
    assert ds.data["this"] == "that"

    projects.set_current("another one")
    assert databases["foo"] == "bar"


@bw2test
def test_copy_project_switch_current():
    projects.copy_project("another one")
    assert projects.current == "another one"


# TODO: purge delete directories
