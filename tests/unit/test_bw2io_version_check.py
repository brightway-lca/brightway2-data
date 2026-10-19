from importlib.metadata import PackageNotFoundError
from unittest.mock import patch

import pytest

from bw2data import preferences
from bw2data.errors import IncompatibleBW2IOVersion
from bw2data.tests import bw2test
from bw2data.updates import BW2IO_REQUIREMENT, Updates, check_bw2io_version

MIGRATIONS_UPDATE = "4.0 migrations filename change"


@pytest.mark.parametrize("installed", ["0.8.12", "0.8.11", "0.7", "0.9.dev41"])
def test_check_bw2io_version_rejects_bw2io_08(installed):
    with patch("bw2data.updates.metadata.version", return_value=installed):
        with pytest.raises(IncompatibleBW2IOVersion) as excinfo:
            check_bw2io_version()
    message = str(excinfo.value)
    assert installed in message
    assert str(BW2IO_REQUIREMENT) in message
    assert "pip install" in message


@pytest.mark.parametrize("installed", ["0.9", "0.9.3", "0.9.17", "0.10rc1", "1.0"])
def test_check_bw2io_version_accepts_bw2data_4_releases(installed):
    with patch("bw2data.updates.metadata.version", return_value=installed):
        assert check_bw2io_version() == installed


def test_check_bw2io_version_not_installed():
    with patch("bw2data.updates.metadata.version", side_effect=PackageNotFoundError("bw2io")):
        assert check_bw2io_version() is None


@bw2test
def test_fix_migrations_filename_old_bw2io_raises_without_importing():
    Updates.set_initial_updates()
    preferences["updates"][MIGRATIONS_UPDATE] = False

    with patch("bw2data.updates.metadata.version", return_value="0.8.12"):
        with patch("builtins.__import__", wraps=__import__) as mocked_import:
            with pytest.raises(IncompatibleBW2IOVersion):
                Updates.do_update(MIGRATIONS_UPDATE)

    assert "bw2io" not in {call.args[0] for call in mocked_import.call_args_list}
    # The update was not applied, so it must still be pending
    assert not preferences["updates"][MIGRATIONS_UPDATE]
    assert MIGRATIONS_UPDATE in Updates.check_automatic_updates()


@bw2test
def test_fix_migrations_filename_bw2io_missing_marks_update_done():
    Updates.set_initial_updates()
    preferences["updates"][MIGRATIONS_UPDATE] = False

    with patch("bw2data.updates.metadata.version", side_effect=PackageNotFoundError("bw2io")):
        Updates.do_update(MIGRATIONS_UPDATE)

    assert preferences["updates"][MIGRATIONS_UPDATE]
