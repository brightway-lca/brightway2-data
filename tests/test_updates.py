import random

from bw2data import Updates, config
from bw2data.tests import bw2test


@bw2test
def test_set_updates_clean_install():
    assert "updates" not in config.p
    assert not Updates.check_status()
    assert len(config.p["updates"]) == len(Updates.UPDATES)


@bw2test
def test_explain():
    key = random.choice(list(Updates.UPDATES.keys()))
    assert Updates.UPDATES[key]["explanation"] == Updates.explain(key)


@bw2test
def test_do_updates():
    # Test with mock that overwrites UPDATES?
    pass
