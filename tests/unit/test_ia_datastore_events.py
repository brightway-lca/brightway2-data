from bw2data.database import DatabaseChooser
from bw2data.method import Method
from bw2data.signals import (
    on_method_delete,
    on_method_write,
    on_normalization_delete,
    on_normalization_write,
    on_weighting_delete,
    on_weighting_write,
)
from bw2data.tests import bw2test
from bw2data.weighting_normalization import Normalization, Weighting


@bw2test
def test_method_write_emits_signal():
    received = []

    @on_method_write.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        DatabaseChooser("bio").write({("bio", "co2"): {}})
        m = Method(("test", "method"))
        m.write([[("bio", "co2"), 1.0]])
        assert received == [("test", "method")]
    finally:
        on_method_write.disconnect(handler)


@bw2test
def test_method_write_signal_false():
    received = []

    @on_method_write.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        DatabaseChooser("bio").write({("bio", "co2"): {}})
        m = Method(("test", "method"))
        m.write([[("bio", "co2"), 1.0]], signal=False)
        assert received == []
    finally:
        on_method_write.disconnect(handler)


@bw2test
def test_method_delete_emits_signal():
    received = []

    @on_method_delete.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        DatabaseChooser("bio").write({("bio", "co2"): {}})
        m = Method(("test", "method"))
        m.write([[("bio", "co2"), 1.0]])
        m.delete()
        assert received == [("test", "method")]
        assert not m.registered
    finally:
        on_method_delete.disconnect(handler)


@bw2test
def test_method_delete_signal_false():
    received = []

    @on_method_delete.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        DatabaseChooser("bio").write({("bio", "co2"): {}})
        m = Method(("test", "method"))
        m.write([[("bio", "co2"), 1.0]])
        m.delete(signal=False)
        assert received == []
        assert not m.registered
    finally:
        on_method_delete.disconnect(handler)


@bw2test
def test_weighting_write_emits_signal():
    received = []

    @on_weighting_write.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        w = Weighting(("test", "weighting"))
        w.write([1.0])
        assert received == [("test", "weighting")]
    finally:
        on_weighting_write.disconnect(handler)


@bw2test
def test_weighting_write_signal_false():
    received = []

    @on_weighting_write.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        w = Weighting(("test", "weighting"))
        w.write([1.0], signal=False)
        assert received == []
    finally:
        on_weighting_write.disconnect(handler)


@bw2test
def test_weighting_delete_emits_signal():
    received = []

    @on_weighting_delete.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        w = Weighting(("test", "weighting"))
        w.write([1.0])
        w.delete()
        assert received == [("test", "weighting")]
        assert not w.registered
    finally:
        on_weighting_delete.disconnect(handler)


@bw2test
def test_normalization_write_emits_signal():
    received = []

    @on_normalization_write.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        DatabaseChooser("bio").write({("bio", "co2"): {}})
        n = Normalization(("test", "norm"))
        n.write([[("bio", "co2"), 1.0]])
        assert received == [("test", "norm")]
    finally:
        on_normalization_write.disconnect(handler)


@bw2test
def test_normalization_write_signal_false():
    received = []

    @on_normalization_write.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        DatabaseChooser("bio").write({("bio", "co2"): {}})
        n = Normalization(("test", "norm"))
        n.write([[("bio", "co2"), 1.0]], signal=False)
        assert received == []
    finally:
        on_normalization_write.disconnect(handler)


@bw2test
def test_normalization_delete_emits_signal():
    received = []

    @on_normalization_delete.connect
    def handler(sender, name, **kwargs):
        received.append(name)

    try:
        DatabaseChooser("bio").write({("bio", "co2"): {}})
        n = Normalization(("test", "norm"))
        n.write([[("bio", "co2"), 1.0]])
        n.delete()
        assert received == [("test", "norm")]
        assert not n.registered
    finally:
        on_normalization_delete.disconnect(handler)


@bw2test
def test_method_delete_removes_files():
    from bw2data import projects

    DatabaseChooser("bio").write({("bio", "co2"): {}})
    m = Method(("test", "method"))
    m.write([[("bio", "co2"), 1.0]])

    pickle_path = projects.dir / "intermediate" / (m.filename + ".pickle")
    processed_path = m.filepath_processed()

    assert pickle_path.exists()
    assert processed_path.exists()

    m.delete()

    assert not pickle_path.exists()
    assert not processed_path.exists()
    assert not m.registered


@bw2test
def test_delete_unregistered_is_noop():
    m = Method(("nonexistent", "method"))
    m.delete()
    assert not m.registered
