from contextlib import contextmanager
from typing import Iterator

from blinker import signal
from peewee import Model

try:
    from typing import override
except ImportError:
    from typing_extensions import override


signaleddataset_on_save = signal(
    "bw2data.signaleddataset_on_save",
    doc="""Emitted *after* SignaledDataset is saved.

Only sent when the current project is event sourced (`projects.dataset.is_sourced`) or
signals are turned on for this process with `bw2data.signals.set_signalled()`; looking up the
previous version costs one query per save.

Expected inputs:
    * `old`: dict - the previous version of the dataset
    * `new`: dict - the new version of the dataset

No expected return value.
""",
)

signaleddataset_on_delete = signal(
    "bw2data.signaleddataset_on_delete",
    doc="""Emitted *before* a SignaledDataset is deleted.

Expected inputs:
    * `old`: dict - the deleted version of the dataset

No expected return value.
""",
)

on_activity_database_change = signal(
    "bw2data.on_activity_database_change",
    doc="""Emitted *after* a sourced `Activity` has its `database` attribute changed.

Expected inputs:
    * `old`: dict - dict like `{'id': int, 'database': str}` with the *previous* database name
    * `new`: dict - dict like `{'id': int, 'database': str}` with the *new* database name

No expected return value.
""",
)

on_activity_code_change = signal(
    "bw2data.on_activity_code_change",
    doc="""Emitted *after* a sourced `Activity` has its `code` attribute changed.

Expected inputs:
    * `old`: dict - dict like `{'id': int, 'code': str}` with the *previous* code
    * `new`: dict - dict like `{'id': int, 'code': str}` with the *new* code

No expected return value.
""",
)

on_database_metadata_change = signal(
    "bw2data.on_database_metadata_change",
    doc="""Emitted *after* any database metadata has changed.

Only sent when the current project is event sourced (`projects.dataset.is_sourced`) or
signals are turned on for this process with `bw2data.signals.set_signalled()`; building `old`
and `new` re-reads every `DatabaseRecord` row.

Expected inputs:
    * `old`: dict - previous full database metadata dict (all databases)
    * `new`: dict - current full database metadata dict (all databases)

No expected return value.
""",
)

on_database_delete = signal(
    "bw2data.on_database_delete",
    doc="""Emitted *after* a database is deleted from SQLite and `bw2data.databases`.

Expected inputs:
    * `name`: str - database name

No expected return value.
""",
)

on_database_reset = signal(
    "bw2data.on_database_reset",
    doc="""Emitted *after* a `Database` has all its data deleted from SQLite and search indices, but
not from `bw2data.databases`.

Expected inputs:
    * `name`: str - database name

No expected return value.
""",
)

on_database_write = signal(
    "bw2data.on_database_write",
    doc="""Emitted *after* a `Database` has new data written (replacing all previous data).

Unless `Database.write` is given `signal` explicitly, only sent when the current project is
event sourced (`projects.dataset.is_sourced`) or signals are turned on for this process with
`bw2data.signals.set_signalled()`.

`Database.copy` writes the new database before it copies parameters, so during a copy this
signal is sent before the parameters of the new database exist. Use `on_database_copy` to
react to a finished copy.

Expected inputs:
    * `name`: str - database name

No expected return value.
""",
)

on_database_copy = signal(
    "bw2data.on_database_copy",
    doc="""Emitted *after* `Database.copy` has finished, when the new database has its data and
parameters.

Always sent, and doesn't generate a revision; on event-sourced projects the changes made by
the copy are already recorded by the signals sent while copying.

Expected inputs:
    * `name`: str - name of the new database
    * `source`: str - name of the database which was copied

No expected return value.
""",
)

on_project_parameter_recalculate = signal(
    "bw2data.on_project_parameter_recalculate",
    doc="""Emitted *after* a call to `bw2data.parameters.ProjectParameter.recalculate()`.

No expected inputs.

No expected return value.
""",
)

on_project_parameter_update_formula_parameter_name = signal(
    "bw2data.on_project_parameter_update_formula_parameter_name",
    doc="""Emitted *after* a call to `bw2data.parameters.ProjectParameter.update_formula_parameter_name()`.

Expected inputs:
    * `old` - dict like {"old": str} with *previous* parameter name
    * `new` - dict like {"new": str} with *new* parameter name

No expected return value.
""",
)

on_database_parameter_recalculate = signal(
    "bw2data.on_database_parameter_recalculate",
    doc="""Emitted *after* a call to `bw2data.parameters.DatabaseParameter.recalculate()`.

Expected inputs:
    * `name`: str - database name

No expected return value.
""",
)

on_database_parameter_update_formula_project_parameter_name = signal(
    "bw2data.on_database_parameter_update_formula_project_parameter_name",
    doc="""Emitted *after* a call to `bw2data.parameters.DatabaseParameter.update_formula_project_parameter_name()`.

Expected inputs:
    * `old` - dict like {"old": str} with *previous* parameter name
    * `new` - dict like {"new": str} with *new* parameter name

No expected return value.
""",
)

on_database_parameter_update_formula_database_parameter_name = signal(
    "bw2data.on_database_parameter_update_formula_database_parameter_name",
    doc="""Emitted *after* a call to `bw2data.parameters.DatabaseParameter.update_formula_database_parameter_name()`.

Expected inputs:
    * `old` - dict like {"old": str} with *previous* parameter name
    * `new` - dict like {"new": str} with *new* parameter name

No expected return value.
""",
)

on_activity_parameter_recalculate = signal(
    "bw2data.on_activity_parameter_recalculate",
    doc="""Emitted *after* a call to `bw2data.parameters.ActivityParameter.recalculate()`.

Expected inputs:
    * `name`: str - group name

No expected return value.
""",
)

on_activity_parameter_recalculate_exchanges = signal(
    "bw2data.on_activity_parameter_recalculate_exchanges",
    doc="""Emitted *after* a call to `bw2data.parameters.ActivityParameter.recalculate_exchanges()`.

Expected inputs:
    * `name`: str - group name

No expected return value.
""",
)

on_activity_parameter_update_formula_project_parameter_name = signal(
    "bw2data.on_activity_parameter_update_formula_project_parameter_name",
    doc="""Emitted *after* a call to `bw2data.parameters.ActivityParameter.update_formula_project_parameter_name()`.

Expected inputs:
    * `old` - dict like {"old": str} with *previous* parameter name
    * `new` - dict like {"new": str} with *new* parameter name

No expected return value.
""",
)

on_activity_parameter_update_formula_database_parameter_name = signal(
    "bw2data.on_activity_parameter_update_formula_database_parameter_name",
    doc="""Emitted *after* a call to `bw2data.parameters.ActivityParameter.update_formula_database_parameter_name()`.

Expected inputs:
    * `old` - dict like {"old": str} with *previous* parameter name
    * `new` - dict like {"new": str} with *new* parameter name

No expected return value.
""",
)

on_activity_parameter_update_formula_activity_parameter_name = signal(
    "bw2data.on_activity_parameter_update_formula_activity_parameter_name",
    doc="""Emitted *after* a call to `bw2data.parameters.ActivityParameter.update_formula_activity_parameter_name()`.

Expected inputs:
    * `old` - dict like {"old": str} with *previous* parameter name
    * `new` - dict like {"new": str, "include_order": bool} with *new* parameter name

No expected return value.
""",
)

project_changed = signal(
    "bw2data.project_changed",
    doc="""
Emitted when project changed, after redirecting any SQLite database references.

Expected inputs:
    * `bw2data.projects.ProjectDataset` instance

No expected return value.
""",
)

project_created = signal(
    "bw2data.project_created",
    doc="""
Emitted when project created, but before switching to that project, and before any filesystem ops.

Expected inputs:
    * `bw2data.projects.ProjectDataset` instance

No expected return value.
""",
)

project_deleted = signal(
    "bw2data.project_deleted",
    doc="""
Emitted when a project is deleted, after removal from the database but before any filesystem ops.

Expected inputs:
    * `bw2data.projects.ProjectDataset` instance

No expected return value.
""",
)


_signalled = False


def set_signalled(value: bool = True) -> None:
    """Turn the change signals which are costly to build on or off for this process.

    `signaleddataset_on_save`, `on_database_metadata_change`, `on_database_write` and the
    `_save_signal` of `SerializedDict` subclasses need extra queries to build their arguments.
    They are always sent on event-sourced projects, where they generate revisions. On other
    projects they are only sent after `set_signalled(True)`; call it once at startup in an
    application which listens to these signals, such as a GUI.

    The setting is held in memory for the current process. It is not stored in the project,
    and is not changed by switching projects."""
    global _signalled
    _signalled = bool(value)


def is_signalled() -> bool:
    """Whether change signals were turned on for this process with `set_signalled`."""
    return _signalled


@contextmanager
def signalled(value: bool = True) -> Iterator[None]:
    """Context manager to set `set_signalled(value)` for a block, then restore the previous
    setting."""
    previous = _signalled
    set_signalled(value)
    try:
        yield
    finally:
        set_signalled(previous)


def should_emit() -> bool:
    """Whether the change signals which are costly to build are sent: signals are turned on
    for this process, or the current project is event sourced."""
    if _signalled:
        return True

    from bw2data import projects

    return bool(projects.dataset.is_sourced)


class SignaledDataset(Model):
    @override
    def save(self, signal: bool = True, *args, **kwargs) -> None:
        """Receives a mapper to convert the data to the expected dictionary format"""
        # Fetching the previous version costs one query per save. Only pay for it when the
        # project is event sourced or a listener asked for signals with `set_signalled`.
        signal = signal and should_emit()
        old = type(self).get_or_none(type(self).id == self.id) if signal else None
        super().save(*args, **kwargs)
        if signal:
            signaleddataset_on_save.send(
                self,
                old=old,
                new=self,
            )

    @override
    def delete_instance(self, signal: bool = True, *args, **kwargs) -> None:
        if signal:
            signaleddataset_on_delete.send(self, old=self)
        super().delete_instance(*args, **kwargs)

    # From the peewee docs
    # https://docs.peewee-orm.com/en/latest/peewee/playhouse.html#signal-support
    # For what I hope are obvious reasons, Peewee signals do not work when you use the
    # Model.insert(), Model.update(), or Model.delete() methods. These methods generate queries that
    # execute beyond the scope of the ORM, and the ORM does not know about which model instances
    # might or might not be affected when the query executes.

    # def update(self, *args, **kwargs) -> None:
    #     raise NotImplementedError("SQL update statements not compatible with signaled datasets")

    # def delete(self, *args, **kwargs) -> None:
    #     raise NotImplementedError("SQL delete statements not compatible with signaled datasets")

    # def insert(self, *args, **kwargs) -> None:
    #     raise NotImplementedError("SQL insert statements not compatible with signaled datasets")

    # def _update_without_signal(self, *args, **kwargs) -> None:
    #     """
    #     Internal API for issuing SQL update statements.

    #     Reserved for use by `bw2data`, and API can change at any time. Please use `.save()` instead.
    #     """
    #     return super().update(*args, **kwargs)

    # def _delete_without_signal(self, *args, **kwargs) -> None:
    #     """
    #     Internal API for issuing SQL delete statements.

    #     Reserved for use by `bw2data`, and API can change at any time. Please use
    #     `.delete_instance()` instead.
    #     """
    #     return super().delete(*args, **kwargs)

    # def _insert_without_signal(self, *args, **kwargs) -> None:
    #     """
    #     Internal API for issuing SQL insert statements.

    #     Reserved for use by `bw2data`, and API can change at any time. Please use `.save()` instead.
    #     """
    #     return super().insert(*args, **kwargs)
