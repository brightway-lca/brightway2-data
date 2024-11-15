from blinker import signal
from peewee import Model

try:
    from typing import override
except ImportError:
    from typing_extensions import override


signaleddataset_on_save = signal(
    "bw2data.signaleddataset_on_save",
    doc="""Emitted *after* SignaledDataset is saved.

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
    doc="""Emitted *after* any element of `databases.json` has changed.

Expected inputs:
    * `old`: dict - previous `databases.json` metadata dict
    * `new`: dict - current `databases.json` metadata dict

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

Expected inputs:
    * `name`: str - database name

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


class SignaledDataset(Model):
    @override
    def save(self, signal: bool = True, *args, **kwargs) -> None:
        """Receives a mapper to convert the data to the expected dictionary format"""
        old = type(self).get_or_none(type(self).id == self.id)
        super().save(*args, **kwargs)
        if signal:
            signaleddataset_on_save.send(
                old=old,
                new=self,
            )

    @override
    def delete_instance(self, signal: bool = True, *args, **kwargs) -> None:
        if signal:
            signaleddataset_on_delete.send(old=self)
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
