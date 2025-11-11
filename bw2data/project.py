import json
import os
import shutil
import warnings
from collections.abc import Iterable
from copy import copy
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Sequence, Tuple, Union

import deepdiff
import wrapt
from bw_processing import safe_filename
from peewee import SQL, BooleanField, DoesNotExist, IntegerField, Model, SqliteDatabase, TextField
from platformdirs import PlatformDirs

import bw2data.signals as bw2signals
from bw2data import config
from bw2data.errors import InconsistentData, PossibleInconsistentData
from bw2data.filesystem import create_dir
from bw2data.logs import stdout_feedback_logger
from bw2data.signals import project_changed, project_created
from bw2data.sqlite import PickleField, SubstitutableDatabase
from bw2data.utils import maybe_path

if TYPE_CHECKING:
    from bw2data import revisions


READ_ONLY_PROJECT = """
***Read only project***

This project is being used by another process and no writes can be made until:
    1. You close the other program, or switch to a different project, *and*
    2. You call `projects.enable_writes` *and* get the response `True`.

    If you are **sure** that this warning is incorrect, call
    `projects.enable_writes(force=True)` to enable writes.
"""


def lockable():
    return False


class ProjectDataset(Model):
    # Event sourcing
    is_sourced = BooleanField(default=False, constraints=[SQL("DEFAULT 0")])
    revision = IntegerField(null=True)

    data = PickleField()
    name = TextField(index=True, unique=True)
    # Peewee doesn't set defaults in the database but rather in Python.
    # But for backwards compatibility we need a default `True` value
    # and this hack is the recommended way to get this behaviour.
    # See https://docs.peewee-orm.com/en/latest/peewee/models.html?highlight=table%20generation
    full_hash = BooleanField(default=True, constraints=[SQL("DEFAULT 1")])

    def __str__(self):
        return "Project: {}".format(self.name)

    __repr__ = lambda x: str(x)

    def __lt__(self, other):
        if not isinstance(other, ProjectDataset):
            raise TypeError
        else:
            return self.name.lower() < other.name.lower()

    @property
    def dir(self):
        return projects._project_dir(self)

    def set_sourced(self) -> None:
        """Set the project to be event sourced."""
        self.is_sourced = True
        # Backwards compatible with existing projects
        if not (self.dir / "revisions").is_dir():
            (self.dir / "revisions").mkdir()
        self.save()

    def _write_revision(self, revision: "revisions.Revision"):
        """Write revision to disk."""
        from bw2data import revisions

        rev_id = revision["metadata"]["revision"]
        with open(self.dir / "revisions" / f"{rev_id}.rev", "w") as f:
            f.write(revisions.JSONEncoder(indent=2).encode(revision))

    def _write_head(self, head: Optional["revisions.ID"] = None):
        """Write starting revision to disk."""
        head = head if head is not None else self.revision
        with open(self.dir / "revisions" / "head", "w") as f:
            f.write(str(head))

    def add_revision(
        self,
        delta: Sequence["revisions.Delta"],
        metadata: Optional[dict[str, Any]] = None,
    ) -> Optional[int]:
        """
        Add a revision to the project changing the state of one or more objects.

        {
          "metadata": {
            "revision": <this-revision-id>
            "parent_revision": <parent-revision-id>
            "title": "<optional>"
            "description": "<optional>"
            "authors": "<optional>" (maybe shouldn't be optional)
          },
          "data": [
                    {
                      "type": "database object type" (e.g. "activity", "exchange", "parameter"),
                      "id": "database object id" (e.g. "foo", "bar", "baz"),
                      "delta": <difference between revisions>
                    }, ...
                 ]
        }
        """
        if not self.is_sourced:
            return None

        from bw2data import revisions

        ext_metadata = revisions.generate_metadata(metadata, parent_revision=self.revision)
        self.revision = ext_metadata["revision"]
        self._write_revision(revisions.generate_revision(ext_metadata, delta))
        self.save()
        self._write_head()
        return self.revision

    def apply_revision(self, revision: dict) -> None:
        """
        Load a patch generated from a previous `add_revision` into the project.
        """
        from bw2data import revisions

        # Once *any* revision is applied, we need to track changes
        # Otherwise we could apply a patch, then make an untracked change, and we wouldn't know if
        # a second patch could be safely applied in the future.
        if not self.is_sourced:
            self.set_sourced()

        meta = revision["metadata"]
        self._check_local_versus_remote_revision(self.revision, meta.get("parent_revision"))

        for revision_data in revision["data"]:
            obj_class = revisions.REVISIONED_LABEL_AS_OBJECT[revision_data["type"]]
            obj_class.handle(revision_data)
        self.revision = meta["revision"]
        self.save()

    def _check_local_versus_remote_revision(
        self, local: Optional[str], remote: Optional[str]
    ) -> None:
        # Patch parent can be a revision ID or `None`
        # Local state can be a revision ID or `None`
        # There are five possible cases:
        # 1. patch parent: None | local: None
        # Beginning of event sourcing, applying patch leads to new local state
        # No real guarantee that they start from the same state, but we can hope for the best
        # 2. patch parent: int | local: None
        # Can't apply safely as patch state could be different than local
        # Raise `PossibleInconsistentData`
        # 3. patch parent: None | local: int
        # Can't apply safely as local state could be different than patch
        # Raise `PossibleInconsistentData`
        # 4. patch parent: int | local: int | patch parent == local
        # Can apply safely, starting state is consistent
        # 5. patch parent: int | local: int | patch parent != local
        # Raise `InconsistentData`
        if local is remote is None:
            pass
        elif local and not remote:
            raise PossibleInconsistentData
        elif not local and remote:
            raise PossibleInconsistentData
        elif local and remote and local == remote:
            pass
        elif local and remote and local != remote:
            raise InconsistentData

    def _load_revisions(
        self,
        head: Optional["revisions.ID"] = None,
    ) -> Optional[Tuple["revisions.ID", Sequence["revisions.Revision"]]]:
        """Reads all revisions from disk."""
        revs = []
        if head is None:
            if not (self.dir / "revisions" / "head").is_file():
                # No revisions recorded yet
                return None
            with open(self.dir / "revisions" / "head", "r") as f:
                head = int(f.read())
        for filename in os.listdir(self.dir / "revisions"):
            if not filename.endswith(".rev"):
                continue
            with open(self.dir / "revisions" / filename, "r") as f:
                revs.append(json.load(f))
        return head, revs

    def _rebase(
        self,
        graph: "revisions.RevisionGraph",
    ) -> Iterable["revisions.Revision"]:
        """
        Rebases current revision list on top of remote head, if necessary.

        In a trivial fast-forward merge, this function will simply return the
        revisions which need to be applied.  If a rebase is necessary, it will:
        - move the head to the merge base
        - return the (remote) revision range to be applied on top of it
        - move the head of the graph to the rebased head, the project head can
          be fast-forwarded to it after the application of the remote revisions
        """
        local_id, remote_id = self.revision, graph.head
        base = graph.merge_base(local_id, remote_id)
        if base == local_id:
            ret = graph.range(local_id, remote_id)
        elif base is None:
            raise PossibleInconsistentData
        else:
            self._write_revision(graph.rebase(remote_id, base, local_id))
            graph.set_head(local_id)
            self.revision = base
            ret = graph.range(base, remote_id)
        return reversed(list(ret))

    def _fast_forward(
        self,
        graph: "revisions.RevisionGraph",
        revision: "revisions.ID",
    ):
        """Moves the current head forward, if necessary."""
        if self.revision == revision:
            return
        assert graph.is_ancestor(self.revision, revision)
        self.revision = revision
        self.save()
        self._write_head()

    def load_revisions(self, head: Optional[int] = None) -> None:
        """
        Load all revisions unapplied for this project.
        """
        from bw2data import revisions

        loaded = self._load_revisions(head)
        if loaded is None:
            return
        g = revisions.RevisionGraph(*loaded)
        for rev in self._rebase(g):
            self.apply_revision(rev)
        self._fast_forward(g, g.head)


def add_full_hash_column(base_data_dir: Path, db: SqliteDatabase) -> None:
    src_filepath = base_data_dir / "projects.db"
    backup_filepath = base_data_dir / "projects.backup-full-hash.db"
    shutil.copy(src_filepath, backup_filepath)

    MIGRATION_WARNING = """
Adding a column to the projects database.
A backup copy of this database '{}' was made at '{}'.
If you have problems, file an issue, restore the backup data, and use a stable version of Brightway.
""".format(
        src_filepath, backup_filepath
    )
    stdout_feedback_logger.warning(MIGRATION_WARNING)

    ADD_FULL_HASH_COLUMN = """ALTER TABLE projectdataset ADD COLUMN "full_hash" integer default 1"""
    db.execute_sql(ADD_FULL_HASH_COLUMN)


def add_sourced_columns(base_data_dir: Path, db: SqliteDatabase) -> None:
    src_filepath = base_data_dir / "projects.db"
    backup_filepath = base_data_dir / "projects.backup-is-sourced.db"
    shutil.copy(src_filepath, backup_filepath)

    MIGRATION_WARNING = """
Adding two columns to the projects database.
A backup copy of this database '{}' was made at '{}'.
If you have problems, file an issue, restore the backup data, and use a stable version of Brightway.
""".format(
        src_filepath, backup_filepath
    )
    stdout_feedback_logger.warning(MIGRATION_WARNING)

    ADD_IS_SOURCED_COLUMN = (
        """ALTER TABLE projectdataset ADD COLUMN "is_sourced" integer default 0"""
    )
    db.execute_sql(ADD_IS_SOURCED_COLUMN)
    ADD_REVISION_COLUMN = """ALTER TABLE projectdataset ADD COLUMN "revision" integer"""
    db.execute_sql(ADD_REVISION_COLUMN)


class ProjectManager(Iterable):
    _basic_directories = (
        "backups",
        "intermediate",
        "lci",
        "processed",
        "revisions",
    )
    _is_temp_dir = False
    read_only = False

    def __init__(self):
        self._base_data_dir, self._base_logs_dir = self._get_base_directories()
        self._create_base_directories()
        self.db = SubstitutableDatabase(self._base_data_dir / "projects.db", [ProjectDataset])

        columns = {o.name for o in self.db._database.get_columns("projectdataset")}

        # We don't use Peewee migrations because it won't set default values for new columns.
        # One could therefore get an error from using the development branch alongside the stable
        # branch.
        if "full_hash" not in columns:
            add_full_hash_column(base_data_dir=self._base_data_dir, db=self.db._database)
        if "is_sourced" not in columns:
            add_sourced_columns(base_data_dir=self._base_data_dir, db=self.db._database)
        self.set_current("default", update=False)

    def __iter__(self):
        for project_ds in ProjectDataset.select():
            yield project_ds

    def __contains__(self, name):
        return ProjectDataset.select().where(ProjectDataset.name == name).count() > 0

    def __len__(self):
        return ProjectDataset.select().count()

    def __repr__(self):
        if len(self) > 20:
            return (
                "Brightway2 projects manager with {} objects, including:"
                "{}\nUse `sorted(projects)` to get full list, "
                "`projects.report()` to get\n\ta report on all projects."
            ).format(
                len(self),
                "".join(["\n\t{}".format(x) for x in sorted([x.name for x in self])[:10]]),
            )
        else:
            return (
                "Brightway2 projects manager with {} objects:{}"
                "\nUse `projects.report()` to get a report on all projects."
            ).format(
                len(self),
                "".join(["\n\t{}".format(x) for x in sorted([x.name for x in self])]),
            )

    ### Internal functions for managing projects

    def _get_base_directories(self):
        envvar = maybe_path(os.getenv("BRIGHTWAY2_DIR"))
        if envvar:
            if not envvar.is_dir():
                raise OSError(
                    ("BRIGHTWAY2_DIR variable is {}, but this is not" " a valid directory").format(
                        envvar
                    )
                )
            else:
                stdout_feedback_logger.info(
                    "Using environment variable BRIGHTWAY2_DIR for data "
                    "directory:\n{}".format(envvar)
                )
                envvar = envvar.absolute()
                logs_dir = envvar / "logs"
                create_dir(logs_dir)
                return envvar, logs_dir

        dirs = PlatformDirs("Brightway3", "pylca")
        data_dir = Path(dirs.user_data_dir)
        logs_dir = Path(dirs.user_log_dir)
        return data_dir, logs_dir

    def _create_base_directories(self):
        create_dir(self._base_data_dir)
        create_dir(self._base_logs_dir)

    def change_base_directories(
        self,
        base_dir: Path,
        base_logs_dir: Optional[Path] = None,
        project_name: Optional[str] = "default",
        update: Optional[bool] = True,
    ) -> None:
        if not isinstance(base_dir, Path) and base_dir.is_dir():
            raise ValueError(f"{base_dir} is not a `Path` or not a directory")
        if not os.access(base_dir, os.W_OK) and os.access(base_dir, os.R_OK):
            raise ValueError(f"{base_dir} doesn't have read and write permissions")
        self._base_data_dir = base_dir

        if base_logs_dir is not None:
            if not isinstance(base_logs_dir, Path) and base_logs_dir.is_dir():
                raise ValueError(f"{base_logs_dir} is not a `Path` or not a directory")
            if not os.access(base_logs_dir, os.W_OK):
                raise ValueError(f"{base_logs_dir} doesn't have write permission")
            self._base_logs_dir = base_logs_dir

        config.cache = {}
        self.db.change_path(base_dir / "projects.db")
        self.set_current(project_name, update=update)

    def _project_dir(self, p: Optional[ProjectDataset] = None) -> Path:
        p = p or self.dataset
        return Path(self._base_data_dir) / safe_filename(p.name, full=p.full_hash)

    @property
    def current(self):
        return self._project_name

    @property
    def twofive(self):
        return bool(self.dataset.data.get("25"))

    def set_current(self, name, writable=True, update=True):
        self._project_name = str(name)

        # Need to allow writes when creating a new project
        # for new metadata stores
        self.read_only = False
        self.create_project(name)
        self.dataset = ProjectDataset.get(ProjectDataset.name == self._project_name)
        self._reset_meta()
        self._reset_sqlite3_databases()
        project_changed.send(self.dataset)

        if not writable:
            self.read_only = True

        if not self.read_only and update:
            self._do_automatic_updates()

    def _do_automatic_updates(self):
        """Run any available automatic updates"""
        from bw2data.updates import Updates

        for update_name in Updates.check_automatic_updates():
            stdout_feedback_logger.info("Applying automatic update: {}".format(update_name))
            Updates.do_update(update_name)

    def _reset_meta(self):
        for obj in config.metadata:
            obj.__init__()

    def _reset_sqlite3_databases(self):
        for relative_path, substitutable_db in config.sqlite3_databases:
            substitutable_db.change_path(self.dir / relative_path)

    ### Public API
    @property
    def dir(self):
        return self._project_dir()

    @property
    def logs_dir(self):
        return Path(self._base_logs_dir) / safe_filename(self.current, full=self.dataset.full_hash)

    @property
    def output_dir(self):
        """Get directory for output files.

        Uses environment variable ``BRIGHTWAY2_OUTPUT_DIR``; ``preferences['output_dir']``; or directory ``output`` in current project.

        Returns output directory path.

        """
        ep, pp = (
            maybe_path(os.getenv("BRIGHTWAY2_OUTPUT_DIR")),
            maybe_path(config.p.get("output_dir")),
        )
        if ep and ep.is_dir():
            return ep
        elif pp and pp.is_dir():
            return pp
        else:
            return self.request_directory("output")

    def create_project(self, name=None, **kwargs):
        name = name or self.current

        kwargs["25"] = True
        full_hash = kwargs.pop("full_hash", False)
        try:
            self.dataset = ProjectDataset.get(ProjectDataset.name == name)
        except DoesNotExist:
            self.dataset = ProjectDataset.create(data=kwargs, name=name, full_hash=full_hash)
            project_created.send(self.dataset)
        create_dir(self.dir)
        for dir_name in self._basic_directories:
            create_dir(self.dir / dir_name)
        create_dir(self.logs_dir)

    def copy_project(self, new_name, switch=True):
        """Copy current project to a new project named ``new_name``. If ``switch``, switch to new project."""
        if new_name in self:
            raise ValueError("Project {} already exists".format(new_name))
        fp = self._base_data_dir / safe_filename(new_name, full=self.dataset.full_hash)
        if fp.exists():
            raise ValueError("Project directory already exists")
        project_data = ProjectDataset.get(ProjectDataset.name == self.current).data
        ProjectDataset.create(data=project_data, name=new_name, full_hash=self.dataset.full_hash)
        shutil.copytree(self.dir, fp)
        create_dir(self._base_logs_dir / safe_filename(new_name))
        if switch:
            self.set_current(new_name)

    def request_directory(self, name):
        """
        Return the absolute path to the subdirectory `dirname`, creating it if necessary.

        Returns `False` if directory can't be created.
        """
        fp = self.dir / str(name)
        create_dir(fp)
        if not fp.is_dir():
            return False
        return fp

    def migrate_project_25(self):
        """Migrate project to Brightway 2.5.

        Reprocesses all databases and LCIA objects."""
        assert not self.twofive, "Project is already 2.5 compatible"

        from bw2data.updates import Updates

        Updates()._reprocess_all()

        self.dataset.data["25"] = True
        self.dataset.save()

    def delete_project(self, name=None, delete_dir=False):
        """Delete project ``name``, or the current project.

        ``name`` is the project to delete. If ``name`` is not provided, delete the current project.

        By default, the underlying project directory is not deleted; only the project name is removed from the list of active projects. If ``delete_dir`` is ``True``, then also delete the project directory.

        If deleting the current project, this function sets the current directory to ``default`` if it exists, or to a random project.

        Returns the current project."""
        victim = name or self.current
        if victim not in self:
            raise ValueError("{} is not a project".format(victim))

        if len(self) == 1:
            raise ValueError("Can't delete only remaining project")

        ProjectDataset.delete().where(ProjectDataset.name == victim).execute()

        if delete_dir:
            dir_path = self._base_data_dir / safe_filename(victim)
            assert dir_path.is_dir(), "Can't find project directory"
            shutil.rmtree(dir_path)
        else:
            stdout_feedback_logger.warning(
                f"Removing project from project {name} list, but not deleting data; if you switch "
                "to this project again you will have the same data again. To delete data "
                "permanently, pass `(..., delete_dir=True)`."
            )

        if name is None or name == self.current:
            if "default" in self:
                self.set_current("default")
            else:
                self.set_current(next(iter(self)).name)
        return self.current

    def purge_deleted_directories(self):
        """Delete project directories for projects which are no longer registered.

        Returns number of directories deleted."""
        registered = {safe_filename(obj.name) for obj in self}
        bad_directories = [
            self._base_data_dir / dirname
            for dirname in os.listdir(self._base_data_dir)
            if (self._base_data_dir / dirname).is_dir() and dirname not in registered
        ]

        for fp in bad_directories:
            shutil.rmtree(fp)

        return len(bad_directories)

    def report(self):
        """Give a report on current projects, including installed databases and file sizes.

        Returns tuples of ``(project name, number of databases, size of all databases (GB))``.
        """
        from bw2data import databases

        _current = self.current
        data = []

        def get_dir_size(dirpath):
            """Modified from http://stackoverflow.com/questions/12480367/how-to-generate-directory-size-recursively-in-python-like-du-does.

            Does not follow symbolic links"""
            return sum(
                sum(os.path.getsize(Path(root) / name) for name in files)
                for root, dirs, files in os.walk(dirpath)
            )

        names = sorted([x.name for x in self])
        for obj in names:
            self.set_current(obj, update=False, writable=False)
            data.append((obj, len(databases), get_dir_size(projects.dir) / 1e9))
        self.set_current(_current)
        return data

    def rename_project(self, new_name: str) -> None:
        """Rename current project, and switch to the new project."""
        from bw2data import databases

        if new_name in databases:
            raise ValueError(f"Project `{new_name}` already exists.")

        warnings.warn(
            "Renaming current project; this is relatively expensive and could take a little while."
        )

        old_name = copy(self.current)
        self.copy_project(new_name)
        self.set_current(new_name)
        self.delete_project(old_name, delete_dir=True)

    def use_short_hash(self):
        if not self.dataset.full_hash:
            return
        try:
            old_dir, old_logs_dir = self.dir, self.logs_dir
            self.dataset.full_hash = False
            if self.dir.exists():
                raise OSError("Target directory {} already exists".format(self.dir))
            if self.logs_dir.exists():
                raise OSError("Target directory {} already exists".format(self.logs_dir))
            old_dir.rename(self.dir)
            old_logs_dir.rename(self.logs_dir)
            self.dataset.save()
        except Exception as ex:
            self.dataset.full_hash = True
            raise ex

    def use_full_hash(self):
        if self.dataset.full_hash:
            return
        try:
            old_dir, old_logs_dir = self.dir, self.logs_dir
            self.dataset.full_hash = True
            if self.dir.exists():
                raise OSError("Target directory {} already exists".format(self.dir))
            if self.logs_dir.exists():
                raise OSError("Target directory {} already exists".format(self.logs_dir))
            old_dir.rename(self.dir)
            old_logs_dir.rename(self.logs_dir)
            self.dataset.save()
        except Exception as ex:
            self.dataset.full_hash = False
            raise ex


def signal_dispatcher(
    sender, old: Optional[Any] = None, new: Optional[Any] = None, operation: Optional[str] = None
) -> Union[int, None]:
    """Not sure why this is necessary, but fails silently if call `add_revision` directly"""
    from bw2data import revisions

    delta = revisions.generate_delta(old, new, operation)
    if not delta:
        return None
    return projects.dataset.add_revision((delta,))


def signal_dispatcher_generic_no_diff(
    sender, name: str, verb: str, prefix: str, obj_type: str
) -> int:
    from bw2data import revisions

    delta = revisions.Delta(
        # Seems awkward but the whole toolchain assumes a `Delta` object
        delta=deepdiff.Delta(deepdiff.DeepDiff({}, {}, verbose_level=2)),
        obj_type=obj_type,
        obj_id=name,
        change_type=f"{prefix}_{verb}",
    )
    return projects.dataset.add_revision((delta,))


signal_dispatcher_on_database = partial(
    signal_dispatcher_generic_no_diff, prefix="database", obj_type="lci_database"
)
signal_dispatcher_on_project_parameter = partial(
    signal_dispatcher_generic_no_diff, prefix="project_parameter", obj_type="project_parameter"
)
signal_dispatcher_on_database_parameter = partial(
    signal_dispatcher_generic_no_diff, prefix="database_parameter", obj_type="database_parameter"
)
signal_dispatcher_on_activity_parameter = partial(
    signal_dispatcher_generic_no_diff, prefix="activity_parameter", obj_type="activity_parameter"
)


def signal_dispatcher_on_database_write(sender, name: str) -> int:
    from bw2data import revisions
    from bw2data.backends.schema import ActivityDataset, ExchangeDataset

    deltas = [
        revisions.Delta.generate(old=None, new=ds)
        for ds in ActivityDataset.select().where(ActivityDataset.database == name)
    ] + [
        revisions.Delta.generate(old=None, new=exc)
        for exc in ExchangeDataset.select().where(ExchangeDataset.output_database == name)
    ]
    return projects.dataset.add_revision(deltas)


def signal_dispatcher_on_update_formula_parameter_name(
    sender, old: str, new: str, kind: str, extra: str = ""
) -> int:
    from bw2data import revisions

    delta = revisions.Delta(
        delta=deepdiff.Delta(deepdiff.DeepDiff(old, new, verbose_level=2)),
        obj_type=f"{kind}_parameter",
        obj_id="__update_formula_parameter_name_dummy__",
        change_type=f"{kind}_parameter_update_formula_{extra}parameter_name",
    )
    return projects.dataset.add_revision((delta,))


signal_dispatcher_on_project_parameter_update_formula_parameter_name = partial(
    signal_dispatcher_on_update_formula_parameter_name,
    kind="project",
)
signal_dispatcher_on_database_parameter_update_formula_project_parameter_name = partial(
    signal_dispatcher_on_update_formula_parameter_name, kind="database", extra="project_"
)
signal_dispatcher_on_database_parameter_update_formula_database_parameter_name = partial(
    signal_dispatcher_on_update_formula_parameter_name, kind="database", extra="database_"
)
signal_dispatcher_on_activity_parameter_update_formula_project_parameter_name = partial(
    signal_dispatcher_on_update_formula_parameter_name, kind="activity", extra="project_"
)
signal_dispatcher_on_activity_parameter_update_formula_database_parameter_name = partial(
    signal_dispatcher_on_update_formula_parameter_name, kind="activity", extra="database_"
)
signal_dispatcher_on_activity_parameter_update_formula_activity_parameter_name = partial(
    signal_dispatcher_on_update_formula_parameter_name, kind="activity", extra="activity_"
)

# `.connect()` directly just fails silently...
signal_dispatcher_on_activity_database_change = partial(
    signal_dispatcher, operation="activity_database_change"
)
signal_dispatcher_on_activity_code_change = partial(
    signal_dispatcher, operation="activity_code_change"
)
signal_dispatcher_on_database_metadata_change = partial(
    signal_dispatcher, operation="database_metadata_change"
)
signal_dispatcher_on_database_reset = partial(signal_dispatcher_on_database, verb="reset")
signal_dispatcher_on_database_delete = partial(signal_dispatcher_on_database, verb="delete")
signal_dispatcher_on_project_parameter_recalculate = partial(
    signal_dispatcher_on_project_parameter, verb="recalculate", name="__recalculate_dummy__"
)
signal_dispatcher_on_database_parameter_recalculate = partial(
    signal_dispatcher_on_database_parameter, verb="recalculate"
)
signal_dispatcher_on_activity_parameter_recalculate = partial(
    signal_dispatcher_on_activity_parameter, verb="recalculate"
)
signal_dispatcher_on_activity_parameter_recalculate_exchanges = partial(
    signal_dispatcher_on_activity_parameter, verb="recalculate_exchanges"
)

projects = ProjectManager()
bw2signals.signaleddataset_on_save.connect(signal_dispatcher)
bw2signals.signaleddataset_on_delete.connect(signal_dispatcher)
bw2signals.on_activity_database_change.connect(signal_dispatcher_on_activity_database_change)
bw2signals.on_activity_code_change.connect(signal_dispatcher_on_activity_code_change)
bw2signals.on_database_metadata_change.connect(signal_dispatcher_on_database_metadata_change)
bw2signals.on_database_reset.connect(signal_dispatcher_on_database_reset)
bw2signals.on_database_delete.connect(signal_dispatcher_on_database_delete)
bw2signals.on_database_write.connect(signal_dispatcher_on_database_write)
bw2signals.on_project_parameter_recalculate.connect(
    signal_dispatcher_on_project_parameter_recalculate
)
bw2signals.on_database_parameter_recalculate.connect(
    signal_dispatcher_on_database_parameter_recalculate
)
bw2signals.on_activity_parameter_recalculate.connect(
    signal_dispatcher_on_activity_parameter_recalculate
)
bw2signals.on_activity_parameter_recalculate_exchanges.connect(
    signal_dispatcher_on_activity_parameter_recalculate_exchanges
)

bw2signals.on_project_parameter_update_formula_parameter_name.connect(
    signal_dispatcher_on_project_parameter_update_formula_parameter_name
)
bw2signals.on_database_parameter_update_formula_project_parameter_name.connect(
    signal_dispatcher_on_database_parameter_update_formula_project_parameter_name
)
bw2signals.on_database_parameter_update_formula_database_parameter_name.connect(
    signal_dispatcher_on_database_parameter_update_formula_database_parameter_name
)
bw2signals.on_activity_parameter_update_formula_project_parameter_name.connect(
    signal_dispatcher_on_activity_parameter_update_formula_project_parameter_name
)
bw2signals.on_activity_parameter_update_formula_database_parameter_name.connect(
    signal_dispatcher_on_activity_parameter_update_formula_database_parameter_name
)
bw2signals.on_activity_parameter_update_formula_activity_parameter_name.connect(
    signal_dispatcher_on_activity_parameter_update_formula_activity_parameter_name
)


@wrapt.decorator
def writable_project(wrapped, instance, args, kwargs):
    warnings.warn("`writable_project` is obsolete and does nothing", DeprecationWarning)
    return wrapped(*args, **kwargs)
