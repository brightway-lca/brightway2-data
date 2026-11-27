import json
import os
import shutil
import uuid
import warnings
from collections.abc import Iterable
from contextlib import closing
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
from bw2data.signals import project_changed, project_created, project_deleted
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

    # Event sourcing keeps two revision pointers (#316):
    #
    # * ``self.revision`` (the ``revision`` column in ``projects.db``) is the last revision
    #   applied to this project's data.
    # * ``revisions/head`` is the tip of the revision log in ``revisions/``, which can be
    #   shared with collaborators; ``load_revisions`` applies what lies between the two.
    #
    # Both are normally equal. ``add_revision`` writes the revision file, then the column, then
    # the head file, so the head file is written last and a crash leaves either a revision file
    # which nothing points to (ignored) or a column which is ahead of the head file (repaired
    # by ``load_revisions``). Files are written to a temporary file and moved into place, so a
    # crash of the process never leaves a partly written revision or head. They are not
    # fsynced (a revision is written on every change), so a power cut still can; a head file
    # which can't be read is replaced by the column, see ``_read_head``.

    @staticmethod
    def _write_atomically(path: Path, text: str) -> None:
        """Write ``text`` to ``path`` so that ``path`` is never partly written."""
        # Doesn't end with `.rev`, so `_load_revisions` never reads it
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(text, encoding="utf-8")
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def _write_revision(self, revision: "revisions.Revision"):
        """Write revision to disk."""
        from bw2data import revisions

        rev_id = revision["metadata"]["revision"]
        self._write_atomically(
            self.dir / "revisions" / f"{rev_id}.rev",
            revisions.JSONEncoder(indent=2).encode(revision),
        )

    def _write_head(self, head: Optional["revisions.ID"] = None):
        """Write the tip of the revision log to disk; default is ``self.revision``."""
        head = head if head is not None else self.revision
        if head is None:
            # Nothing applied yet; `None` is not a head which can be read back
            return
        self._write_atomically(self.dir / "revisions" / "head", str(head))

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

    def _read_head(self) -> Optional["revisions.ID"]:
        """Read the head file. A head file which can't be read, e.g. one left empty by a crash
        of a version which didn't write it atomically (#316), is replaced by the project's
        revision, with a warning; ``None`` if the project has none."""
        path = self.dir / "revisions" / "head"
        content = path.read_text(encoding="utf-8")
        try:
            return int(content)
        except ValueError:
            pass
        if self.revision is None:
            warnings.warn(
                f"Ignoring {path}, which doesn't hold a revision id ({content[:40]!r}); "
                "this project has no revision to replace it with"
            )
            return None
        warnings.warn(
            f"{path} doesn't hold a revision id ({content[:40]!r}); replacing it with the "
            f"project's revision, {self.revision}. Revisions received from collaborators "
            "since it was damaged are only loaded with `load_revisions(head)`."
        )
        self._write_head()
        return self.revision

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
            head = self._read_head()
            if head is None:
                return None
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
        if (
            self.revision is not None
            and self.revision != g.head
            and self.revision in g.id_map
            # Stops at the head; doesn't walk the history below it
            and g.merge_base(self.revision, g.head) == g.head
        ):
            # The data is ahead of the log: `add_revision` stopped after saving the column but
            # before writing the head file. Nothing to apply, only the head file to catch up.
            # The column is saved first, as everywhere, in case it only changed in memory.
            self.save()
            self._write_head()
            return
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
        # ``None`` on the very first call, when there is no project to go back to
        previous = (
            getattr(self, "_project_name", None),
            getattr(self, "dataset", None),
            self.read_only,
        )
        self._project_name = str(name)

        # Need to allow writes when creating a new project
        # for new metadata stores
        self.read_only = False
        try:
            self.create_project(name)
            self.dataset = ProjectDataset.get(ProjectDataset.name == self._project_name)
            self._reset_meta()
            self._reset_sqlite3_databases()
        except BaseException:
            self._restore_project(*previous)
            raise
        project_changed.send(self, dataset=self.dataset)

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

    def _restore_project(self, name, dataset, read_only):
        """Go back to the project ``name`` after a failed switch to another project.

        A switch changes the project name, the metadata stores and each SQLite file in turn; if
        one of these steps fails, the others must not stay on the new project."""
        if name is None or dataset is None:
            return
        self._project_name, self.dataset, self.read_only = name, dataset, read_only
        try:
            self._reset_meta()
            self._reset_sqlite3_databases()
        except Exception:
            stdout_feedback_logger.exception(
                "Can't go back to project %s after a failed project switch", name
            )

    def _reset_meta(self):
        for obj in config.metadata:
            obj.__init__()

    def _reset_sqlite3_databases(self):
        lci_db = self._lci_db()
        # A project with extensions opens its LCI file only once, with them, below
        deferred = lci_db if lci_db is not None and self._stored_extensions() else None
        for relative_path, substitutable_db in config.sqlite3_databases:
            if substitutable_db is not deferred:
                substitutable_db.change_path(self.dir / relative_path)
        self._attach_stored_extensions(opened=deferred is None)

    ### Read-only LCI extensions

    #: Key of ``ProjectDataset.data`` with the extensions of a project: a list of dicts with
    #: ``name``, ``path`` (absolute), ``id``, ``immutable`` and ``allow_main_requirements``.
    #: Stored in ``projects.db``, so that they are known before the project's files are opened.
    _EXTENSIONS_KEY = "lci_extensions"
    _LCI_DATABASE = os.path.join("lci", "databases.db")
    #: ``{name: (status, problem)}`` of the current project's extensions which are not attached
    _extension_problems: dict = {}

    def _lci_db(self) -> Optional[SubstitutableDatabase]:
        """The LCI database file object; ``None`` while ``bw2data`` is being imported."""
        for relative_path, substitutable_db in config.sqlite3_databases:
            if str(relative_path) == self._LCI_DATABASE:
                return substitutable_db
        return None

    def _stored_extensions(self) -> list[dict]:
        return list(self.dataset.data.get(self._EXTENSIONS_KEY) or [])

    def _usable_extensions(self, stored: list[dict]) -> tuple[list, dict]:
        """``usable_extensions`` for the stored entries ``stored``."""
        from bw2data.backends.extensions import ExtensionSpec, usable_extensions

        if not stored:
            return [], {}
        specs = [
            ExtensionSpec(
                name=entry["name"],
                path=entry["path"],
                immutable=bool(entry.get("immutable")),
                allow_main_requirements=bool(entry.get("allow_main_requirements")),
            )
            for entry in stored
        ]
        return usable_extensions(
            self.dir / self._LCI_DATABASE,
            specs,
            ids={entry["name"]: entry["id"] for entry in stored},
        )

    def _attach_stored_extensions(self, opened: bool = True, previous: Optional[dict] = None):
        """Open the LCI file of the current project with its stored extensions.

        An extension which can't be attached (missing file, replaced file, missing
        requirement, ...) is left out with a warning, together with the extensions which need
        it; the project still opens. ``projects.extensions`` then shows why. Only an error in
        opening the project's own file is passed on.

        ``opened`` is false if the LCI file of the project is not open yet. ``previous`` are
        the problems from before an attach or detach; only changed ones are warned about."""
        self._extension_problems = {}
        lci_db = self._lci_db()
        if lci_db is None:
            # ``bw2data.backends`` calls this again once the file is open
            return
        stored = self._stored_extensions()
        path = self.dir / self._LCI_DATABASE
        if not stored:
            if lci_db.extensions or not opened:
                lci_db.change_path(path)
            return

        from bw2data.backends.extensions import INVALID

        def all_invalid(names, err):
            return {name: (INVALID, str(err)) for name in names}

        stored_names = [entry["name"] for entry in stored]
        try:
            usable, problems = self._usable_extensions(stored)
        except Exception as err:
            if opened:
                usable, problems = [], all_invalid(stored_names, err)
            else:
                # E.g. tables which are only created when the file is opened
                lci_db.change_path(path)
                opened = True
                try:
                    usable, problems = self._usable_extensions(stored)
                except Exception as err:
                    usable, problems = [], all_invalid(stored_names, err)
        if usable or lci_db.extensions or not opened:
            try:
                lci_db.change_path(path, extensions=usable)
            except Exception as err:
                # The files don't fit together, or to the project's own databases, or can't be
                # read. ``change_path`` went back to what was open before, maybe another file.
                problems.update(all_invalid([spec.name for spec in usable], err))
                lci_db.change_path(path)
        self._extension_problems = problems
        self._track_lcia_files(lci_db)
        self._warn_extension_problems(problems, previous)

    #: Keys of a stored extension entry: the file names of the processed datapackages of its
    #: LCIA objects, as of when it was last attached, and whether it was not available when
    #: the project last looked
    _LCIA_FILES_KEY = "lcia_files"
    _UNAVAILABLE_KEY = "unavailable"

    def _track_lcia_files(self, lci_db) -> None:
        """Keep the stored entries of the extensions up to date with what is attached now,
        and remove the processed data in the project under the names of the LCIA objects of
        an extension which is available again. While it was not, the project may have had
        objects of its own with these names, whose processed data would otherwise be taken
        for the extension's.

        The entry of an attached extension gets the file names of the processed data of its
        LCIA objects, so that they can be removed if it is detached while not available."""
        stored = self._stored_extensions()
        attached = {e.name: e for e in lci_db.extensions}
        changed, stale = False, []
        for position, entry in enumerate(stored):
            entry = stored[position] = dict(entry)
            extension = attached.get(entry["name"])
            if extension is None:
                if not entry.get(self._UNAVAILABLE_KEY):
                    entry[self._UNAVAILABLE_KEY] = True
                    changed = True
                continue
            files = self._lcia_processed_files(lci_db, [extension])
            names = sorted(path.name for path in files)
            if entry.get(self._LCIA_FILES_KEY, []) != names:
                entry[self._LCIA_FILES_KEY] = names
                changed = True
            if entry.pop(self._UNAVAILABLE_KEY, False):
                stale += files
                changed = True
        if changed:
            self._store_extensions(stored)
        self._remove_files(stale, "LCIA object", stacklevel=4)

    def _warn_extension_problems(self, problems: dict, previous: Optional[dict] = None) -> None:
        from bw2data.sqlite import _user_stacklevel

        for name, (status, problem) in problems.items():
            if previous is not None and previous.get(name, (None,))[0] == status:
                continue
            warnings.warn(
                f"Extension {name!r} of project {self.current!r} is not available ({status}): "
                f"{problem}. Its databases are not in the project until this is solved; "
                "`projects.detach_extension` removes the extension from the project.",
                stacklevel=_user_stacklevel(),
            )

    @property
    def extensions(self) -> tuple:
        """The read-only LCI extensions of the current project, as
        ``bw2data.backends.extensions.ProjectExtension``: ``name``, ``path``, ``id``, ``status``
        (``"available"``, ``"missing file"``, ``"missing requirement"`` or ``"invalid"``),
        ``problem``, ``databases``, ``methods``, ``normalizations`` and ``weightings``."""
        stored = self._stored_extensions()
        if not stored:
            return ()
        from bw2data.backends.extensions import AVAILABLE, INVALID, ProjectExtension

        lci_db = self._lci_db()
        attached = {e.name: e for e in lci_db.extensions} if lci_db is not None else {}
        result = []
        for entry in stored:
            name = entry["name"]
            status, problem = self._extension_problems.get(name, (AVAILABLE, None))
            if name not in attached and status == AVAILABLE:
                status, problem = INVALID, "the extension is not attached"
            result.append(
                ProjectExtension(
                    name=name,
                    path=Path(entry["path"]),
                    id=entry["id"],
                    immutable=bool(entry.get("immutable")),
                    allow_main_requirements=bool(entry.get("allow_main_requirements")),
                    status=status,
                    problem=problem,
                    databases=attached[name].databases if name in attached else (),
                    methods=attached[name].methods if name in attached else (),
                    normalizations=attached[name].normalizations if name in attached else (),
                    weightings=attached[name].weightings if name in attached else (),
                )
            )
        return tuple(result)

    def attach_extension(
        self,
        path,
        name: Union[str, Sequence[str], None] = None,
        immutable: bool = False,
        allow_main_requirements: bool = False,
    ) -> tuple:
        """Attach read-only LCI extension files to the current project.

        The databases stored in an extension are read like the project's own, and the
        project's databases can link to them, but they can't be changed. The extensions are
        stored with the project, and attached again every time it is opened.

        ``path`` is one file or a list of files. Files which need each other must be given
        together, in any order, unless the ones they need are already attached. ``name``
        identifies an extension in the project; for a list, it is a list of names in the same
        order. The default is the file name without its suffix, followed by ``_2``, ``_3``,
        ... if the project already has an extension with this name. ``immutable`` and
        ``allow_main_requirements`` are as in ``bw2data.backends.extensions.ExtensionSpec``.

        Raises ``bw2data.errors.ExtensionError`` (``ExtensionDependencyError`` for
        requirements) if the files can't be attached; see
        ``bw2data.backends.extensions.check_extensions``. Nothing is changed then.

        Stored extensions which were not available are attached as well if they now can be,
        e.g. because a new file is what they were missing.

        Returns the new entries of ``projects.extensions``."""
        from bw2data.backends.extensions import (
            INVALID,
            ExtensionSpec,
            check_extensions,
            requirement_order,
        )
        from bw2data.errors import ExtensionError

        single = isinstance(path, (str, os.PathLike))
        paths = [path] if single else list(path)
        if not paths:
            raise ValueError("No extension file given")
        if name is None:
            given_names = [None] * len(paths)
        elif isinstance(name, str):
            if not single:
                raise ValueError("For a list of files, `name` must be a list of names")
            given_names = [name]
        else:
            given_names = list(name)
            if len(given_names) != len(paths):
                raise ValueError(f"{len(paths)} files, but {len(given_names)} names")
        lci_db = self._lci_db()
        stored = self._stored_extensions()
        names = {entry["name"] for entry in stored}
        stored_paths = {Path(entry["path"]): entry["name"] for entry in stored}
        # Names given explicitly are not taken by default names
        reserved = {given for given in given_names if given is not None}
        new_specs, new_paths = [], {}
        for filepath, spec_name in zip(paths, given_names):
            filepath = Path(filepath).resolve()
            if filepath in stored_paths:
                raise ExtensionError(
                    f"{filepath} is already an extension of this project, named "
                    f"{stored_paths[filepath]!r}"
                )
            if filepath in new_paths:
                raise ExtensionError(f"{filepath} is given twice")
            new_paths[filepath] = True
            if spec_name is None:
                spec_name, number = filepath.stem, 1
                while spec_name in names or spec_name in reserved:
                    number += 1
                    spec_name = f"{filepath.stem}_{number}"
            elif spec_name in names:
                raise ExtensionError(
                    f"This project already has an extension named {spec_name!r}; give "
                    f"{filepath} another `name`"
                )
            names.add(spec_name)
            new_specs.append(
                ExtensionSpec(
                    name=spec_name,
                    path=filepath,
                    immutable=immutable,
                    allow_main_requirements=allow_main_requirements,
                )
            )

        main_path = self.dir / self._LCI_DATABASE
        # With the extensions which are attached now
        attached_now = [e.spec for e in lci_db.extensions]
        attached = check_extensions(main_path, attached_now + new_specs)
        new_names = [spec.name for spec in new_specs]
        stored_ids = {entry["id"]: entry["name"] for entry in stored}
        entries = []
        for extension in requirement_order(attached):
            if extension.name not in new_names:
                continue
            if extension.id in stored_ids:
                raise ExtensionError(
                    f"{extension} is already an extension of this project, named "
                    f"{stored_ids[extension.id]!r} (id {extension.id})"
                )
            entries.append(
                {
                    "name": extension.name,
                    "path": str(extension.path),
                    "id": extension.id,
                    # To clean up after an extension which is detached while unavailable
                    "databases": list(extension.databases),
                    "immutable": extension.spec.immutable,
                    "allow_main_requirements": extension.spec.allow_main_requirements,
                }
            )

        # Stored extensions which were not available come back if they can
        usable, problems = self._usable_extensions(stored + entries)
        for spec_name in new_names:
            if spec_name in problems:
                raise ExtensionError(
                    f"Can't attach extension {spec_name!r}: {problems[spec_name][1]}"
                )
        try:
            check_extensions(main_path, usable)
        except ExtensionError as err:
            # Some of those which come back don't fit (any more); they stay unavailable
            keep = {spec.name for spec in attached_now} | set(new_names)
            problems.update(
                {spec.name: (INVALID, str(err)) for spec in usable if spec.name not in keep}
            )
            usable = [spec for spec in usable if spec.name in keep]

        previous = dict(self._extension_problems)
        self._store_extensions(stored + entries)
        try:
            lci_db.change_path(main_path, extensions=usable)
        except BaseException:
            # ``change_path`` went back to the file with the extensions attached before
            self._store_extensions(stored)
            raise
        before = {spec.name for spec in attached_now}
        new_databases = [
            database
            for extension in lci_db.extensions
            if extension.name not in before
            for database in extension.databases
        ]
        try:
            # The project has no processed data for the databases which it now gets, or data
            # of another version of the file: start from a new local state, to be processed
            new_extensions = [e for e in lci_db.extensions if e.name not in before]
            self._add_lcia_locations(lci_db, new_extensions)
            # Of other versions of these LCIA objects, or of objects of the project which had
            # the same names
            stale = self._lcia_processed_files(lci_db, new_extensions)
            self._start_extension_databases(lci_db, new_databases)
        except BaseException:
            # Nothing is attached which wasn't before
            self._store_extensions(stored)
            lci_db.change_path(main_path, extensions=attached_now)
            raise
        self._remove_files(stale, "LCIA object", stacklevel=3)
        self._extension_problems = problems
        self._track_lcia_files(lci_db)
        self._warn_extension_problems(problems, previous)
        bw2signals.on_extension_attached.send(self, names=[entry["name"] for entry in entries])
        return tuple(e for e in self.extensions if e.name in new_names)

    def export_extension(
        self,
        filepath: Union[str, Path],
        databases: Union[str, Sequence[str]],
        version: Optional[str] = None,
        overwrite: bool = False,
        methods: Sequence[tuple] = (),
        normalizations: Sequence[tuple] = (),
        weightings: Sequence[tuple] = (),
    ) -> str:
        """Write databases of the current project into a new read-only LCI extension file
        ``filepath``, which any project can attach with ``attach_extension``. Returns the id of
        the new extension. ``version`` is a label for people, e.g. ``"3.11"``.

        The databases must be in the project's own file. They may link to each other, and to
        databases in the extensions attached to the project: the new file then requires those
        extensions, in exactly the versions attached now, and can only be attached together
        with them. Links to any other database are refused (``bw2data.errors.ExtensionError``),
        as an extension can't depend on data which can still change. IO table databases are
        refused as well.

        The file holds the databases' activities, with their ids, their exchanges, with new
        ids, their metadata (``depends`` worked out from the exchanges), their search indexes,
        and their processed datapackages, so that projects which attach the file don't have to
        process them; nothing else of the project. Databases which are ``dirty``, or have no
        processed datapackage, are processed first. Only committed changes are exported.
        Raises ``FileExistsError`` if ``filepath`` exists, unless ``overwrite``; see
        ``bw2data.backends.extensions.build_extension``.

        ``methods``, ``normalizations`` and ``weightings`` are lists of names of LCIA objects
        of the project to put into the file as well, with their metadata and factors. Methods
        and normalizations can only be exported together with the databases of all the flows
        they have factors for, e.g. the database of elementary flows which a set of methods
        characterizes: in an extension, an object and its flows are always attached and
        detached together. The file holds a fingerprint of each object (its name and factors),
        listed by ``AttachedExtension.lcia_fingerprints``. The objects are not processed; a
        project which attaches the file processes them when they are first used."""
        from bw2data.backends.extensions import (
            _lcia_export_names,
            build_extension,
            check_export,
        )

        lci_db = self._lci_db()
        if lci_db is None:
            raise RuntimeError("No project is open")
        if lci_db.db.in_transaction():
            raise RuntimeError("Can't export inside a transaction: only committed data is read")
        databases = [databases] if isinstance(databases, str) else list(databases)
        # Read once here: the names may come from a generator
        lcia = _lcia_export_names(
            {"method": methods, "normalization": normalizations, "weighting": weightings}
        )
        main_path = self.dir / self._LCI_DATABASE
        # Before anything is processed: an export which is refused changes nothing
        check_export(
            main_path,
            filepath,
            databases,
            version,
            attached=lci_db.extensions,
            overwrite=overwrite,
            lcia=lcia,
        )
        return build_extension(
            main_path,
            filepath,
            databases,
            version=version,
            attached=lci_db.extensions,
            overwrite=overwrite,
            datapackages=self._processed_for_export(databases),
            lcia=lcia,
        )

    @staticmethod
    def _processed_for_export(databases) -> dict:
        """``{database: path of its processed datapackage}`` for the databases ``databases``,
        which ``check_export`` accepted. They are processed first if they are ``dirty`` or
        have none."""
        from bw2data import Database

        result = {}
        for name in databases:
            database = Database(name)
            # Processes the database if it is dirty
            path = database.filepath_processed()
            if not path.is_file():
                database.process()
            result[name] = path
        return result

    def detach_extension(self, name: str, force: bool = False) -> None:
        """Remove the read-only LCI extension ``name`` from the current project. The file itself
        is not touched.

        Refused with ``bw2data.errors.ExtensionError`` while another attached extension
        requires it, or while the project has exchanges whose input is in it, or
        characterization or normalization factors for its flows. ``force=True`` detaches
        anyway, with a warning: the extensions which require it are then no longer available,
        and such exchanges and factors point at nothing."""
        from bw2data.errors import ExtensionError

        stored = self._stored_extensions()
        if name not in {entry["name"] for entry in stored}:
            raise ValueError(f"This project has no extension named {name!r}")
        lci_db = self._lci_db()
        attached = {e.name: e for e in lci_db.extensions}
        extension = attached.get(name)
        if extension is not None:
            databases = extension.databases
        else:
            # Not available: the databases it had when it was attached
            (entry,) = [entry for entry in stored if entry["name"] == name]
            databases = tuple(entry.get("databases") or ())
        if extension is not None:
            problems = [
                f"extension {other.name!r} requires it"
                for other in attached.values()
                if other is not extension
                and any(required_id == extension.id for _, required_id, _ in other.requirements)
            ]
            if databases:
                problems.extend(self._extension_users(lci_db, extension))
            if problems:
                message = f"extension {name!r}: " + "; ".join(problems)
                if not force:
                    raise ExtensionError(
                        f"Can't detach {message}. Pass `force=True` to detach it anyway."
                    )
                warnings.warn(f"Detaching {message}", stacklevel=2)
        if extension is not None:
            # While the file is attached: the names of its LCIA objects are read from it
            lcia_files = self._lcia_processed_files(lci_db, [extension])
        else:
            # Not available: of the objects it had when it was last attached. If the project
            # has objects of its own with these names by now, they are processed again
            (entry,) = [entry for entry in stored if entry["name"] == name]
            lcia_files = [
                self.dir / "processed" / filename
                for filename in entry.get(self._LCIA_FILES_KEY) or ()
            ]
        previous = dict(self._extension_problems)
        self._store_extensions([entry for entry in stored if entry["name"] != name])
        try:
            self._attach_stored_extensions(previous=previous)
        except BaseException:
            self._store_extensions(stored)
            raise
        self._remove_files(lcia_files, "LCIA object", stacklevel=3)
        signal_databases = tuple(databases) if extension is not None else ()
        if databases and extension is None:
            databases = self._unused_databases(lci_db, databases)
        if databases:
            self._forget_extension_databases(lci_db, databases, stacklevel=3)
        bw2signals.on_extension_detached.send(self, name=name, databases=signal_databases)

    def update_extension(self, name: str, path=None, force: bool = False):
        """Use a new version of the read-only LCI extension ``name``: the file which is at its
        path now, e.g. after it was replaced by a new build, or the file ``path``. The
        extension keeps its name and options, and the project's links into it stay as they
        are. This also brings back an extension which is not available because its file was
        replaced or moved.

        If the file is another version of the extension (it has another id), the project's
        local state of the extension's databases is reset, as when it is attached: their
        processed data and the search indexes built in the project are removed, they are
        marked ``dirty``, to be processed again, and their locations are added to
        ``geomapping``. ``on_extension_detached`` and then ``on_extension_attached`` are sent.
        If it is the same version, e.g. the same file at another path, only the path changes.

        Raises ``bw2data.errors.ExtensionError`` if the file can't be attached (see
        ``attach_extension``). For another version, it is also refused while another attached
        extension requires the extension (a requirement names one version), while databases
        of the project have exchanges with inputs which are not in the new version, or while
        methods or normalizations of the project have factors for flows which are in none of
        the files any more. ``force=True`` updates anyway, with a warning: the extensions which
        require it are then no longer available, and such exchanges and factors point at
        nothing. Nothing is changed if it raises.

        Replace the file of an extension only while no project uses it, or build the new
        version at another path and pass it as ``path``. What happens to a project which has
        the file open while it is replaced is undefined.

        Returns the extension's new entry of ``projects.extensions``."""
        from bw2data.backends.extensions import (
            INVALID,
            ExtensionSpec,
            check_extensions,
            extension_id,
            missing_link_targets,
        )
        from bw2data.errors import ExtensionError

        stored = self._stored_extensions()
        names = [entry["name"] for entry in stored]
        if name not in names:
            raise ValueError(f"This project has no extension named {name!r}")
        position = names.index(name)
        entry = stored[position]
        filepath = Path(entry["path"]) if path is None else Path(path).resolve()
        new_id = extension_id(filepath)
        for other in stored:
            if other["name"] == name:
                continue
            if Path(other["path"]) == filepath:
                raise ExtensionError(
                    f"{filepath} is already an extension of this project, named {other['name']!r}"
                )
            if other["id"] == new_id:
                raise ExtensionError(
                    f"{filepath} is already an extension of this project, named "
                    f"{other['name']!r} (id {new_id})"
                )
        lci_db = self._lci_db()
        main_path = self.dir / self._LCI_DATABASE
        attached = {e.name: e for e in lci_db.extensions}
        old = attached.get(name)
        same = new_id == entry["id"]
        if same and old is not None and filepath == Path(entry["path"]):
            # Nothing has changed
            return next(e for e in self.extensions if e.name == name)

        # A requirement names one version: the extensions which require the old one, also
        # through others, are no longer available with the new one
        dependents, gone = [], {entry["id"]}
        while not same:
            found = [
                other
                for other in attached.values()
                if other.name != name
                and other.name not in dependents
                and any(required_id in gone for _, required_id, _ in other.requirements)
            ]
            if not found:
                break
            dependents += [other.name for other in found]
            gone.update(other.id for other in found)
        spec = ExtensionSpec(
            name=name,
            path=filepath,
            immutable=bool(entry.get("immutable")),
            allow_main_requirements=bool(entry.get("allow_main_requirements")),
        )
        others = [e.spec for e in lci_db.extensions if e.name != name and e.name not in dependents]
        # Raises if the new file doesn't fit to the project and the extensions which stay
        new = check_extensions(main_path, others + [spec])[-1]

        if not same:
            broken = [
                f"extension {other.name!r} requires it"
                for other in attached.values()
                if other.name != name
                and any(required_id == entry["id"] for _, required_id, _ in other.requirements)
            ]
            old_databases = (
                old.databases if old is not None else tuple(entry.get("databases") or ())
            )
            flows = self._extension_flows(lci_db, old)
            unavailable = []
            if flows is None:
                # The old version can't be read, so every flow with factors is looked for.
                # Those in the files of the other extensions which are not available are not
                # missing because of this update
                unavailable = [
                    ExtensionSpec(name=other["name"], path=other["path"])
                    for other in stored
                    if other["name"] != name and other["name"] not in attached
                ]
            users, methods, normalizations = missing_link_targets(
                main_path, others + [spec], old_databases, flows, also=unavailable
            )
            if users:
                broken.append(
                    "these databases of the project have exchanges with inputs which are not "
                    "in the new version: " + ", ".join(repr(user) for user in users)
                )
            if flows is None:
                where = (
                    "in none of the project's files (the old version can't be read, so they "
                    "may not have been its flows)"
                )
            else:
                where = "not in the new version"
            for label, records in (("methods", methods), ("normalizations", normalizations)):
                if records:
                    broken.append(
                        f"these {label} have factors for flows which are {where}: "
                        + ", ".join(repr(record) for record in records)
                    )
            if broken:
                message = f"extension {name!r} to {filepath} (id {new_id}): " + "; ".join(broken)
                if not force:
                    raise ExtensionError(
                        f"Can't update {message}. Pass `force=True` to update it anyway."
                    )
                warnings.warn(f"Updating {message}", stacklevel=2)

        updated = dict(entry, path=str(filepath), id=new.id, databases=list(new.databases))
        new_stored = stored[:position] + stored[position + 1 :]
        # Stored after the extensions it requires, as ``attach_extension`` does
        required = {required_id for _, required_id, _ in new.requirements}
        after = [i for i, other in enumerate(new_stored) if other["id"] in required]
        new_stored.insert(max([position] + [i + 1 for i in after]), updated)

        previous = dict(self._extension_problems)
        processed = []
        # Of the old version's LCIA objects, read while its file is attached
        lcia_files = [] if same or old is None else self._lcia_processed_files(lci_db, [old])
        self._store_extensions(new_stored)
        try:
            usable, problems = self._usable_extensions(new_stored)
            if name in problems:
                raise ExtensionError(f"Can't update extension {name!r}: {problems[name][1]}")
            try:
                check_extensions(main_path, usable)
            except ExtensionError as err:
                # Stored extensions which come back don't fit (any more); they stay unavailable
                keep = {other.name for other in others} | {name}
                problems.update({s.name: (INVALID, str(err)) for s in usable if s.name not in keep})
                usable = [s for s in usable if s.name in keep]
            lci_db.change_path(main_path, extensions=usable)
            if not same:
                # The databases start again, as in a newly attached extension, and the names
                # which the new version doesn't have are forgotten
                forgotten = tuple(db for db in old_databases if db not in new.databases)
                if old is None:
                    # The names it had when it was last attached
                    forgotten = self._unused_databases(lci_db, forgotten)
                # Before the local state changes: if this fails, it is the old version's
                updated_extension = [e for e in lci_db.extensions if e.name == name]
                self._add_lcia_locations(lci_db, updated_extension)
                lcia_files += self._lcia_processed_files(lci_db, updated_extension)
                # In one transaction: if this fails, the local state is the old version's
                with lci_db.atomic():
                    self._start_extension_databases(lci_db, new.databases)
                    self._forget_extension_databases(lci_db, forgotten, processed=False)
                processed = [db for db in old_databases if db in new.databases] + list(forgotten)
        except BaseException:
            # The extension stays as it was: available, if its old file can still be read, or
            # with the problem which keeps it from being attached
            self._store_extensions(stored)
            self._attach_stored_extensions(previous=previous)
            raise
        # The processed data of the old version; only now, as files don't come back
        self._remove_processed(processed, stacklevel=3)
        self._remove_files(lcia_files, "LCIA object", stacklevel=3)
        self._extension_problems = problems
        self._track_lcia_files(lci_db)
        self._warn_extension_problems(problems, previous)
        if not same:
            # As ``detach_extension``: an extension which was not available had no databases
            detached = tuple(old_databases) if old is not None else ()
            bw2signals.on_extension_detached.send(self, name=name, databases=detached)
            bw2signals.on_extension_attached.send(self, names=[name])
        elif old is None:
            bw2signals.on_extension_attached.send(self, names=[name])
        return next(e for e in self.extensions if e.name == name)

    @staticmethod
    def _extension_flows(lci_db, extension) -> Optional[set]:
        """Ids of the flows of the attached ``extension`` with factors in the project's
        methods or normalizations; ``None`` if it is not attached or can't be read."""
        import sqlite3

        from peewee import DatabaseError

        from bw2data.errors import ExtensionError

        if extension is None:
            return None
        flows = (
            f'SELECT "id" FROM "{extension.schema}"."activitydataset" '
            f'WHERE "database" IN ({", ".join("?" * len(extension.databases))})'
        )
        result = set()
        try:
            for table in ("characterizationfactor", "normalizationfactor"):
                sql = f'SELECT DISTINCT "flow" FROM main."{table}" WHERE "flow" IN ({flows})'
                with closing(lci_db.execute_sql(sql, extension.databases)) as cursor:
                    result.update(row[0] for row in cursor.fetchall())
        except (DatabaseError, sqlite3.Error, ExtensionError):
            # E.g. a new connection, to which the file of an extension can't be attached
            return None
        return result

    @staticmethod
    def _unused_databases(lci_db, databases) -> tuple:
        """Those of the names ``databases``, of an extension which was not available, which
        neither the project's own file nor an attached extension uses by now."""
        databases = tuple(databases)
        if not databases:
            return ()
        marks = ", ".join("?" * len(databases))
        sql = f'SELECT "name" FROM main."databaserecord" WHERE "name" IN ({marks})'
        with closing(lci_db.execute_sql(sql, databases)) as cursor:
            used = {row[0] for row in cursor.fetchall()}
        used.update(lci_db.extension_databases())
        return tuple(database for database in databases if database not in used)

    @staticmethod
    def _forget_extension_databases(
        lci_db, databases, stacklevel: int = 3, processed: bool = True
    ) -> None:
        """Remove the project's local state of the extension databases ``databases``: the
        ``ExtensionDatabaseState`` rows, search indexes built in the project, and processed
        data (unless ``processed`` is false). Geomapping entries stay, as other processed data
        may use them."""
        from bw2data.backends.schema import ExtensionDatabaseState
        from bw2data.search.indices import index_table

        ExtensionDatabaseState.delete().where(
            ExtensionDatabaseState.name.in_(list(databases))
        ).execute()
        for database in databases:
            # Indexes built in the project for databases whose file had none
            lci_db.execute_sql(f'DROP TABLE IF EXISTS main."{index_table(database)}"')
        if processed:
            ProjectManager._remove_processed(databases, stacklevel=stacklevel + 1)

    @staticmethod
    def _remove_processed(databases, stacklevel: int = 3) -> None:
        from bw2data.backends import SQLiteBackend

        for database in databases:
            backend = SQLiteBackend(database)
            processed = backend.dirpath_processed() / backend.filename_processed()
            try:
                processed.unlink(missing_ok=True)
            except OSError as err:
                # E.g. still open in a calculation, on Windows. Nothing is undone for this
                warnings.warn(
                    f"Can't remove the processed data of database {database!r} "
                    f"({processed}): {err}",
                    stacklevel=stacklevel,
                )

    @staticmethod
    def _remove_files(paths, what: str, stacklevel: int = 3) -> None:
        """Remove the processed datapackages ``paths``, with a warning for those which can't
        be removed."""
        for path in paths:
            try:
                path.unlink(missing_ok=True)
            except OSError as err:
                # E.g. still open in a calculation, on Windows. Nothing is undone for this
                warnings.warn(
                    f"Can't remove the processed data of an {what} ({path}): {err}",
                    stacklevel=stacklevel,
                )

    def _lcia_processed_files(self, lci_db, extensions) -> list:
        """Paths of the processed datapackages, in the project, of the LCIA objects stored in
        the attached ``extensions``. They are not kept when an extension is attached, updated
        or detached: another version of an object with the same name has the same file."""
        from bw_processing import clean_datapackage_name

        from bw2data.backends.extensions import LCIA_KINDS, _q, _table
        from bw2data.ia_data_store import abbreviate

        paths = []
        for extension in extensions:
            for kind in LCIA_KINDS:
                if not extension.lcia.get(kind.name):
                    continue
                sql = (
                    f"SELECT name, abbreviation FROM {_q(extension.schema)}."
                    f"{_q(_table(kind.record))}"
                )
                with closing(lci_db.execute_sql(sql)) as cursor:
                    rows = cursor.fetchall()
                for name, abbreviation in rows:
                    name = kind.record.name.python_value(name)
                    filename = clean_datapackage_name((abbreviation or abbreviate(name)) + ".zip")
                    paths.append(self.dir / "processed" / filename)
        return paths

    @staticmethod
    def _add_lcia_locations(lci_db, extensions) -> None:
        """Add the locations of the characterization factors of the methods stored in the
        attached ``extensions`` to ``geomapping``, as for the databases of an extension."""
        from bw2data import geomapping
        from bw2data.backends.extensions import _q, _table
        from bw2data.backends.schema import CharacterizationFactor

        locations = set()
        for extension in extensions:
            if not extension.lcia.get("method"):
                continue
            sql = (
                f"SELECT DISTINCT geocollection, location FROM {_q(extension.schema)}."
                f"{_q(_table(CharacterizationFactor))} WHERE location IS NOT NULL"
            )
            with closing(lci_db.execute_sql(sql)) as cursor:
                for collection, location in cursor.fetchall():
                    locations.add(location if collection is None else (collection, location))
        if locations:
            geomapping.add(locations)

    @staticmethod
    def _start_extension_databases(lci_db, databases) -> None:
        """Give the newly attached extension databases ``databases`` a new local state: their
        locations in ``geomapping``, ``dirty`` (to be processed) unless the extension's file
        holds their processed datapackage, and no search index built in the project, which
        would be for another version of the file."""
        from bw2data import geomapping
        from bw2data.backends import SQLiteBackend
        from bw2data.backends.extensions import has_stored_datapackage
        from bw2data.backends.schema import ExtensionDatabaseState
        from bw2data.search.indices import index_table

        # Extensions have no geomapping: their location names get ids here. Added first; if
        # what follows fails, they stay, as they do when an extension is detached
        locations = set()
        for database in databases:
            locations.update(SQLiteBackend(database).inventory_locations())
        geomapping.add(locations)
        with lci_db.atomic():
            for database in databases:
                # Not dirty: the record in the extension says it isn't
                dirty = None if has_stored_datapackage(database) else True
                ExtensionDatabaseState.replace(name=database, dirty=dirty).execute()
                lci_db.execute_sql(f'DROP TABLE IF EXISTS main."{index_table(database)}"')

    @staticmethod
    def _extension_users(lci_db, extension) -> list[str]:
        """What in the project's own file links to the attached ``extension``: databases with
        exchanges whose input is in it, and methods and normalizations with factors for its
        activities."""
        from bw2data.backends.schema import MethodRecord, NormalizationRecord

        databases = extension.databases
        marks = ", ".join("?" * len(databases))
        result = []
        sql = (
            'SELECT DISTINCT "output_database" FROM main."exchangedataset" '
            f'WHERE "input_database" IN ({marks}) ORDER BY 1'
        )
        with closing(lci_db.execute_sql(sql, databases)) as cursor:
            users = [row[0] for row in cursor.fetchall()]
        if users:
            result.append(
                "these databases of the project have exchanges with inputs in it: "
                + ", ".join(repr(user) for user in users)
            )
        flows = (
            f'SELECT "id" FROM "{extension.schema}"."activitydataset" '
            f'WHERE "database" IN ({marks})'
        )
        for record, table, column, label in (
            (MethodRecord, "characterizationfactor", "method_id", "methods"),
            (NormalizationRecord, "normalizationfactor", "normalization_id", "normalizations"),
        ):
            sql = f'SELECT DISTINCT "{column}" FROM main."{table}" WHERE "flow" IN ({flows})'
            with closing(lci_db.execute_sql(sql, databases)) as cursor:
                ids = [row[0] for row in cursor.fetchall()]
            if ids:
                names = sorted(
                    str(row.name) for row in record.select(record.name).where(record.id.in_(ids))
                )
                result.append(
                    f"these {label} have factors for its flows: "
                    + ", ".join(repr(n) for n in names)
                )
        return result

    def _store_extensions(self, entries: list[dict]) -> None:
        if entries:
            self.dataset.data[self._EXTENSIONS_KEY] = entries
        else:
            self.dataset.data.pop(self._EXTENSIONS_KEY, None)
        self.dataset.save()

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
            project_created.send(self, dataset=self.dataset)
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
        create_dir(self._base_logs_dir / safe_filename(new_name, full=self.dataset.full_hash))
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

        victim_dataset = ProjectDataset.get(ProjectDataset.name == victim)
        ProjectDataset.delete().where(ProjectDataset.name == victim).execute()
        project_deleted.send(self, dataset=victim_dataset)

        if delete_dir:
            dir_path = self._base_data_dir / safe_filename(victim, full=victim_dataset.full_hash)
            assert dir_path.is_dir(), "Can't find project directory"
            for _, substitutable_db in config.sqlite3_databases:
                try:
                    if Path(substitutable_db._filepath).is_relative_to(dir_path):
                        if not substitutable_db.db.is_closed():
                            substitutable_db.db.close()
                except Exception:
                    pass
            shutil.rmtree(dir_path)
            logs_path = self._base_logs_dir / safe_filename(victim, full=victim_dataset.full_hash)
            if logs_path.is_dir():
                shutil.rmtree(logs_path)
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
        registered = {safe_filename(obj.name, full=obj.full_hash) for obj in self}
        bad_directories = [
            self._base_data_dir / dirname
            for dirname in os.listdir(self._base_data_dir)
            if (self._base_data_dir / dirname).is_dir() and dirname not in registered
        ]

        for fp in bad_directories:
            shutil.rmtree(fp)

        bad_log_dirs = [
            self._base_logs_dir / dirname
            for dirname in os.listdir(self._base_logs_dir)
            if (self._base_logs_dir / dirname).is_dir() and dirname not in registered
        ]

        for fp in bad_log_dirs:
            shutil.rmtree(fp)

        return len(bad_directories) + len(bad_log_dirs)

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

    # `old` and `new` share no keys. Since deepdiff 8.0 that diffs as one `values_changed` on
    # `root` unless `threshold_to_diff_deeper=0`, and `_unwrap_diff_dict` in `revisions.py`
    # reads the `dictionary_item_removed` and `dictionary_item_added` keys.
    delta = revisions.Delta(
        delta=deepdiff.Delta(
            deepdiff.DeepDiff(old, new, verbose_level=2, threshold_to_diff_deeper=0)
        ),
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
