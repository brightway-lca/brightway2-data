import json
import os
import shutil
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
        self._warn_extension_problems(problems, previous)

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
        ``problem`` and ``databases``."""
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
        from bw2data.backends.schema import ExtensionDatabaseState
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
        from bw2data import geomapping
        from bw2data.backends import SQLiteBackend
        from bw2data.search.indices import index_table

        before = {spec.name for spec in attached_now}
        new_databases = [
            database
            for extension in lci_db.extensions
            if extension.name not in before
            for database in extension.databases
        ]
        try:
            # Extensions have no geomapping: their location names get ids here. Added first;
            # if what follows fails, they stay, as they do when an extension is detached
            locations = set()
            for database in new_databases:
                locations.update(SQLiteBackend(database).inventory_locations())
            geomapping.add(locations)
            # The project has no processed data for the databases which it now gets, or data
            # of another version of the file: start from a new local state, to be processed
            with lci_db.atomic():
                for database in new_databases:
                    ExtensionDatabaseState.replace(name=database, dirty=True).execute()
                    # A search index built in the project is for another version of the file
                    lci_db.execute_sql(f'DROP TABLE IF EXISTS main."{index_table(database)}"')
        except BaseException:
            # Nothing is attached which wasn't before
            self._store_extensions(stored)
            lci_db.change_path(main_path, extensions=attached_now)
            raise
        self._extension_problems = problems
        self._warn_extension_problems(problems, previous)
        bw2signals.on_extension_attached.send(self, names=[entry["name"] for entry in entries])
        return tuple(e for e in self.extensions if e.name in new_names)

    def export_extension(
        self,
        filepath: Union[str, Path],
        databases: Union[str, Sequence[str]],
        version: Optional[str] = None,
        overwrite: bool = False,
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
        ids, their metadata (``depends`` worked out from the exchanges) and their search
        indexes; nothing else of the project. Only committed changes are exported. Raises
        ``FileExistsError`` if ``filepath`` exists, unless ``overwrite``; see
        ``bw2data.backends.extensions.build_extension``."""
        from bw2data.backends.extensions import build_extension

        lci_db = self._lci_db()
        if lci_db is None:
            raise RuntimeError("No project is open")
        if lci_db.db.in_transaction():
            raise RuntimeError("Can't export inside a transaction: only committed data is read")
        return build_extension(
            self.dir / self._LCI_DATABASE,
            filepath,
            databases,
            version=version,
            attached=lci_db.extensions,
            overwrite=overwrite,
        )

    def detach_extension(self, name: str, force: bool = False) -> None:
        """Remove the read-only LCI extension ``name`` from the current project. The file itself
        is not touched.

        Refused with ``bw2data.errors.ExtensionError`` while another attached extension
        requires it, or while the project has exchanges whose input is in it, or
        characterization or normalization factors for its flows. ``force=True`` detaches
        anyway, with a warning: the extensions which require it are then no longer available,
        and such exchanges and factors point at nothing."""
        from bw2data.backends.schema import ExtensionDatabaseState
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
        previous = dict(self._extension_problems)
        self._store_extensions([entry for entry in stored if entry["name"] != name])
        try:
            self._attach_stored_extensions(previous=previous)
        except BaseException:
            self._store_extensions(stored)
            raise
        signal_databases = tuple(databases) if extension is not None else ()
        if databases and extension is None:
            # Names which the project's own file or another extension uses by now are not
            # this extension's any more
            marks = ", ".join("?" * len(databases))
            sql = f'SELECT "name" FROM main."databaserecord" WHERE "name" IN ({marks})'
            with closing(lci_db.execute_sql(sql, databases)) as cursor:
                used = {row[0] for row in cursor.fetchall()}
            used.update(lci_db.extension_databases())
            databases = tuple(database for database in databases if database not in used)
        if databases:
            from bw2data.backends import SQLiteBackend
            from bw2data.search.indices import index_table

            ExtensionDatabaseState.delete().where(
                ExtensionDatabaseState.name.in_(list(databases))
            ).execute()
            for database in databases:
                # Indexes built in the project for databases whose file had none
                lci_db.execute_sql(f'DROP TABLE IF EXISTS main."{index_table(database)}"')
                # Processed data; geomapping entries stay, as other processed data may use them
                backend = SQLiteBackend(database)
                processed = backend.dirpath_processed() / backend.filename_processed()
                try:
                    processed.unlink(missing_ok=True)
                except OSError as err:
                    # E.g. still open in a calculation, on Windows. The extension is detached
                    warnings.warn(
                        f"Can't remove the processed data of database {database!r} "
                        f"({processed}): {err}",
                        stacklevel=2,
                    )
        bw2signals.on_extension_detached.send(self, name=name, databases=signal_databases)

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
