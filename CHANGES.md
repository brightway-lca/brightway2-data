# `bw2data` Changelog

## Unreleased

### Breaking changes

* BREAKING CHANGE: [#312: Require Python 3.11 or later](https://github.com/brightway-lca/brightway2-data/pull/312)
* BREAKING CHANGE: [#284: Store the database metadata registry (`databases`) in a `DatabaseRecord` table in `lci/databases.db` instead of the per-project `databases.json` file](https://github.com/brightway-lca/brightway2-data/pull/284). This only changes where the per-database metadata (`backend`, `depends`, `dirty`, `modified`, `searchable`, `geocollections`, custom keys, etc.) is stored; `Database` objects and their activities and exchanges are unchanged. As before, changes to `Database(name).metadata` or `databases[name]` are kept in memory until saved; save them with the new `Database(name).metadata.save()` (or `databases[name].save()`), which sends `on_database_metadata_change` on event-sourced projects. `databases.flush()` is deprecated: it still saves all pending changes, but warns and points to `.metadata.save()`. Assigning or deleting a whole entry (`databases[name] = {...}`, `db.metadata = {...}`, `del databases[name]`) is still saved immediately. An existing `databases.json` is moved into SQLite by schema migration 3 (see #325) when the project is opened, and left in place as a backup.
* BREAKING CHANGE: [#313: Store `geomapping` in `Geocollection` and `GeoMappingRecord` tables in `lci/databases.db` instead of `geomapping.pickle`](https://github.com/brightway-lca/brightway2-data/issues/313). Ids are assigned by SQLite, so processes adding locations to the same project at the same time can no longer hand out the same id, and ids of deleted locations are never reused. Keys must be a location string (`"CH"`) or a `(geocollection, feature id)` tuple of two strings (`("world", "CH")`); anything else, including integer feature ids, raises `ValueError`. Geocollections are rows in their own table, created automatically when locations are added, with an `extra` JSON column for metadata; `geomapping.collections()` and `geomapping.locations(collection)` list them. An existing `geomapping.pickle` is migrated on first access, keeping all ids (integer feature ids are converted to strings), and left in place as a backup. `geomapping.flush()` is deprecated and does nothing. `geomapping` is no longer a `PickledDict`: `load()`, `serialize()`, `deserialize()`, `backup()`, `filepath`, `pack()`/`unpack()`, `random()`, `list`, assigning to `geomapping.data` (now read-only) and the `dirpath` constructor argument are removed. `geomapping.delete(keys)` ignores keys which aren't present instead of raising `KeyError` (`del geomapping[key]` still raises). `Activity.save()` raises `ValueError` for an invalid location before anything is written. Once a project is migrated, older `bw2data` versions still read and write `geomapping.pickle`, so don't use one project with old and new versions. A missing or empty location in a CF row (`(flow, amount, None)`) now means `config.global_location`, matching the method's `geocollections` metadata, instead of adding `None` to `geomapping`.
* BREAKING CHANGE: [#325: Store `preferences` in a `preference` table in `lci/databases.db` instead of `preferences.pickle`, and add versioned schema migrations for `lci/databases.db`](https://github.com/brightway-lca/brightway2-data/pull/325). The schema version is stored in `PRAGMA user_version`; pending migration steps (`bw2data.backends.migrations.MIGRATIONS`) run in one transaction when a project is opened. If the file is locked by another process or read-only, the migration is postponed with a warning and tried again the next time the project is opened. Migration 1 moves an existing `preferences.pickle` into SQLite (preferences already stored in SQLite are kept) and leaves the file in place as a backup; once a project is migrated, older `bw2data` versions still read and write the pickle, so don't use one project with old and new versions. Preference keys must be strings: assigning a non-string key raises `TypeError`, and non-string keys in an old pickle are not migrated (with a warning). `preferences` works as before (`flush()` saves changes made inside values), with these differences: reading never writes, so the default `use_cache` is only stored once it is changed; an assignment, deletion or `flush()` writes only the preferences changed in this process, so preferences set by another process in the meantime are no longer overwritten (`preferences.load()` reads them); and an assignment or deletion which can't be saved is not applied in memory either. `preferences` is no longer a `PickledDict`: `serialize()`, `deserialize()`, `backup()`, `filepath`, `pack()`/`unpack()`, `random()`, assigning to `preferences.data` (now read-only; it can still be changed in place, followed by `flush()`) and the `dirpath` constructor argument are removed. `SubstitutableDatabase` takes a new `on_open` callback; if a file can't be opened, `SubstitutableDatabase.change_path` and `projects.set_current` now go back to the previous file and project before raising.

* BREAKING CHANGE: Store the `data` of activities and exchanges as tagged JSON (see `bw2data.codec`, #336) compressed with zstd, instead of as pickles (storage S3, part of #333). `Database.write()` with at least 500 datasets first trains a zstd dictionary on a sample of them, stored in the new `zstddictionary` table of `lci/databases.db`, and compresses the rows with it; the values activities and exchanges share (comments, properties, pedigree matrices, ...) are then stored once, in the dictionary. Smaller writes and single saves use the latest dictionary, or none if there is none yet. A dictionary is only used for values compressed inside a transaction, in which it is read from the file, so that a row can never be stored with a dictionary which another process has replaced and deleted in the meantime; `ActivityDataset.save()` and `ExchangeDataset.save()` therefore run in a transaction (an `IMMEDIATE` one, unless the caller is already in a transaction), and rows written through the ORM outside of a transaction (`Model.update()`, `Model.insert()`) are compressed without a dictionary. Older dictionaries are kept while rows use them, and deleted before `VACUUM` once unused. Existing pickled rows are still read, but only through `codec.safe_pickle_loads`, so a row with a class other than the built-in types, `datetime`, `decimal`, `uuid` and numpy raises `UnsafePickleError` unless the class is listed in `config.trusted_pickle_classes` (for example temporal distributions pickled by bw2data versions before they were stored as JSON strings). Values which can't be stored as tagged JSON raise `TypeError` naming the key, instead of being pickled; numpy scalars are stored as the Python number they hold. Temporal distributions are no longer converted to JSON strings before they are stored: `bw_temporalis` registers its classes with `bw2data.codec` (needs the `bw_temporalis` release after 1.3.1; older releases raise `TypeError` when an exchange with a temporal distribution is saved), and they are read back as objects. JSON strings stored by earlier versions are still restored by the exchange proxy, and revisions still store temporal distributions as JSON strings. Rows already in a file are not converted yet, and older bw2data versions can't read the new rows. Adds `backports.zstd>=1.0.0` as a dependency on Python before 3.14, which has `compression.zstd`. `codec.register()` takes `replace=True`, for a class which is defined again (e.g. in a notebook). `SubstitutableDatabase` takes a new `before_vacuum` callback. `benchmarks/storage.py` compares the new format with pickles.

* BREAKING CHANGE: [#342: The `data` of an exchange no longer stores `input` and `output`](https://github.com/brightway-lca/brightway2-data/pull/342) (storage S4, part of #333); they are only stored once, in the `input_database`, `input_code`, `output_database` and `output_code` columns, which were already canonical. Exchange data in memory is unchanged: `Exchange` proxies, `Database.load()`, revisions and the `data` attribute of an `ExchangeDataset` instance read from the database give both keys as tuples, taken from these columns (`ExchangeDataset.data` is now an `ExchangeDataField`). Code reading the `data` column without a model instance, for example with `.dicts()`, `.tuples()` or raw SQL, gets the data without `input` and `output`; `bw2data.backends.utils.exchange_row_as_dict(row)` puts them back from a `.dicts()` row. Rows written earlier still hold both keys, and the columns win wherever the two differ. If `process()` finds an invalid amount, the error names the exchange with the keys from the columns. Every exchange without other tuples or special values is now plain JSON, which is decoded without the tag walk. `process()` no longer raises `InvalidExchange` for stored data without `input`.

* BREAKING CHANGE: [#348: The search index of a database is stored in `lci/databases.db`, in place of one SQLite file per database in `search/`](https://github.com/brightway-lca/brightway2-data/pull/348). It is a contentless FTS5 table `searchindex_<md5 of the database name>` with the node id as `rowid`: the text is not stored a second time, and the index changes in the same transaction as the node, so a save which fails leaves it as it was. Schema migration 7 builds the indexes of all searchable databases from their nodes when a project is opened, and deletes the old index files. `Database.search` is unchanged; `Searcher.search(..., proxy=False)` results also have the node `id`. A database without a `searchable` flag (registered, not written) is searchable: the nodes saved into it are indexed, as before, and the migration indexes such a database if its old index file has rows. `Database.write(..., searchable=False)` with data now sets the flag to `False` and drops the index. `IndexManager` and `Searcher` take the database name (the `filename` of a registered database is still accepted). Only stored nodes can be indexed: `IndexManager.add_dataset` raises `UnknownObject` for anything else, the indexed text is always read from the stored node (`update_dataset` no longer indexes the values it is given), and `delete_dataset` must be called before the row is changed; new `IndexManager.add(ids)`, `remove(ids)`, `add_all()` and `exists()`, which skip nodes which are already (or not) in the index. The database name is no longer indexed (the code still is). `bw2data.search.schema` is removed. If nodes were changed without the index (e.g. with `ActivityDataset` directly), `Database.make_searchable(reset=True)` builds it again. Fixed along the way: changing the `code` or `database` of a node in a database without a `searchable` flag left the index out of date, and `merge_databases` did not index the nodes it moved.
* BREAKING CHANGE: [#344: Schema migration 6 converts the pickled `data` of existing activities and exchanges to compressed tagged JSON when a project is opened](https://github.com/brightway-lca/brightway2-data/pull/344) (storage S5, part of #333). **Older bw2data versions can't read a project afterwards**, and no backup is kept, as it would be as large as the file the migration makes smaller. Pickles are read with `codec.safe_pickle_loads`; exchanges lose `input` and `output` (as in #342), and temporal distributions stored as JSON strings become the tagged values which `bw_temporalis` registers (without importing it). The rows are compressed with the active zstd dictionary; if there is none, and at least 500 activities to convert, one is trained on a sample of up to 5,000 rows and made the active one. The file is vacuumed after the commit, which frees the space of the pickles and switches it to 16 KB pages (#334); with `config.vacuum = False` it isn't, and a failed `VACUUM` only warns. The migration is postponed with a warning, converting nothing, if there is less free disk space than 1.1 times the size of the pages which hold pickled rows (at most the file; the rollback journal needs it), or if any row can't be converted (a class which `config.trusted_pickle_classes` doesn't allow, an unreadable pickle, or a value which can't be stored as tagged JSON). All rows are checked before the first one is changed, so that a project with such a row is only read each time it is opened; the warning lists each different error with the number of rows and the first three of them (activities by key, exchanges by id and keys), and pickled rows can still be read until then. `benchmarks/storage.py` times the migration: 20,000 activities and 500,000 exchanges take 14 s including the check and `VACUUM`, and the file goes from 446 to 83 MB. Migration steps can now be postponed on their own (`MigrationPostponed`; earlier steps are still committed, as each step runs in a savepoint) and can return a function to run after the commit.

### Changes

* [#338: Switching projects no longer runs a full garbage collection for each database file](https://github.com/brightway-lca/brightway2-data/pull/338), which took most of the time of `projects.set_current`. The collections were added in #259 to release SQLite file handles on Windows; every cursor has been closed explicitly since #308.
* [#334: Use 16 KB pages for `lci/databases.db`](https://github.com/brightway-lca/brightway2-data/pull/334), which makes ecoinvent 3.13 cutoff about 11% smaller (1,766 MB to 1,567 MB). New files get the new page size; existing files are converted the next time they are vacuumed, except files which another tool switched to WAL journal mode, which keep their page size. Small files get bigger, because every table and index takes at least one page: an empty project's file grows from 48 KB to 192 KB. Both page sizes can be read by old and new bw2data versions. `Database.write()` no longer raises when the `VACUUM` after a successful write fails (for example because another process holds a lock); the data is already stored, so it warns instead.
* [#336: Add `bw2data.codec`, a JSON encoding that keeps types (tuples, sets, datetimes, non-string keys, NaN, large integers, numpy arrays, ...) using tagged values, and `codec.safe_pickle_loads`, which only loads the classes bw2data stores](https://github.com/brightway-lca/brightway2-data/pull/336). Other classes raise `UnsafePickleError` unless listed in the new `config.trusted_pickle_classes`. Packages can register their own types with `codec.register()` or a `bw2data.codec` entry point. A value which can't be stored exactly raises `TypeError` naming where it is; this includes values nested more than 254 JSON levels deep (each tagged value takes two). Nothing uses the codec yet; it prepares replacing the pickled columns (#333). Adds `orjson>=3.8.0` as a dependency.
* [#327: New `geomapping` and `Geocollection` ids are snowflake ids](https://github.com/brightway-lca/brightway2-data/pull/327), like the ids of activities and exchanges, instead of coming from an SQLite `AUTOINCREMENT` sequence. Ids are still never reused, and no longer collide between the files of different projects. Existing ids are kept, so processed datapackages stay valid. Schema migration 2 rebuilds `geocollection` and `geomappingrecord` without `AUTOINCREMENT`, keeping all rows and ids. Code which assumed small, dense geomapping ids must handle 64-bit integers.
* [#328: Move `databases.json` into SQLite with schema migration 3](https://github.com/brightway-lca/brightway2-data/pull/328), instead of on first access of `databases`. The file is read only once per project, so deleting all databases no longer brings back the ones in the backup file. Databases which are already in the `databaserecord` table are kept as they are (as after a postponed migration); the others are added. Values of the wrong type (e.g. a list as `version`) are converted to something which can be stored, and entries which can't be stored at all are skipped with a warning, so that such a file no longer blocks opening the project. `DatabaseRecord` sets its table name explicitly.
* BREAKING CHANGE: [#329: Store the LCIA metadata registries `methods`, `weightings` and `normalizations` in `method`, `weighting` and `normalization` tables in `lci/databases.db` instead of `methods.json`, `weightings.json` and `normalizations.json`](https://github.com/brightway-lca/brightway2-data/pull/329). Each row has a snowflake id and the name tuple as its unique natural key; `abbreviation` (and `num_cfs` and `geocollections` for methods) have their own columns, and all other keys, including `unit` and `description`, are stored as JSON in `extra`. The registries work like `databases`: `methods[name]` returns a dict-like proxy (also returned by `Method(name).metadata`), whose changes are saved with `methods[name].save()` or `Method(name).metadata.save()`; assigning or deleting a whole entry is saved immediately, and assigning keeps the row's id. `methods.flush()` (and the same for `weightings` and `normalizations`) is deprecated: it still saves all pending changes, but warns. Names must be non-empty tuples of values which can be stored as JSON (normally strings); a list is accepted and treated as the equal tuple, and any other name raises `TypeError` on assignment (string names were never read back correctly from the JSON files) and is simply not in the registry when looked up. Replacing the metadata of a registered object (`Method(name).metadata = {...}`, `methods[name] = {...}`) keeps its `abbreviation` unless a new one is given; before, this removed it and broke `write()` and `process()`. The registries are no longer `CompoundJSONDict`s: `load()`, `serialize()`, `deserialize()`, `backup()`, `filepath`, `pack()`/`unpack()`, assigning to `.data` (now read-only) and the `dirpath` constructor argument are removed. Schema migration 4 moves the JSON files into SQLite when the project is opened, and leaves them in place as backups; objects which are already in a table are kept as they are (as after a postponed migration), and entries which can't be stored (e.g. a name with a list in it) are skipped with a warning; once a project is migrated, older `bw2data` versions still read and write the JSON files, so don't use one project with old and new versions. `DatabaseRecordProxy` is renamed to `RecordProxy` (the old name still works).
* BREAKING CHANGE: [#330: Store characterization factors, normalization factors and weightings in `characterizationfactor`, `normalizationfactor` and `weightingfactor` tables in `lci/databases.db` instead of `intermediate/<abbreviation>.pickle`](https://github.com/brightway-lca/brightway2-data/pull/330). There is one row per factor, referring to its method, normalization or weighting by id; deleting the method (`del methods[name]`) now also deletes its factors. Flows are stored as activity ids; locations are stored by name (`location`, plus `geocollection` for a `(geocollection, location)` tuple), never as geomapping ids. Values are stored as `amount` (REAL) plus the other keys of an uncertainty dictionary as JSON, so `load()` returns integer amounts as floats and NumPy scalars as Python numbers, and a CF row written with a `None` or empty location comes back without it, as `(flow, amount)`. `write()` replaces all factors and saves the metadata (e.g. `num_cfs`) in one transaction, and raises `ValueError` without writing anything for a row it can't store: a location which isn't a string or a `(geocollection, location)` pair of strings, or an amount which isn't a number (`True` and `False` included) or is NaN, which SQLite can't store (infinite amounts are fine). `Normalization.write()` stores flows as activity ids, like `Method.write()`, so `Normalization(name).load()` returns ids instead of `(database, code)` keys. `load()` raises `MissingIntermediateData` for an object which is registered but was never written. Other `ImpactAssessmentDataStore` subclasses (e.g. in `bw2regional`) still use pickles. `replace_cfs(old_key, new_key)` is one SQL update; it accepts keys or ids, returns the changed methods and processes them (it raised `TypeError` or changed nothing before, as factors are stored with ids); if some of them can't be processed, the others still are, and an `ExceptionGroup` naming the changed and the unprocessed methods is raised at the end. Schema migration 5 moves existing pickles into SQLite when the project is opened, converting `(database, code)` keys and NumPy integers to ids and integer feature ids in `(geocollection, feature id)` locations to strings (as the `geomapping` migration does), and keeps the files as backups; objects whose pickle can't be read or has rows which can't be stored (e.g. a flow which no longer exists, or a NaN amount) are skipped with a warning, and can't be loaded until written again.
* [#337: A project whose `lci/databases.db` is read-only or locked by another process can be opened again after upgrading `bw2data`](https://github.com/brightway-lca/brightway2-data/pull/337). Opening a project creates the tables added by newer versions of `bw2data`, which failed with `attempt to write a readonly database` (or `database is locked`). For `lci/databases.db` (`SubstitutableDatabase` with the new `postpone_tables=True`; other files fail as before), this is now postponed. A read-only file gets a warning naming the missing tables, which are created the next time the file is opened and can be written. For a locked file, the tables are created and the schema migrations run before the first statement for which the file is no longer locked, without reopening the project; there is a warning only if tables are known to be missing. Until then, data stored in the missing tables is not available. Tables are created in one transaction, so a file never ends up with only some of them. A file which is locked even for reading no longer makes the schema migrations fail: they are postponed with a warning, like for a write lock. Whether an error means "locked" or "read-only" is now decided from the SQLite result code, which also covers a read-only project directory (`unable to open database file`) and no longer matches `database table is locked`. These warnings are reported at the calling line outside `bw2data`.
* Read-only LCI extensions: a user guide page (`docs/content/extensions.md`) on building, attaching, updating and detaching extensions, including building an inventory on a separate flow list. `benchmarks/extensions.py` (not run in CI) times a synthetic database in an extension against the same database in the project's own file.
* Read-only LCI extensions: extension files store the processed datapackage of each of their databases (#352), so that a project which attaches a file doesn't have to process its databases. `projects.export_extension` processes the exported databases first if they are `dirty` or have no processed datapackage, but only once the export has passed the checks which can refuse it (new `bw2data.backends.extensions.check_export`), and stores the `bw_processing` zip files as they are in the new optional table `extension_datapackage` (`database`, `content`). `Database(name).datapackage()` of a database in an attached extension reads it from there (into memory, with `ZipFileSystem(fo=io.BytesIO(...))`); nothing is copied into the project. `filepath_processed()` writes it to the project's processed folder, for code which needs a file, and replaces a file there which is not the stored one (e.g. left by a deleted database with the same name). The databases of an attached extension whose file has their datapackages are not marked `dirty`. They are processed in the project as before if the file has no datapackage for them (files built earlier), or if a requirement of the extension is met by a database in the project's own file (`allow_main_requirements`), whose activity ids differ from those the arrays were built with. Which of the two applies is decided on each use, as it changes with what is attached: a database which can't use its stored datapackage (any more) is also processed if the project has no processed file for it, or only a copy of the stored one. The location ids in the stored `inv_geomapping_matrix` are those of the project which built the file; they are not translated (only regionalized calculations read that array; #331 will name them). `build_extension` takes `datapackages={database: path}`. New `bw2data.backends.extensions.has_stored_datapackage(database)` and `stored_datapackage(database, usable_only=True)`; `AttachedExtension.datapackages` lists the databases whose datapackage a file holds.
* Read-only LCI extensions: `projects.update_extension(name, path=None, force=False)` takes up a new version of an extension: the file which is at its path now, or the file `path`. It keeps the extension's name and options and the project's links into it, resets the project's local state of its databases as attaching does (processed data and search indexes built in the project removed, `dirty` set, locations added to `geomapping`; databases which the new version lacks are forgotten) and sends `on_extension_detached` and `on_extension_attached`. For the same version at another path (a moved file), only the path changes. It is refused, unless `force=True`, while another attached extension requires the old version, while databases of the project have exchanges with inputs which are not in the new version, or while methods or normalizations have factors for flows which are in none of the files any more; if the old version can't be read (the extension was not available), the flows of all factors are checked, the message says so, and flows in the files of other extensions which are not available don't count as missing. An update which fails leaves the local state of the old version as it was. For an extension which was not available, database names which the project or another extension uses by now are left alone, and `on_extension_detached` is sent without databases, as by `detach_extension`. It also brings back an extension which was not available because its file was replaced or moved; the warning for a replaced file now points to it. A file should only be replaced while no project uses it; what happens to a project which has it open is undefined. New `bw2data.backends.extensions.extension_id(path)` and `missing_link_targets()`.
* [#332: Read-only LCI extensions, part 2](https://github.com/brightway-lca/brightway2-data/pull/332) (no behaviour change yet): `bw2data.backends.extensions.overlay_sql()` builds the SQL that overlays read-only SQLite files attached to `lci/databases.db`, using TEMP views over `activitydataset`, `exchangedataset` and `databaserecord`, with `INSTEAD OF` triggers that send writes to the project's own file and refuse changes to extension data. `ownership_check_sql()` builds the query that checks that each database name belongs to exactly one file. A new, empty `extensiondatabasestate` table holds the project-local state (`dirty`, `searchable`, `modified`, `processed`) of extension databases.
* Read-only LCI extensions: a database stored in an attached extension is searched with the search index in the extension's file (`IndexManager.schema` is the extension's schema, and `IndexManager.read_only` is true); nothing is built in the project. `make_unsearchable` and `make_searchable` change the project-local `searchable` flag, which decides whether that index is used. An index of such a database in the project's own file is used first: `make_searchable` builds it if the file has no index, or one with other columns or options than this version creates, and `make_searchable(reset=True)` always does, to replace an index in the file which is out of date. It is dropped by `make_unsearchable`, and when the extension is attached or detached (also when it is detached while not available). Single nodes can't be added to or removed from the index of a database in an extension (`ReadOnlyExtensionError`). Fixed: applying a revision which makes a database unsearchable raised `TypeError`.
* Read-only LCI extensions, part 6: `projects.export_extension(filepath, databases, version=None, overwrite=False)` writes databases of the current project into a new extension file, which any project can attach, and returns its id (a new UUID for every file). The file has the schema of the project's file and its schema version, the databases' activities (with their ids), exchanges (with new ids), metadata and search indexes; the project-local tables are empty. No zstd dictionary of the project goes into the file, as a dictionary holds pieces of all the data it was trained on: values which use one are compressed again, with a new dictionary trained on the exported values alone. `depends` is worked out from the exchanges, `processed` is removed and `dirty` cleared. `extension_info` holds the id, the `version` label, the fingerprint of each database, the bw2data version and when it was built. A database may link to the other exported databases and to databases in the project's attached extensions; the file then requires those, in exactly the attached version (`extension_requires`). Links to any other database are refused with `ExtensionError`, as are databases stored in an extension, IO table databases, exports inside a transaction, and a `filepath` which is the project's own file or the file of an attached extension (also with `overwrite`). The file is built next to `filepath` and only moved there once it passes the checks made on attach. Attaching an extension adds the locations of its databases to `geomapping` (processing such a database adds any which are missing, e.g. in a copied project); detaching it removes their processed datapackages. New `SQLiteBackend.inventory_locations()`.
* Read-only LCI extensions, part 5: changes to the data of an attached extension are refused in Python, before anything is changed, with `bw2data.errors.ReadOnlyExtensionError`. It is an `ExtensionError`, and also a `peewee.IntegrityError`, which is what SQLite's own refusal raised before; refusals by the triggers now raise it too. Refused for a database stored in an extension: `Database.write`, `delete`, `rename`, `register`, `new_node`, `new_activity` and `delete_duplicate_exchanges`; `del databases[name]` (before calculation setups are changed); `databases[name] = ...`; saving its metadata with changes to other keys than `dirty`, `searchable`, `modified` and `processed` (the unsaved changes are discarded); saving, deleting or changing the code or database of its nodes; saving or deleting its edges. A database name used by an extension can't be given to a database of the project (`rename`, `copy`, moving a node). Allowed: reading, `Database(name).copy(...)` and `Activity.copy(database=...)` into the project, edges from the project's databases into extensions, `process()` and search. `process()` stores `processed` in the project (`ExtensionDatabaseState.processed`, shown in the record's `extra`) and leaves the extension's `depends` as it is. `Database(name).is_extension` and `Database(name).extension` tell whether and where a database is stored in an extension. Replaying database metadata from revisions (`databases.data = ...`) leaves the records of extension databases alone. Extension databases are not processed when they are attached, and the project has no search index for them until `make_searchable(reset=True)`; both follow with building extensions. `IOTableBackend.write_exchanges` is refused as well. `node.upstream().delete()` is checked against the databases which store the edges, so the project's edges into a node of an extension can be deleted. The databases of a newly attached extension are marked `dirty` in the project, so they are processed when first needed, also when a file is replaced by another version; `projects.detach_extension` removes the project's local state of its databases.
* Read-only LCI extensions, part 4: `projects.attach_extension(path, name=None, immutable=False, allow_main_requirements=False)` attaches one extension file or a list of them to the current project (for a list, `name` is a list of names; the default name is the file name without its suffix, with `_2`, `_3`, ... added if the project already has an extension with that name), `projects.detach_extension(name, force=False)` removes one, and `projects.extensions` lists them with their `status` (`available`, `missing file`, `missing requirement` or `invalid`), `problem` and `databases`. The extensions are stored with the project in `projects.db` (name, absolute path, extension id and options), so they are attached again whenever the project is opened, including when `bw2data` is imported, and `projects.copy_project` copies them; `projects.delete_project(delete_dir=True)` never touches the extension files. A list is stored such that each file comes after the ones it requires; files which require each other in a cycle are refused. When a project is opened, an extension which can't be attached (the file is missing, is no longer the extension which was attached, or needs an extension which is not available) is left out with a warning, together with the extensions which need it, and the project still opens; if the remaining files don't fit together or to the project's own databases, or can't be read, none is attached. The LCI file of a project with extensions is opened once per project switch, with them. `attach_extension` changes nothing if it raises; stored extensions which were not available come back with it if they can, and stay unavailable if they no longer fit. Warnings about unavailable extensions are given when the project is opened, and on an attach or detach only for extensions whose status changes. Detaching is refused while another attached extension requires the extension, while databases of the project have exchanges with inputs in it, or while methods or normalizations of the project have factors for its flows; `force=True` detaches anyway, with a warning. Attaching and detaching keep unsaved metadata changes of the project's own databases. New signals `on_extension_attached` and `on_extension_detached`. `bw2test` and `BW2DataTest` detach extensions when a test ends.
* Read-only LCI extensions, part 3: `SubstitutableDatabase(extensions=...)` and `SubstitutableDatabase.change_path(filepath, extensions=...)` attach read-only extension files (`bw2data.backends.extensions.ExtensionSpec(name, path, immutable=False, allow_main_requirements=False)`) to every connection of `lci/databases.db`, after the schema migrations have run on the project's own file. While extensions are attached, `ActivityDataset`, `ExchangeDataset` and `DatabaseRecord` point at the overlay views, so databases in the extensions are read through the normal API, and edges can link from the project's databases into them; writes go to the project's file only. There is no project-level API yet (`projects.attach_extension` follows), and a database stored in an extension can't be processed yet, so it can't be used in a calculation: `process()` saves `processed` and `depends`, which are read-only for an extension's record (this and the early refusals of other changes follow with the Python-level guards). Before attaching, `check_extensions()` refuses, with `ExtensionError`, a file which is missing, is not an extension (no `extension_info` table with an `id`), or has a schema version older than `MIN_EXTENSION_SCHEMA_VERSION` (6) or newer than this bw2data; the same extension twice; a database name used in two files; an activity or exchange id used in two files; and two different zstd dictionaries with the same id. It raises `ExtensionDependencyError` for a requirement (`extension_requires`) which no attached extension meets with the exact fingerprint (a hash of the `(code, type)` pairs of a database's activities, `database_fingerprint()`), unless `allow_main_requirements` is set and the project has the database with that fingerprint. `sqlite3_lci_db.extensions` and `sqlite3_lci_db.extension_databases()` list what is attached. Values in an extension are decompressed with the zstd dictionaries of their own file. If an extension can't be attached, `change_path` goes back to the previous file and its extensions. Saving database metadata no longer relies on the number of changed rows, which is always 0 for a write through the views. `Database.write()` now gives exchanges snowflake ids, like activities, instead of letting SQLite number them, which made the exchange ids of different files collide. New ids come from `bw2data.snowflake_ids.new_snowflake_id()`, which tries again when the generator returns `None` (more than 4096 ids in one millisecond, or the clock was set back) instead of storing a row without a snowflake id.

## 4.8 (2026-10-19)

### Breaking changes

* BREAKING CHANGE: [#277: Pass the sender as the first positional argument in all blinker `signal.send()` calls](https://github.com/brightway-lca/brightway2-data/pull/277). Receivers must accept the sender as their first positional argument. `project_changed` and `project_created` now send the `ProjectManager` as the sender and the `ProjectDataset` as the `dataset=` keyword argument, instead of sending the `ProjectDataset` as the sender. Receivers written for the old call raise `TypeError` in `projects.set_current()`; if you use `multifunctional`, upgrade to 1.1.2 or later ([#288](https://github.com/brightway-lca/brightway2-data/issues/288)).
* BREAKING CHANGE: [#299: `signaleddataset_on_save` and `on_database_metadata_change` are only sent when the project is event sourced](https://github.com/brightway-lca/brightway2-data/pull/299). These signals exist to generate revisions, which are discarded on unsourced projects, so `SignaledDataset.save` now skips the query for the previous version and `SerializedDict.flush` skips re-reading the metadata file when `projects.dataset.is_sourced` is `False`. Code that connected to either signal to observe changes in unsourced projects will no longer be called.

### Changes

* [#272: `as_uncertainty_dict` sets `loc` to `amount` for undefined and no-uncertainty types when `loc` is missing or NaN](https://github.com/brightway-lca/brightway2-data/pull/272)
* [#273: Accept a `MethodConfig` object as `method_config` in `get_multilca_data_objs`](https://github.com/brightway-lca/brightway2-data/pull/273)
* [#274: Fix log directory handling for full-hash projects in `purge_deleted_directories`, `delete_project` and `copy_project`, which caused `FileNotFoundError` when logging](https://github.com/brightway-lca/brightway2-data/pull/274)
* [#275: Restore the `filter` and `mask` arguments of `Database.search()`](https://github.com/brightway-lca/brightway2-data/pull/275)
* [#276: Raise a helpful `ValueError` when a method, weighting or normalization is missing in `prepare_lca_inputs`](https://github.com/brightway-lca/brightway2-data/pull/276)
* [#278: `Activity.save()` warns and sets the default node type when `type` is not given](https://github.com/brightway-lca/brightway2-data/pull/278)
* [#279: Skip coverage on Windows and macOS to speed up CI](https://github.com/brightway-lca/brightway2-data/pull/279)
* [#280: Add `project_deleted` signal](https://github.com/brightway-lca/brightway2-data/pull/280)
* [#282: Fix `KeyError` when deleting a newly saved activity that is used in calculation setups](https://github.com/brightway-lca/brightway2-data/pull/282)
* [#283: `Database.copy()` copies database parameters, activity parameters and parameterized exchanges](https://github.com/brightway-lca/brightway2-data/pull/283)
* [#285: Fix logging f-string formatting in `proxies.py`](https://github.com/brightway-lca/brightway2-data/pull/285)
* [#286: Update links in the README](https://github.com/brightway-lca/brightway2-data/pull/286)
* [#287: Add Code of Conduct link to `CONTRIBUTING.md`](https://github.com/brightway-lca/brightway2-data/pull/287)
* [#289: Make the maximum parameter formula length configurable via `config.max_formula_length` (default 50,000 characters)](https://github.com/brightway-lca/brightway2-data/issues/289)
* [#291: Check the installed `bw2io` version from package metadata before the automatic `migrations` filename update imports it, and raise `IncompatibleBW2IOVersion` with upgrade instructions for `bw2io<0.9`](https://github.com/brightway-lca/brightway2-data/issues/291)
* [#293: Warn when temporal distribution amounts do not sum to 1](https://github.com/brightway-lca/brightway2-data/pull/293)
* [#295: Require `deepdiff>=8.6.2`, keeping the existing parameter-rename revision format](https://github.com/brightway-lca/brightway2-data/pull/295)
* [#296: Store `bw_temporalis` temporal distributions as JSON on every write path and restore them on load, independent of import order](https://github.com/brightway-lca/brightway2-data/issues/296)
* [#298: Only `VACUUM` the LCI SQLite file when there is space to reclaim; add `config.vacuum` policy](https://github.com/brightway-lca/brightway2-data/pull/298)
* [#301: Flag reference exchanges in processed technosphere datapackages](https://github.com/brightway-lca/brightway2-data/pull/301)

### Dependency changes

* `bw_processing>=1.6` (was `>=0.9.5`), for [#301](https://github.com/brightway-lca/brightway2-data/pull/301)
* `deepdiff>=8.6.2` (was `~=7.0.1`), for [#295](https://github.com/brightway-lca/brightway2-data/pull/295)
* New dependency: `packaging`, for [#291](https://github.com/brightway-lca/brightway2-data/issues/291)

## 4.7 (2026-05-13)

* [#265: Write `database_dependencies` to datapackage metadata](https://github.com/brightway-lca/brightway2-data/pull/265)
* [#266: Add memoization cache to `get_id()` with signal-based invalidation](https://github.com/brightway-lca/brightway2-data/pull/266)
* [#268: Use raw SQLite `executemany` for bulk database writes](https://github.com/brightway-lca/brightway2-data/pull/268)
* [#270: Fix unclosed sqlite3 connections in `updates.py`](https://github.com/brightway-lca/brightway2-data/pull/270)

## 4.6.2 (2026-04-15)

* Version bump; 4.6.1 was not published to PyPI

## 4.6.1 (2026-04-15)

* [#259: Fix Windows file handle leaks in `delete_project` and `change_path`](https://github.com/brightway-lca/brightway2-data/pull/259)
* [#260: Reduce CI matrix to Python 3.9, 3.11, and 3.14](https://github.com/brightway-lca/brightway2-data/pull/260)
* [#261: Speed up Windows CI: use uv, fix temp dir accumulation and file handle leaks](https://github.com/brightway-lca/brightway2-data/pull/261)

## 4.6 (2026-03-04)

* [#258: Compatibility with peewee>=4.0.1](https://github.com/brightway-lca/brightway2-data/pull/258)

## 4.5.4 (2025-12-05)

* Restore Python 3.9 compatibility

## 4.5.3 (2025-11-12)

* Fixed bug where copying would move over original edges

## 4.5.2 (2025-11-11)

* Add `create_aggregated_process` and `copy_activities` functions
* [#252: Production edges have stupid labels](https://github.com/brightway-lca/brightway2-data/issues/252)
* [#251: Deleting project should give log message that data is still available](https://github.com/brightway-lca/brightway2-data/issues/251)

## 4.5.1 (2025-10-13)

* [#250: Serialize `bw_temporalis` `TemporalDistribution` objects to JSON when saving to database - and restore when loading](https://github.com/brightway-lca/brightway2-data/issues/250)
* [#245: Github tests fail on Windows/Python 3.13](https://github.com/brightway-lca/brightway2-data/issues/245)

## 4.5 (2025-06-23)

* [PR #243: Remove pin to Numpy `<2`](https://github.com/brightway-lca/brightway2-data/pull/243)

## 4.4.4 (2025-04-29)

* [PR #238: Add `signal` parameter to write method in `IOTableBackend`](https://github.com/brightway-lca/brightway2-data/pull/238)


## 4.4.3 (2025-03-21)

* [PR #235: Properly delete search database](https://github.com/brightway-lca/brightway2-data/pull/235)

## 4.4.2 (2025-01-22)

* [PR #233: Remove `"`` from search to avoid SQL escaping errors](https://github.com/brightway-lca/brightway2-data/pull/233)
* [PR #232: Fix type annotations in Python 3.9](https://github.com/brightway-lca/brightway2-data/pull/232)
* [PR #231: Escape search strings properly for SQLite `FTS5` format](https://github.com/brightway-lca/brightway2-data/pull/231)
* [PR #230: Fix (some) type annotations](https://github.com/brightway-lca/brightway2-data/pull/230)

## 4.4 (2025-01-15)

* [PR #226: Rebase revisions when necessary](https://github.com/brightway-lca/brightway2-data/pull/226)
* [Fix #227: Setting impact category `num_cfs` not saved to disk](https://github.com/brightway-lca/brightway2-data/issues/227)
* [PR #213: Remove parentheses and curly braces from FTS5 search queries](https://github.com/brightway-lca/brightway2-data/pull/213)

## 4.3 (2024-12-03)

* BREAKING CHANGE: Removed looking up node attributes in reference product edges using `node['foo']` (i.e. `node.__getitem__()`). The code searching for reference product edges was incorrect - when it was correct to search for both functional inputs and outputs, there was a severe performance degradation as all edges needed to be loaded and examined.

## 4.2.1 (2024-12-03)

* Make `__version__` a tuple again to avoid breaking downstream libraries

## 4.2 (2024-12-03)

* Don't try to look up attributes in reference product edge if node type is not "process"
* Allow reference product exchange to be a functional input

## 4.1 (2024-11-29)

* Delete upstream edges on node deletion

# 4.0 (2024-11-25)

* Switch all ID generation to client side instead of database side, and use [snowflake ids](https://en.wikipedia.org/wiki/Snowflake_ID) with a starting epoch of 2024-01-01.
* Add optional ability to serialize data state changes via [DeepDiff](https://pypi.org/project/deepdiff/). This implementation is well tested but is not recommended for casual use as it doesn't have any built-in conflict resolution or other functionality which would be needed to a production-level distributed data generation service using Brightway.
* Add signals based on [blinker](https://pypi.org/project/blinker/).

## 4.0.DEV60 (2024-11-07)

* Add possibility for location string normalization before geomapping array
* Pin previous version of `pint` to skip frozen dataclass error

## 4.0.DEV59 (2024-10-14)

* Add error handling in updating migrations files

## 4.0.DEV58 (2024-10-10)

* [#196](https://github.com/brightway-lca/brightway2-data/issues/196): Fix copying over database metadata when copying database

## 4.0.DEV57 (2024-10-03)

* [#193](https://github.com/brightway-lca/brightway2-data/pull/193): Set node types when writing a `Database`

This is a big change and could break some code:

* It changes data when calling `.write()` as it guarantees that `type` is always present
* Many datasets will have their type label changed from `process` to `processwithreferenceproduct` (e.g. all ecoinvent). Code with uses hard-coded "process" values will break. See `bw2data.configuration` for more details on labels you should expect.
* Only nodes with type `processwithreferenceproduct` will be checked for implicit production. This shouldn't break anything as we change `process` to `processwithreferenceproduct` if implicit production was going to be added.

* [#194](https://github.com/brightway-lca/brightway2-data/pull/194): Replace `print` with logging and add optional `structlog`
* Fix `calculation_setups` getting out of sync on node and database deletion

## 4.0.DEV56 (2024-09-24)

* [#192](https://github.com/brightway-lca/brightway2-data/issues/192): Providing `Node` objects to `prepare_lca_inputs` raises an error

## 4.0.DEV55 (2024-09-20)

* [#190](https://github.com/brightway-lca/brightway2-data/issues/190): `BW2Exception` should inherit `Exception` (instead of `BaseException`)
* [#187](https://github.com/brightway-lca/brightway2-data/issues/187): `Except: pass` in databases metastore
* [#178](https://github.com/brightway-lca/brightway2-data/issues/178): Use of `pydantic_settings` in `configuration.py` allows for environment variables

## 4.0.DEV54 (2024-09-14)

* Allow `get_node(key=(<some tuple))`
* Raise `UnknownObject` when writing LCIA methods referring to non-existent flows
* Allow passing `Node` objects to LCIA method `.write()`
* Allow iterating over LCIA methods

## 4.0.DEV53 (2024-09-12)

* Add additional labels

## 4.0.DEV52 (2024-09-02)

* Remove duplicate attr setting in `new_node`

## 4.0.DEV51 (2024-08-21)

* [PR #179: Make `rapidfuzz` optional on `sys_platform=emscripten`](https://github.com/brightway-lca/brightway2-data/pull/179).

## 4.0.DEV50 (2024-08-17)

* Undo `isort` reordering of `__init__.py` which caused circular import errors

## 4.0.DEV49 (2024-08-16)

* Switch from FTS4 to FTS5 for better availability across all OSes and publication platforms.
* Deprecated and removed `filter` and `mask` functionality from `.search`. Use list comprehensions for more complex search queries.

## 4.0.DEV48 (2024-08-16)

* Nobody saw nothing, and if you value your stuffed animals, it will stay that way.

## 4.0.DEV47 (2024-08-09)

* [PR #177: Switch from `whoosh` to SQLite FTS search index](https://github.com/brightway-lca/brightway2-data/pull/177).

## 4.0.DEV46 (2024-07-23)

* [PR #176: Fix un-hashable error](https://github.com/brightway-lca/brightway2-data/pull/176).
* Import multifunctional if available

## 4.0.DEV45 (2024-07-06)

* Use peewee instead of raw SQL when processing technosphere and biosphere exchanges, and properly exclude non-process nodes (i.e. those not included in `labels.process_node_types`) from processed arrays.
* Fix a bug with upstream exchanges introduced in `4.0.DEV44`

## 4.0.DEV44 (2024-07-04)

* [PR #175: add `check_typos` to `IOTableBackend` write function](https://github.com/brightway-lca/brightway2-data/pull/175).
* Allow passing custom `Exchanges` class in `Activity`

## 4.0.DEV43 (2024-06-18)

* Add biosphere node default labels
* Add `labels` into `__init__` namespace

## 4.0.DEV42 (2024-06-08)

* Fix some packaging bugs
* Fix some missing imports

## 4.0.DEV41 (2024-06-07)

* Update packaging
* Migrate from `pyfilesystem2` to `fsspec`

## 4.0.DEV40 (2024-06-05)

* Add `get_multilca_data_objs` function

## 4.0.DEV39 (2024-05-29)

* [PR #170: Make string labels for matrix construction and typo detection user-configurable](https://github.com/brightway-lca/brightway2-data/pull/170). Instead of fixed strings the labels which determine which values are used in matrices, which matrices they are used in, and what numeric sign they have is now completely configurable.
* Breaking change: Removed `include_substitution` from `Node.technosphere` exchanges iterator. Substitution is production, and cannot logically be considered an input.
* Breaking change: `bw2data.utils.TYPE_DICTIONARY` was removed. This hasn't been used in years.
* Make `code` argument to `Database.new_node` optional
* Fix [#139: Raise an warning when saving new nodes with type `technosphere` or `biosphere`](https://github.com/brightway-lca/brightway2-data/issues/139)

## 4.0.DEV38 (2024-05-14)

* Use [rapidfuzz](https://pypi.org/project/rapidfuzz/) library for faster typo checks

## 4.0.DEV37 (2024-05-12)

* Add ability to skip typo checks for speed boost of large, known good, imports

## 4.0.DEV36 (2024-05-11)

* [PR #160: Fix a type error in ProjectManager's `report()` function](https://github.com/brightway-lca/brightway2-data/pull/160)
* [PR #146: Fix pipeline](https://github.com/brightway-lca/brightway2-data/pull/164)
* [PR #143: Fix iterator over exchanges](https://github.com/brightway-lca/brightway2-data/pull/163)
* Allow custom datapackage processing by refactoring lookup dictionaries

## 4.0.DEV35 (2023-11-24)

* Prevent false positives cases where incorrect key is substr of valid key in `_check_keys` function
* Implement test cases for valid exchange key and type in `exchange_proxy.py`
* Add numpy docstrings in `typos`
* Add numpy docstrings in `proxies.save` function

## 4.0.DEV34 (2023-11-21)

* Implement `ecoinvent_interface` to compare strings

## 4.0.DEV33 (2023-11-14)

* Add `projects.change_base_directories`
* Update test decorator to use `projects.change_base_directories`

## 4.0.DEV32 (2023-10-12)

* Fix a bug in `IOTable.write` function

## 4.0.DEV31 (2023-10-10)

* Fix a missing test clause for optional dependencies

## 4.0.DEV30 (2023-10-10)

* Add warnings for common activity and exchange typos

## 4.0.DEV29 (2023-09-18)

* Fix for https://github.com/brightway-lca/brightway-live/issues/39

## 4.0.DEV28 (2023-09-17)

* Make adding data to search index optional when writing

## 4.0.DEV27 (2023-09-17)

* Fix bug from withdrawn DEV22-24 line

## 4.0.DEV26 (2023-09-17)

* Allow writing databases without touching search index

## 4.0.DEV25 (2023-09-15)

* Remove `requests` for easier WASM packaging

## 4.0.DEV21 (2023-08-12)

* #140: Search for single characters
* Purge `pyprind` and `psutil`

## 4.0.DEV20 (2023-07-25)

* Remove `fasteners` and write locks
* Remove `docopt` and `bw2-uptodate.py`
* Substitute `platformdirs` for `appdirs`
* Remove `psutil` dependency

## 4.0.DEV19 (2023-06-06)

* Yanked due to insufficient testing and documentation

## 4.0.DEV18 (2022-08-19)

* Add `Exchanges.to_dataframe`

## 4.0.DEV17 (2022-08-18)

* [PR #109: Export edges and activities to dataframes](https://github.com/brightway-lca/brightway2-data/pull/109)
* [PR #105: Complete revamp of IO Table functionality](https://github.com/brightway-lca/brightway2-data/pull/105)
* [PR #104: Fix `Exchange.lca()`](https://github.com/brightway-lca/brightway2-data/pull/104)
* [Fix #95](https://github.com/brightway-lca/brightway2-data/issues/95)
* [Moved `wurst` extraction functions to `bw2data`](https://github.com/brightway-lca/brightway2-data/commit/55f107177ead8a73cdf88a5b5be56bf6811bf27f)

## 4.0.DEV16 (2022-06-09)

* Fix and test export CSV metadata from databases during datapackage creation

## 4.0.DEV15 (2022-06-04)

* Improve lookup of activity classifications and properties

## 4.0.DEV14 (2022-05-23)

* Add ``Activity.rp_exchange`` for easier access to reference product exchange
* ``Activity['foo']`` will now also lookup reference product classifications and properties

## 4.0.DEV13 (2022-04-24)

* Add `Database.new_node`

## 4.0.DEV12 (2022-04-21)

* Add `Database.set_geocollections`

## 4.0.DEV11 (2021-11-01)

* Change behaviour of `.get()` and `.get_activity()` to allow passing arbitrary filters and raise error if multiple results found
* Add `Activity.consumers()` and `Activity.producers()`.
* `technosphere()` no longer returns substitution exchanges by default (but still does if `include_substitution=True`); substitution exchanges are included by default in `production`.
* Add `.datapackage()` convenience method for processed datastores

## 4.0.DEV10 (2021-10-24)

* Only need geocollections for process nodes

## 4.0.DEV9 (2021-10-23)

* Can pass `Activity` objects to `get_activity`
* Add `delete_duplicate_exchanges` utility function
* Automatically add geocollections for easier regionalization setup

## 4.0.DEV8 (2021-10-19)

* Removing caching database lookups, it was causing pain and confusion

## 4.0.DEV7 (2021-10-17)

* `prepare_lca_inputs` shouldn't require a demand (e.g. `switch_method` in LCA class)

## 4.0.DEV6 (2021-10-13)

* Change bw2io migrations update to not require bw2io installation

## 4.0.DEV5 (2021-10-13)

* Add reprocessing migration for all data
* Fix [bw2io issue #115: FileNotFoundError in migrated project](https://github.com/brightway-lca/brightway2-io/issues/115)

## 4.0.DEV4 (2021-10-01)

* CI fixes
* Compatibility with downstream changes for Brightway 2.5

## 4.0.DEV3 (25-05-2021)

Fix bug in `prepare_lca_inputs` to pass `bw_processing` packages instead of `PyFilesystem2` objects.

## 4.0.DEV2 (25-05-2021)

Fix bug for `prepare_lca_inputs` without remapping dicts.

## 4.0.DEV1 (19-05-2021)

## Breaking changes

### Python 2 compatibility removed

Removing the Python 2 compatibility layer allows for much cleaner and more compact code, and the use of some components from the in-development Brightway version 3 libraries.

### All internal paths are now `pathlib.Path` objects

This allows for cleaner and more consistent code. Includes things like `projects.dir`.

### `SingleFile` and `JSON` backends removed

These backends were implemented many years ago, and most of the current code already assumes the SQLite backend.

### SQLite backend code refactored

The `peewee` directory is removed, and most of the code now lives in `bw2data.backends`. `LCIBackend` is removed, and now `SQLiteBackend` directly subclasses `ProcessedDataStore`.

## Other changes

### Use of `bw_processing`

We now use [bw_processing](https://github.com/brightway-lca/bw_processing) to create processed arrays. Processed arrays are compressed directories (because there is a metadata file as well, and possibly multiple array files). All databases, methods, etc. will need to be reprocessed, but this happens automatically via a data migration.

`bw_processing` has a completely different API, and LCA objects no longer have a single set of parameters (like `tech_params`) for each matrix - instead, they have data resources, which offer more flexibility, but also more complexity. See the `bw_processing`, `matrix_utils`, and `bw2calc` documentation.

### Database IDs for activities preferred

Using integer IDs is the preferred way to identify activities and products in Brightway 3. You can now easily get the integer id of an activity:

```python

>>> a = bw2data.Database("something").random()

>>> a.id
19014

```

And use the integer ID in the `get` functions:

```python

>>> bw2data.get_activity(19014)
'treatment of aluminium in car shredder residue, municipal incineration' (kilogram, RoW, None)

>>> bw2data.Database("something").get(19014)
'treatment of aluminium in car shredder residue, municipal incineration' (kilogram, RoW, None)

```

You can also call `get_id` on activity keys:

```python

>>> bw2data.get_id(a)
19014

>>> bw2data.get_id(a.key)
19014

```

### `mapping` no longer used, though compatibility layer added

You can still import `mapping`, but this will just look up the IDs from the SQLite database. There is no longer a separate file.

This change means that you **can no longer add exchanges or characterization factors which reference activities that don't (yet) exist**.

## Smaller changes

* `IOTableBackend.write` arguments have changed to `(products, prod_exchanges, tech_exchanges, bio_exchanges)`.
* `bw2data.utils.safe_filename` was moved to `bw_processing`
* `Database.get` is removed (though `Database('foo').get()` still works). Use `get_activity` instead.

### 3.6.2 (2019-11-11)

* Fixed invalid variable name creation

### 3.6.1 (2019-10-18)

Merged PR [#19: Fix activityparam rename](https://bitbucket.org/cmutel/brightway2-data/pull-requests/19).

## 3.6 (2019-10-09)

- Merged multiple pull requests ([#12](https://bitbucket.org/cmutel/brightway2-data/pull-requests/12), [#13](https://bitbucket.org/cmutel/brightway2-data/pull-requests/13), [#14](https://bitbucket.org/cmutel/brightway2-data/pull-requests/14), [#15](https://bitbucket.org/cmutel/brightway2-data/pull-requests/15), [#16](https://bitbucket.org/cmutel/brightway2-data/pull-requests/16), [#17](https://bitbucket.org/cmutel/brightway2-data/pull-requests/17), [#18](https://bitbucket.org/cmutel/brightway2-data/pull-requests/18)) from Daniël de Koning related to improving the handling of parameters, as part of his work on including parameterization in the [Activity browser](https://github.com/LCA-ActivityBrowser/activity-browser).

### 3.5.1 (2019-09-05)

- Remove temporary directories after tests finish

## 3.5 (2019-05-16)

- PR #11: Fix for searching with stop words

### 3.4.5 (2019-05-11)

- Fix `collections.abc` compatibility with Python 3.8

### 3.4.4 (2019-01-08)

- Make index creation not raise error if index already exists

### 3.4.3 (2018-09-21)

- Fix #60: No SQLite index creation after switching projects
- Merged PR #9: Better handling of non-unique parameter names

### 3.4.2 (2018-06-01)

- Fix #56: Name conflicts with multiple dummy parameters

### 3.4.1 (2018-06-01)

- Fix bug with geocollections and search indices

## 3.4 (2018-05-31)

- Eliminate inconsistency between use of `name` and `variable` by always using `name`

### 3.3.1 (2018-04-23)

- Remove print debugging statements

## 3.3 (2018-04-05)

- Compatibility with Peewee 3 [breaking changes](http://docs.peewee-orm.com/en/latest/peewee/changes.html)

## 3.2 (2018-02-16)

- Better find symbol name dependencies in exchange formulas

## 3.1.1 (2018-02-13)

- Minor compatibility changes for parameters to make unified API

## 3.1 (2018-01-18)

- Improve performance of `random` (#47)
- Added `dynamic_calculation_setups`
- All data in search engine stored in lowercase (#35)
- Fixes to writing sqlite databases for better interaction with user interfaces (#53)

# 3.0 (2017-12-2)

- Add support for hierarchical parameters and formulas (expressed as strings), with automatic and recalculation of dependencies
- Make Activity.upstream() more flexible

## 2.4.7 (2017-09-14)

- Fix bugs in `merge_databases`

## 2.4.6 (2017-08-29)

- Fix bug where `negative` value wasn't used in exchange proxy uncertainty dictionaries

## 2.4.5 (2017-08-15)

- Add database merging function (bw2data.utils.merge_databases)

## 2.4.4 (2017-04-17)

- Fix license text

## 2.4.3 (2017-04-06)

- Specify encoding of license file, and then don't. Yeah computers.

## 2.4.2 (2017-04-06)

- Remove dependency on bw2io

## 2.4.1 (2017-04-05)

- Include substitution types in `.technosphere()` iterator. Can be excluded with `include_substitution=False`.

## 2.4 (2017-03-20)

- Write-only locks are now optional and disable by default
- Removed `projects.current`.
- `Exchanges` is now consistently ordered

## 2.3.2 (2016-07-17)

- Specify a sensible order for sorting processed arrays

## 2.3.1 (2016-07-16)

- Fixed bug with Activity.copy()
- Fixed some bugs with database filtering

# 2.3 (2016-07-14)

- Use consistent sorting for all `DataStore` objects. However, this sorting is not guaranteed across machines.
- Use `np.save` instead of pickling for processed arrays.
- Added `projects.output_dir` and environment variable `BW2_OUTPUT_DIR`.
- Removed deprecated functions in `config`.
- Add field `code` to search index.

## 2.2.2 (2016-06-10)

- Changes to improve testing for bw2data and bw2calc

## 2.2.1 (2016-06-06)

- Fix some places where set_current wasn't introduced
- Rework initialization of projects and add projects tests
- Moved tests to main directory

Windows tests are failing due to naughty strings being used for project names.

# 2.2 (2016-06-03)

- Deprecated `projects.current = 'foo'` in favor or `projects.set_current('foo')`
- Added ability to switch to read only project with `projects.set_current('foo', writable=False)`
- Removed separate write of topomapping files from inventory databases. All topology handling is internal to bw2regional
- Fixed bug where `download_file` wouldn't raise an error is resource was not found.

# 2.1 (2016-05-28)

- Fix database writes not propagating to search index
- Added continuous integration tests on Windows
- Fix bug when iterating over projects

## 2.0.2 (2016-05-20)

- Better `__str__` for metadata
- Make projects sortable
- Allow forcing writes with `projects.enable_writes(force=True)`

## 2.0.1 (2016-04-14)

- Bugfix release to add unstated dependency of `pyprind`

# 2.0 (2016-04-11)

2.0 brings massive changes to how datasets are stored and searched. The first big change is a new default backend, using peewee and SQLite3. This backend has a nicer API, faster random access, and reduced memory consumption. Here are some examples of new usage patterns:

- FEATURE: New backend, sqlite, which is the default. Should massively reduce memory consumption in most cases, as entire databases don't need to be loaded.
- FEATURE: Backend now return activity and exchange proxies instead of raw data, making for easier manipulation and construction.

Note: Both packages `bw2search` and `bw2simple` are obsolete - their functionality is now included in `bw2data` by default.

Data cannot be directly migrated from bw2data < 2; instead, databases should be exported as BW2Package files and then re-imported.

# 1.4 (2014-11-26)

- BUGFIX: JSONDatabases are now JSON-serializable. Database variants must now support the keyword argument `as_dict`, and return an actual `dict` if `as_dict=True`.

## 1.3.3 (2015-02-04)

- Improve SimaPro and Ecospold2 imports

## 1.3.2 (2014-10-27)

- BUGFIX: Added missing `unidecode` dependency.
- BUGFIX: Remove error when bw2calc is not installed.

## 1.3.1 (2014-10-27)

- BUGFIX: `safe_save` now works on Windows.

# 1.3 (2014-10-25)

- FEATURE: Add SimaPro ecospold 1 imports, and create a new import "flavor" called "SimaPro8" that can handle the new way SimaPro breaks ecoinvent 3 activity names.
- FEATURE: `utils.safe_save` makes sure a file write is successful before overwriting known good data.
- CHANGE: Lots of documentation improvements.
- CHANGE: Import comments by default in ecospold 1 & 2. Remove `import_comments.py` file.
- CHANGE: Added some ecoinvent 3 units to `normalize_units`.

# 1.2 (2014-09-04)

- FEATURE: Add `backends.utils.convert_backend` utility function to switch between database backends.
- FEATURE: Added Ecospold 1 & 2 comment importers (`io.import.add_ecospold1_comments` and `io.import_comments.add_ecospold2_comments`). Comments are currently not imported by default.
- CHANGE: Ecospold 1 & 2 importers now store file directory as `directory` in metadata.
- CHANGE: Each Database should specify its `backend` attribute.

## 1.1.1 (2014-08-26)

- BUGFIX: Don't die if `xlsxwriter` not installed.

# 1.1 (2014-08-25)

- FEATURE: Add MATLAB LCI matrix exporter.
- FEATURE: Add `make_latest_version` method for SingleFileDatabases, to make reverting easier.
- BUGFIX: Make sure `uncertainify` can handle negative amount values.

## 1.0.3 (2014-08-16)

- CHANGE: Automatically set `num_cfs` for methods and `number` for databases when `.write()` is called.

## 1.0.2 (2014-08-14)

- BUGFIX: Release memory during `Updates.reprocess_all_1_0`.

## 1.0.1 (2014-08-01)

- CHANGE: Ecospold2 importer is now more resilient to incorrect input data.
- BUGFIX: uncertainify now correctly handles amount <= 0.
- Small documentation fixes.

# 1.0 (2014-07-30)

**bw2-uptodate.py is required for this update**.

Default values for various attributes need to be added when not previously specified.

- FEATURE: Pluggable LCI backends. Two backends are provided - SingleFileDatabase and and JSONDatabase, and others can be easily added. A new notebook shows how to use JSONDatabase.
- FEATURE: Ecospold2 importer is out of alpha status as of Ecoinvent 3.1.
- FEATURE: `bw2-uptodate` should now work without PATH hassles on windows. Name changed from `bw2-uptodate.py`.
- FEATURE: Searching databases is better documented and tested. A new notebook shows searching examples.
- BREAKING CHANGE: The "in" operator in searching is now "has" - the previous semantics were simply incorrect.
- CHANGE: Database exchanges without `type` now raise UntypedExchange error when processed.
- CHANGE: Database exchanges without `amount` or `input` now raise InvalidExchange error when processed.
- CHANGE: The order of database exchanges in processed arrays is sorted is changed.
- CHANGE: LCI database format is now more flexible, and almost all required elements are removed. For example, `{}` is now a valid LCI dataset.
- BUGFIX: Allow unicode in `utils.safe_filename`.
- BUGFIX: `reset_meta()` now also reset config preferences.

## 0.17.1 (2014-06-11)

- CHANGE: Improve resiliency of SimaPro import.

# 0.17 (2014-04-29)

- BREAKING CHANGE: Database 'depends' is now calculated automatically when calling Database.process().

# 0.16 (2014-04-28)

**bw2-uptodate.py is required for this update**

- FEATURE: Added `Database.filepath_intermediate` and `Database.filepath_processed` for easier access to raw data files.
- BREAKING CHANGE: All importers now produce unicode strings. Before, the SimaPro importer produced Latin-1 strings, while the XML importers produced UTF-8.
- CHANGE: `Database.process()` now uses `obj.filename`, not `obj.name`, as this is not always safe for filenames.

## 0.15.1 (2014-04-17)

- FEATURE: Utility functions to view process datasets in web browser
- FEATURE: utils.web_ui_accessible tests if web UI is running and accessible
- CHANGE: SimaPro importer can now add unlinked exchanges as new process datasets
- CHANGE: New preference key: "web_ui_address"

# 0.15 (2014-04-11)

- BREAKING CHANGE: `Database.process` skips exchanges if `type` is not `process`.
- FEATURE: `Database.list_dependents` traverses datasets to get linked databases.
- CHANGE: Query.__repr__ always returns unicode strings.
- CHANGE: SimaPro importer can now import input and output comments, including multiline comments

## 0.14.1 (2014-03-07)

No changes, just messed up packaging...

# 0.14 (2014-03-07)

**bw2-uptodate.py is required for this update**

- CHANGE: `BW2Package.export_obj` now uses `obj.filename` instead of `obj.name` for filepath of backup file (needed for LCIA methods).
- CHANGE: `categories` is no longer required by `utils.activity_hash`.
- CHANGE: `Database.copy()` no longer emits a not registered warning.
- CHANGE: `Database.copy()` makes a deep copy of data before modification.
- CHANGE: `bw2data.__init__` no longer imports the `io` and `proxies` directories, to avoid namespace conflicts with io standard library package.

# 0.13 (2014-02-13)

- BREAKING CHANGE: `Database.process()` now only includes datasets with type `process` in constructing geomapping array.

##0.12.2 (2014-02-04)

- CHANGE: BW2Package import file ignores warnings

## 0.12.1 (2014-02-04)

New BW2Package format

The new BW2Package is not specific to databases or methods, but should work for any data store that implements the DataStore API. This allows for normalization, weighting, regionalization, and others, and makes it easy to backup and restore.

# 0.12 (2014-02-04)

**bw2-uptodate.py is required for this update**

### Safe filenames

The algorithm to create filenames was changed to prevent illegal characters being used. See `utils.safe_filename`.

# 0.11 (2014-01-28)

**bw2-uptodate.py is required for this update**

### Upgrades to updates

The update code filename was changed to `updates.py`, and dramatically simplified. Code was organized and moved to an Updates class. All functionality was removed from utility scripts and `bw2-uptodate.py`. Fresh installs should not have erroneous "updates needed" warnings.

### Generic DataStore makes new matrices easy

`data_store.DataStore` defines a template for all data stores which could be processed into matrix data, and provides a lot of functionality for free. New objects subclass `DataStore` or `ImpactAssessmentDataStore`, and need only define their unique data fields, metadata store, and validator. Abstracting common functionality into a simple class hierarchy should also produce fewer bugs.

### Smaller changes

- BREAKING CHANGE: The filenames for LCIA methods are now derived from the MD5 of the name. This breaks all method abbreviations.
- BREAKING CHANGE: The filename and filepath attributes in SerializedDict and subclasses moved from `_filename` and `filepath` to `filename` and `filepath`
- BREAKING CHANGE: Register for all data store now takes any keyword arguments. There are no required or positional arguments.
- BREAKING CHANGE: Database.process() doesn't raise an AssertionError for empty databases
- FEATURE: Database.process() writes a geomapping processed array (linking activity IDs to locations), in addition to normal matrix arrays.
- FEATURE: Tests now cover more functionality, and should allow for more worry-free development in the future.
- CHANGE: Database datasets are not required to specify a unit.
- CHANGE: The default biosphere database is no longer hard coded, and can be set in config.p['biosphere_database']. The default is still "biosphere".
- CHANGE: The default global location is no longer hard coded, and can be set in config.p['global_location']. The default is still "GLO".
- CHANGE: Ecospold 1 & 2 data extractors now only have classmethods, and these classes don't need to be instantiated. A more functional style was used to try to avoid unpleasant side effects.
