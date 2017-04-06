# Changelog

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
