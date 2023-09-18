# data Changelog

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
