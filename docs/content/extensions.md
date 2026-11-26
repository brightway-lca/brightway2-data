# Read-only LCI extensions

An extension is a read-only SQLite file with one or more inventory databases, which a project attaches to its own `lci/databases.db`. Its databases then work like the project's own: `Database`, `databases`, `get_node`, search, `upstream()` and calculations see them, and the project's databases can link to them. They can't be changed, and every change goes to the project's own file.

Use extensions for large background databases which many projects share, such as ecoinvent or a flow list. They are built once, copied as a file, and attached in every project that needs them. That is faster than importing them into each project, uses less disk space, and every project has exactly the same version.

## Building extensions

`projects.export_extension(filepath, databases, version=None, overwrite=False)` writes databases of the current project into a new extension file and returns its id:

```python
import bw2data as bd

bd.projects.set_current("build flows")
# ... import or write the database "flows-1.0"
bd.projects.export_extension("flows-1.0.db", ["flows-1.0"], version="1.0")
```

The file holds the databases' activities (with their ids), their exchanges, their metadata, their search indexes and their processed datapackages; nothing else of the project. Databases which are `dirty`, or have no processed datapackage, are processed first. Every export gets a new id, even when the data is the same.

`version` is a label for people. Name databases with their version, e.g. `ecoinvent-3.11-cutoff` instead of `ecoinvent`. A project can't have two databases with the same name, so versioned names let projects use two versions side by side.

### Each database belongs to one file

A database name can be stored in only one file: the project's own file or one of its extensions. Attaching an extension with a database name which the project or another attached extension already uses is refused.

So a database which several extensions link to, such as a flow list, must be its own extension. Two inventory extensions can't each bundle their own copy of it.

### Requirements

An exported database may link to the other databases exported with it, and to databases in extensions attached to the project. Links to any other database are refused (`bw2data.errors.ExtensionError`): an extension can't depend on data which can still change.

When an exported database links to a database in an attached extension, the new file *requires* that extension, in exactly the version which is attached. It records the extension's id and a fingerprint of the linked database's activity codes and types. A project can only attach the new file together with that version.

To build an inventory on a separate flow list:

```python
# 1. The flow list, as its own extension
bd.projects.set_current("build flows")
bd.projects.export_extension("flows-1.0.db", "flows-1.0", version="1.0")

# 2. The inventory, in a project which has the flow list attached
bd.projects.set_current("build inventory")
bd.projects.attach_extension("flows-1.0.db")
# ... import or write "inventory-1.0", linking its biosphere exchanges to "flows-1.0"
bd.projects.export_extension("inventory-1.0.db", "inventory-1.0", version="1.0")
```

`inventory-1.0.db` requires `flows-1.0.db`. Other inventories built against the same flow list file can be attached next to it.

IO table databases can't be exported yet.

## Attaching

```python
bd.projects.set_current("my study")
bd.projects.attach_extension(["inventory-1.0.db", "flows-1.0.db"])
```

`projects.attach_extension(path, name=None, immutable=False, allow_main_requirements=False)` takes one file or a list. Files which require each other can be given in any order, but must be given together, unless the ones they require are already attached.

- `name` identifies the extension in the project. The default is the file name without its suffix, e.g. `inventory-1.0`.
- `immutable=True` tells SQLite that the file never changes, for files on network or read-only volumes.
- `allow_main_requirements=True` lets a database in the project's own file meet a requirement of the extension, if it has exactly the required fingerprint.

The extensions are stored with the project and attached again every time it is opened. `projects.copy_project` copies them, and `projects.delete_project` never deletes the files.

If an extension can't be attached when a project is opened (its file is missing, was replaced, or needs an extension which isn't available), the project opens without it and gives a warning. Its databases are then not in the project. `projects.extensions` lists each extension with its `status` (`"available"`, `"missing file"`, `"missing requirement"` or `"invalid"`), the `problem`, its `path`, `id` and `databases`.

## Using extension databases

Extension databases read like any other database. The project's databases can link to them:

```python
mine = bd.Database("my foreground")
mine.write({
    ("my foreground", "bike"): {
        "name": "bike",
        "exchanges": [
            {"input": ("inventory-1.0", "steel"), "amount": 2.5, "type": "technosphere"},
        ],
    },
})
```

Changing an extension database, its nodes or its edges raises `bw2data.errors.ReadOnlyExtensionError`. This is a subclass of `ExtensionError` and of `peewee.IntegrityError`. To change one, copy it into the project first: `bd.Database("inventory-1.0").copy("my inventory")`.

`Database(name).is_extension` tells whether a database is stored in an extension, and `Database(name).extension` tells which one.

Calculations read the processed datapackages stored in the extension file, so attaching needs no processing. For ecoinvent 3.13, the first score after attaching took 1.6 s in a benchmark, against 20.5 s when the project processes the database itself. The project still processes an extension database in two cases: if the file was built before bw2data stored datapackages, or if a requirement is met by a database in the project's own file (`allow_main_requirements`).

## New versions

`projects.update_extension(name, path=None, force=False)` takes up a new version of an extension: the file which is at its path now, or the file `path`. The extension keeps its name, its options and the project's links into it. For the same version at another path (a moved file), only the path changes.

An update is refused unless `force=True` in three cases:

- another attached extension requires the old version (requirements name one version);
- databases of the project link to activities which the new version lacks;
- methods or normalizations of the project have factors for flows which the new version lacks.

Replace the file of an extension only while no project uses it, or build the new version at another path and pass `path`. If a file is replaced while a project has it open, what happens is undefined.

## Detaching

`projects.detach_extension(name, force=False)` removes an extension from the project. The file is not touched. Detaching is refused while another attached extension requires it, while databases of the project link to it, or while methods or normalizations have factors for its flows. `force=True` detaches anyway, with a warning.

## Limits

- **Methods:** LCIA methods can't be stored in extensions yet. Methods in the project can use extension flows.
- **Regionalization:** the location ids in a stored datapackage are those of the project which built the file. Regular calculations don't use them, but regionalized calculations (e.g. with bw2regional) can't use extension databases correctly until exported files carry their own location mapping.
- **Event-sourced projects:** attaching and detaching are not recorded as revisions.
- **Parameters:** parameters can't be stored in extensions.

## Signals

`bw2data.signals.on_extension_attached` and `on_extension_detached` are sent when extensions are attached and detached, and by `update_extension`.
