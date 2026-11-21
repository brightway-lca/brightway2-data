# `SignaledDataset` Events

`bw2data` emits the signal `signaleddataset_on_save` when changes are made to the following objects:

* `bw2data.backends.proxies.Activity` (via `bw2data.backends.schema.ActivityDataset`)
* `bw2data.backends.schema.ActivityDataset`
* `bw2data.backends.proxies.Exchange` (via `bw2data.backends.schema.ExchangeDataset`)
* `bw2data.backends.schema.ExchangeDataset`
* `bw2data.meta.databases`
* `bw2data.parameters.ProjectParameter`
* `bw2data.parameters.DatabaseParameter`
* `bw2data.parameters.ActivityParameter`
* `bw2data.parameters.ParameterizedExchange`

These change events can grouped into three types: `create`, `update`, and `delete`.

Change events can have side effects. For example, deleting an `Activity` will delete the `Exchanges` which have that node as their `output`, and remove that node from the search indices.

# When signals are sent

Signals have two uses: `bw2data` uses them to write revisions on event-sourced projects (`projects.dataset.is_sourced`), and other code can connect to them to react to changes, for example to refresh a user interface.

Most signals are cheap and are always sent. Four need extra queries to build their arguments, so by default they are only sent on event-sourced projects:

* `signaleddataset_on_save` - one query per save to fetch the previous version
* `on_database_metadata_change` - reads the metadata of all databases before and after the change
* `on_database_write` - sent by `Database.write` unless called with `signal=False`
* The `_save_signal` of `SerializedDict` and `PickledDict` subclasses - re-reads the file and copies the data

To receive these four signals on projects which are not event sourced, turn them on for the current process:

```python
from bw2data import signals

signals.set_signalled(True)
```

This setting is held in memory. It is not stored in the project, it applies to every project opened by the process, and it is off in every new process. An application which listens to these signals should call `set_signalled(True)` once at startup; scripts which don't listen keep the faster default. `signals.is_signalled()` returns the current setting, and `with signals.signalled():` turns signals on for a block of code.

The signals carry the same arguments whether they are sent because the project is event sourced or because of `set_signalled`. Revisions are only written on event-sourced projects.

## Copying a database

`Database.copy` sends `on_database_copy` with the new database name (`name`) and the copied database name (`source`) as its last step, when the new database has its data and parameters. It is always sent and doesn't generate a revision.

If `on_database_write` is sent during a copy, it arrives earlier, before the parameters are copied. Listeners which need the complete copy should use `on_database_copy`.

# Side effects

Higher-level objects can cause changes to lower level objects. The object hierarchy is:

1. Database
2. Activity
3. Exchange
4a. ProjectParameter
4b. DatabaseParameter
4c. ActivityParameter
5. ParameterizedExchange (but a change to a `ParameterizedExchange` can change the amount in an `Exchange`)

So changing the name of a `Database` will cause changes to `Activity` and `Exchange` objects, deleting an `Activity` object will delete `Exchange` objects and possibly `ParameterizedExchange` objects, and changing the value of a `DatabaseParameter` could change a `ParameterizedExchange` which would then cause a change in an `Exchange`.

Our general policy is to let the existing code bases make this changes whenever possible. So, for example, updating a `ParameterizedExchange` value should change the `Exchange` value as well, but this code will execute on both the source and target computers, so the change to `Exchange` doesn't need to be captured as an event.

There are exceptions to this policy when the changes can't be reproduced across computers. This mostly means cases where objects are being created - we want to make sure we have identical `id` values everywhere. This includes both object creation but also some special cases which skip object creation via `.save()`, such as copying activities.

# Change initiators: `Activity`

## Node creation and updates

Outside of the exceptions listed below, changes to a `Node` or an `Activity` will call `ActivityDataset.save`, which will dispatch an `signaleddataset_on_save` event and go through the standard code path in `revisions`. Deleting a `Node` / `Activity` will call `delete_instance`, which also goes through the standard code path in `revisions`.

Calling `Node.copy()` will call `.save()` on the new `Node` and on any `Edge` objects created, leading to multiple revision events.

## Node updates exception 1: `database` attribute change

Changing a node's `database` requires updating the edges that reference that node, as node references as done by source and target `database` and `code`.

The necessary changes are applied in the function: `bw2data.backends.proxies.Activity._change_database`. This function **skips** `.save()` and does `UPDATE` SQL queries directly on the database.

Our approach: Call `Activity.__setitem__()` and changing the `database` attribute emits `on_activity_database_change`. This event is consumed by `bw2data.revisions.RevisionedNode.activity_database_change`, which in turn calls `bw2data.backends.proxies.Activity._change_database` with `signal=False` to apply the generated revisions without triggering circular events.

## Node updates exception 2: `code` attribute change

Changing a node's `database` requires updating the edges that reference that node, as node references as done by source and target `database` and `code`.

The necessary changes are applied in the function: `bw2data.backends.proxies.Activity._change_code`. This function **skips** `.save()` and does `UPDATE` SQL queries directly on the database.

Our approach: Call `Activity.__setitem__()` and changing the `code` attribute emits `on_activity_code_change`. This event is consumed by `bw2data.revisions.RevisionedNode.activity_code_change`, which in turn calls `bw2data.backends.proxies.Activity._change_code` with `signal=False` to apply the generated revisions without triggering circular events.

# Change initiators: `Exchange`

Changes to an `Edge` or an `Exchange` will call `ExchangeDataset.save`, which will dispatch an `signaleddataset_on_save` event and go through the standard code path in `revisions`. Deleting a `Edge` / `Exchange` will call `delete_instance`, which also goes through the standard code path in `revisions`.
