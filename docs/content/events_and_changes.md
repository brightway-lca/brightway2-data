# `SignaledDataset` Events

`bw2data` emits the signal `signaleddataset_on_save` when changes are made to the following objects:

* `bw2data.backends.proxies.Activity` (via `bw2data.backends.schema.ActivityDataset`)
* `bw2data.backends.schema.ActivityDataset`
* `bw2data.backends.proxies.Exchange` (via `bw2data.backends.schema.ExchangeDataset`)
* `bw2data.backends.schema.ExchangeDataset`
* `bw2data.meta.databases`

These change events can grouped into three types: `create`, `update`, and `delete`.

Change events can have side effects. For example, deleting an `Activity` will delete the `Exchanges` which have that node as their `output`, and remove that node from the search indices.

# Side effects

Higher-level objects can cause changes to lower level objects. The object hierarchy is:

1. Database
2. Activity
3. Exchange

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
