from blinker import signal

signaleddataset_on_save = signal(
    "bw2data.signaleddataset_on_save",
    doc="""Emitted when a SignaledDataset is saved.

Expected inputs:
    * `old` - the previous version of the dataset
    * `new` - the new version of the dataset

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
