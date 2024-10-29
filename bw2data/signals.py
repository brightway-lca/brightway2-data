from blinker import signal

database_saved = signal(
    "bw2data.database_save",
    doc="""Nothing to see here""",
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

test_signal = signal(
    "bw2data.test_signal",
    doc="""
Signal used for testing.

Expected inputs:
    * `bw2data.signals.Dummy` instance

No expected return value.
""",
)
