from blinker import signal

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
