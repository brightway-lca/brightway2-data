from bw2data import databases
from bw2data.data_store import ProcessedDataStore


def DatabaseChooser(name: str, backend: str = "sqlite") -> ProcessedDataStore:
    """A method that returns a database class instance.

    Database types are specified in `databases[database_name]['backend']`.

    """
    from bw2data.subclass_mapping import DATABASE_BACKEND_MAPPING

    if name in databases:
        backend = databases[name].get("backend") or backend

    if not backend or not isinstance(backend, str):
        raise ValueError(
            f"Invalid value for backend: '{backend}'. Must be a string in "
            + "`bw2data.database.DATABASE_BACKEND_MAPPING`"
        )

    try:
        return DATABASE_BACKEND_MAPPING[backend](name)
    except KeyError as exc:
        raise KeyError(f"Backend {backend} not found") from exc


# Backwards compatibility
Database = DatabaseChooser
