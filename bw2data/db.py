import logging
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any, Callable, Optional

import sqlalchemy as sa
from sqlalchemy import MetaData
from sqlalchemy.orm import Session, scoped_session, sessionmaker

logger = logging.getLogger(__name__)


def merge_metadata(*original_metadata) -> MetaData:
    """
    Merge one or more declarative metadata for sqlalchemy
    """
    merged = MetaData()

    for metadata in original_metadata:
        for table in metadata.tables.values():
            table.to_metadata(merged)

    return merged


class BWSession(Session):
    """
    Wrapper for sqlalchemy Session class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create(self, *models):
        self.add_all(models)
        self.commit()
        for model in models:
            self.refresh(model)


class Database:
    def __init__(self, url, scopefunc: Optional[Callable[[], Any]] = None, **kwargs):
        """
        Initialize the database.

        Args:
            url: url database
            scopefunc: optional function which defines the scope for sessions.
            **kwargs: passed into sqlalchemy engine.
        """
        self._engine = sa.create_engine(url, **kwargs)

        self._session_factory: scoped_session[BWSession] = scoped_session(
            sessionmaker(
                class_=BWSession,
                bind=self._engine,
            ),
            scopefunc,
        )

    @property
    def engine(self):
        return self._engine

    @property
    def session_factory(self):
        return self._session_factory

    @contextmanager
    def session(self, **kwargs) -> AbstractContextManager[BWSession]:
        session: BWSession = self._session_factory(**kwargs)
        try:
            yield session
        except Exception:
            logger.exception("Session rollback because of exception", exc_info=True)
            session.rollback()
            raise


class SubstitutableDatabase:
    def __init__(self, url: str, metadata: "MetaData"):
        self._url = url
        self._metadata = metadata
        self._database = self._create_database()

    def _create_database(self):
        url = self._url
        if issubclass(type(url), Path) or not url.startswith("sqlite://"):
            url = "sqlite:///" + str(url)
        db = Database(url)
        self._metadata.create_all(bind=db.engine)
        return db

    @property
    def db(self):
        return self._database

    def change_path(self, _url):
        self.db.engine.dispose()
        self._url = _url
        self._database = self._create_database()

    def execute_sql(self, *args, **kwargs):
        with self.db.session(autocommit=True) as session:
            return session.execute(*args, **kwargs)

    @contextmanager
    def transaction(self):
        with self.db.session_factory.begin() as session:
            yield session

    @property
    def session_factory(self):
        return self.db.session_factory

    @contextmanager
    def session(self, **kwargs) -> AbstractContextManager[BWSession]:
        yield self.db.session(**kwargs)

    def vacuum(self):
        logging.info("Vacuuming database ")
        self.execute_sql("VACUUM;")
