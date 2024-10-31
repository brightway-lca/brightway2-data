import sqlite3
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional, Union

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql
from playhouse.hybrid import hybrid_property
from sqlalchemy import Computed, String, func
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column
from sqlalchemy.sql.functions import GenericFunction

SnowflakeID = Annotated[int, "Snowflake ID"]
BigInt = Annotated[int, "Big integer"]
Text = Annotated[str, "Text"]


class json_extract(GenericFunction):
    type = String()
    inherit_cache = True

    def __init__(self, column, json_path):
        super(json_extract, self).__init__()
        self.column = column
        self.json_path = json_path


@compiles(json_extract, "postgresql")
def compile_json_extract_postgresql(element, compiler, **kw):
    # PostgreSQL uses -> and ->> operators for JSON
    json_path_expr = element.column
    for key in element.json_path:
        json_path_expr = json_path_expr[key]
    return compiler.process(json_path_expr)


@compiles(json_extract, "sqlite")
def compile_json_extract_sqlite(element, compiler, **kw):
    json_path_str = "$" + "".join(
        [f".{key}" if isinstance(key, str) else f"[{key}]" for key in element.json_path]
    )
    return f"JSON_EXTRACT({compiler.process(element.column)}, '{json_path_str}')"


@compiles(sa.JSON, "sqlite")
def compile_custom_json_sqlite(type_, compiler, **kw):
    """We prefer blob when available otherwise fallback to the text version"""
    if sqlite3.sqlite_version_info >= (3, 45, 0):
        return "BLOB"
    return "TEXT"


SQL_BIGINT = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
SQL_BIGINT = SQL_BIGINT.with_variant(sa.BigInteger(), "postgresql")
SQL_JSON = sa.JSON()
SQL_JSON = SQL_JSON.with_variant(sa.dialects.postgresql.JSONB(), "postgresql")


class Base(MappedAsDataclass, DeclarativeBase):
    """subclasses will be converted to dataclasses"""

    type_annotation_map = {
        SnowflakeID: SQL_BIGINT,
        BigInt: SQL_BIGINT,
        Text: sa.Text(),
        dict: SQL_JSON,
    }


class Node(Base):
    __tablename__ = "node"

    revision_id: Mapped[SnowflakeID] = mapped_column(primary_key=True)
    persistent_id: Mapped[SnowflakeID] = mapped_column()
    transaction_id: Mapped[SnowflakeID] = mapped_column()
    branch_id: Mapped[int] = mapped_column(sa.Integer, server_default="0")
    deleted: Mapped[bool] = mapped_column()
    payload: Mapped[dict] = mapped_column(server_default="{}")
    node_type: Mapped[Text] = mapped_column(Computed(json_extract(payload, "$.type")))

    @hybrid_property
    def code(self) -> str:
        return self.payload.get("code")

    @code.setter
    def code(self, value: str):
        self.payload["code"] = value

    @code.expression
    def code(cls):
        return cls.payload["code"]

    @hybrid_property
    def database(self) -> str:
        return self.payload.get("database")

    @database.setter
    def database(self, value: str):
        self.payload["database"] = value

    @database.expression
    def database(cls):
        return cls.payload["database"]

    @hybrid_property
    def created(self) -> Optional[datetime]:
        return self.payload.get("created")

    @created.setter
    def created(self, value: Optional[datetime]):
        self.payload["created"] = value

    @created.expression
    def created(cls):
        return cls.payload["created"]

    @hybrid_property
    def modified(self) -> Optional[datetime]:
        return self.payload.get("modified")

    @modified.setter
    def modified(self, value: Optional[datetime]):
        self.payload["modified"] = value

    @modified.expression
    def modified(cls):
        return cls.payload["modified"]

    @hybrid_property
    def name(self) -> Optional[str]:
        return self.payload.get("name")

    @name.setter
    def name(self, value: Optional[str]):
        self.payload["name"] = value

    @name.expression
    def name(cls):
        return cls.payload["name"]

    @hybrid_property
    def unit(self) -> Optional[str]:
        return self.payload.get("unit")

    @unit.setter
    def unit(self, value: Optional[str]):
        self.payload["unit"] = value

    @unit.expression
    def unit(cls):
        return cls.payload["unit"]

    @hybrid_property
    def location(self) -> Optional[str]:
        return self.payload.get("location")

    @location.setter
    def location(self, value: Optional[str]):
        self.payload["location"] = value

    @location.expression
    def location(cls):
        return cls.payload["location"]

    # @hybrid_property
    # def node_type(self) -> Optional[str]:
    #     return self.payload.get("type")
    #
    # @node_type.setter
    # def node_type(self, value: Optional[str]):
    #     self.payload["type"] = value
    #
    # @node_type.expression
    # def node_type(cls):
    #     return cls.payload["type"].astext

    @hybrid_property
    def comment(self) -> Union[str, dict, None]:
        return self.payload.get("comment")

    @comment.setter
    def comment(self, value: Union[str, dict, None]):
        self.payload["comment"] = value

    @comment.expression
    def comment(cls):
        return cls.payload["comment"]

    @hybrid_property
    def filename(self) -> Optional[str]:
        return self.payload.get("filename")

    @filename.setter
    def filename(self, value: Optional[str]):
        self.payload["filename"] = value

    @filename.expression
    def filename(cls):
        return cls.payload["filename"].astext

    @hybrid_property
    def references(self) -> Optional[List[dict]]:
        reference_ids = self.payload.get("references")
        return reference_ids

    @references.setter
    def references(self, value: Optional[List[dict]]):
        self.payload["references"] = value

    @references.expression
    def references(cls):
        return cls.payload["references"]

    @hybrid_property
    def tags(self) -> Optional[Dict[str, Any]]:
        return self.payload.get("tags")

    @tags.setter
    def tags(self, value: Optional[Dict[str, Any]]):
        self.payload["tags"] = value

    @tags.expression
    def tags(cls):
        return cls.payload["tags"]


class Edge(Base):
    __tablename__ = "edge"

    revision_id: Mapped[SnowflakeID] = mapped_column(primary_key=True)
    persistent_id: Mapped[SnowflakeID] = mapped_column()
    source_id: Mapped[BigInt] = mapped_column()
    target_id: Mapped[BigInt] = mapped_column()
    transaction_id: Mapped[SnowflakeID] = mapped_column(nullable=True)
    branch_id: Mapped[int] = mapped_column(server_default="0")
    deleted: Mapped[bool] = mapped_column()
    payload: Mapped[dict] = mapped_column(server_default="{}")
    edge_type: Mapped[Text] = mapped_column()


class Transaction(Base):
    __tablename__ = "transaction"

    id: Mapped[BigInt] = mapped_column(primary_key=True)
    transaction_type: Mapped[Text] = mapped_column()
    message: Mapped[Text] = mapped_column()
    branch_id: Mapped[BigInt] = mapped_column(server_default="0")


class Branch(Base):
    __tablename__ = "branch"

    id: Mapped[BigInt] = mapped_column(primary_key=True)
    serial: Mapped[Text] = mapped_column()


ActivityDataset = Node
ExchangeDataset = Edge


def get_id(key):
    if isinstance(key, int):
        return key
    else:
        raise NotImplementedError("get_id not implemented")
