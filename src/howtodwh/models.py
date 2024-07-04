import sqlalchemy
from sqlalchemy import Column, Boolean, func, DateTime, Enum, UUID, BigInteger
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship

from .schemas import (
    RDBMSSourceSystemType,
    RDBMSSourceSystem,
    RDBMSNamespace,
    RelationType,
    RDBMSRelation,
    RDBMSGenericAttributeType,
    RDBMSAttributeType,
    RDBMSAttribute,
    RDBMSConstraint,
    ConstrainType,
    ETLLoadingType,
    ETLLoading,
    RDBMSExcludedFromLoadingAttribute,
    ETLIncrementalLoading,
    ETLLoadingMetric,
    ETLLoadingStatusType,
    ETLLoadingStatus,
    ETLLoadingByChunksStrategy,
    ETLLoadingByChunksMetric,
    DWHRelation,
)

mapper_registry = registry()
metadata_obj = MetaData()


source_system_type = Table(
    "source_system_type",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False, unique=True),
)

mapper_registry.map_imperatively(
    RDBMSSourceSystemType,
    source_system_type,
    properties={
        "systems": relationship(RDBMSSourceSystem, back_populates="type"),
    },
)

source_system = Table(
    "source_system",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("key", String, nullable=False, unique=True),
    Column("fqn", String, nullable=False, unique=True),
    Column("version", String(255), nullable=True),
)


mapper_registry.map_imperatively(
    RDBMSSourceSystem,
    source_system,
    properties={
        "type": relationship(RDBMSSourceSystemType, back_populates="systems"),
    },
)


namespace = Table(
    "namespace",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("system_id", Integer, ForeignKey("source_system.id"), nullable=False),
    Column("external_id", String(255), nullable=True),
)

mapper_registry.map_imperatively(RDBMSNamespace, namespace)


relation = Table(
    "relation",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("type", Enum(RelationType), nullable=False),
    Column("namespace_id", Integer, ForeignKey("namespace.id"), nullable=False),
    Column("external_id", String(255), nullable=True),
)

mapper_registry.map_imperatively(RDBMSRelation, relation)


generic_attribute_types = Table(
    "generic_attribute_type",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
)


mapper_registry.map_imperatively(RDBMSGenericAttributeType, generic_attribute_types)


attribute_type = Table(
    "attribute_type",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("system_id", Integer, ForeignKey("source_system.id"), nullable=False),
    Column("type", ForeignKey("generic_attribute_type.id"), nullable=False),
)


mapper_registry.map_imperatively(RDBMSAttributeType, attribute_type)

attribute = Table(
    "attribute",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("position", Integer, nullable=False),
    Column("type_id", Integer, ForeignKey("attribute_type.id"), nullable=False),
    Column("size", Integer, nullable=True),
    Column("scale", Integer, nullable=True),
    Column("is_nullable", Boolean, nullable=True, server_default=sqlalchemy.true()),
    Column("default_expression", String(255), nullable=True),
    Column("comment", String(255), nullable=True),
    Column("external_id", String(255), nullable=True),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_onupdate=func.now(),
    ),
)

mapper_registry.map_imperatively(
    RDBMSAttribute,
    attribute,
)


constraint = Table(
    "constraint",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("type", Enum(ConstrainType), nullable=False),
    Column("name", String, nullable=False),
)


mapper_registry.map_imperatively(
    RDBMSConstraint,
    constraint,
)


attribute_x_constraint = Table(
    "attribute_constraint",
    metadata_obj,
    Column("attribute_id", Integer, ForeignKey("attribute.id"), primary_key=True),
    Column("constraint_id", Integer, ForeignKey("constraint.id"), primary_key=True),
)


dwh_relation = Table(
    "dwh_relation",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("fqn", String, nullable=False, unique=True),
    Column("has_rows_hash", Boolean, nullable=False, server_default=sqlalchemy.false()),
    Column("has_pks_hash", Boolean, nullable=False, server_default=sqlalchemy.false()),
    Column("has_pks_rn", Boolean, nullable=False, server_default=sqlalchemy.false()),
)

mapper_registry.map_imperatively(DWHRelation, dwh_relation)

etl_loading = Table(
    "etl_loading",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    Column("name", String, nullable=False, unique=True),
    Column("type", Enum(ETLLoadingType), nullable=False),
    Column("has_to_hash", Boolean, nullable=False, server_default=sqlalchemy.false()),
    Column(
        "has_to_order_by_pk", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    Column("source_relation_id", Integer, ForeignKey("relation.id"), nullable=False),
    Column(
        "destination_relation_id",
        Integer,
        ForeignKey("dwh_relation.id"),
        nullable=False,
    ),
)

mapper_registry.map_imperatively(
    ETLLoading,
    etl_loading,
    properties={
        "polymorphic_on": "type",
        "polymorphic_identity": ETLLoadingType.SNAPSHOT,
    },
)


etl_incremental_loading = Table(
    "etl_incremental_loading",
    metadata_obj,
    Column("loading_id", Integer, ForeignKey("etl_loading.id"), primary_key=True),
    Column(
        "is_overlapping", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    Column(
        "increment_attribute_id", Integer, ForeignKey("etl_loading.id"), nullable=False
    ),
)


mapper_registry.map_imperatively(
    ETLIncrementalLoading,
    etl_incremental_loading,
    properties={
        "polymorphic_identity": ETLLoadingType.INCREMENTAL,
    },
)

etl_loading_metric = Table(
    "etl_loading_metric",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    Column("etl_loading_id", Integer, ForeignKey("etl_loading.id")),
    Column("rows_set_hash", UUID, nullable=True),
    Column("records_count", BigInteger, nullable=True),
    Column("increment_value", String, nullable=True),
)

mapper_registry.map_imperatively(
    ETLLoadingMetric,
    etl_loading_metric,
)

etl_loading_status = Table(
    "etl_loading_status",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    Column("etl_loading_id", Integer, ForeignKey("etl_loading.id"), nullable=False),
    Column("status", Enum(ETLLoadingStatusType), nullable=False),
)

mapper_registry.map_imperatively(
    ETLLoadingStatus,
    etl_loading_status,
)

etl_loading_by_chunks = Table(
    "etl_loading_by_chunks",
    metadata_obj,
    Column("chunk_size", Integer, nullable=False),
    Column(
        "crushing_attribute_id", Integer, ForeignKey("attribute.id"), primary_key=True
    ),
    Column("etl_loading_id", Integer, ForeignKey("etl_loading.id"), nullable=False),
)

mapper_registry.map_imperatively(
    ETLLoadingByChunksStrategy,
    etl_loading_by_chunks,
)


etl_loading_by_chunks_metric = Table(
    "etl_loading_by_chunks_metric",
    metadata_obj,
    Column("left_boundary", String, nullable=False),
    Column("right_boundary", String, nullable=False),
)


mapper_registry.map_imperatively(
    ETLLoadingByChunksMetric,
    etl_loading_by_chunks_metric,
)


excluded_from_loading_attribute = Table(
    "excluded_from_loading_attribute",
    metadata_obj,
    Column("attribute_id", Integer, ForeignKey("attribute.id"), primary_key=True),
    Column("loading_id", Integer, ForeignKey("etl_loading.id"), primary_key=True),
)

mapper_registry.map_imperatively(
    RDBMSExcludedFromLoadingAttribute,
    excluded_from_loading_attribute,
)
