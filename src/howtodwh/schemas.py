from datetime import datetime, UTC
from enum import auto, Enum
from dataclasses import dataclass, field
from typing import Optional


@dataclass(unsafe_hash=True)
class RDBMSSourceSystemType:
    id: int = field(init=False, compare=False)
    name: str = field(compare=True, hash=True)


@dataclass(unsafe_hash=True)
class RDBMSSourceSystem:
    id: int = field(init=False, compare=False)
    key: str = field(hash=True, compare=False)
    fqn: str = field(compare=True)
    type: RDBMSSourceSystemType = field(compare=True)
    version: Optional[str] = field(compare=True, default=None)
    namespaces: list["RDBMSNamespace"] = field(compare=False, default_factory=list)
    relations: list["RDBMSRelation"] = field(compare=False, default_factory=list)
    types: list["RDBMSAttributeType"] = field(compare=False, default_factory=list)


@dataclass(unsafe_hash=True)
class RDBMSNamespace:
    id: int = field(init=False, compare=False)
    name: str = field(hash=True, compare=True)
    system_id: int = field(init=False, compare=True)
    external_id: Optional[int] = field(compare=False, default=None)
    relations: list["RDBMSRelation"] = field(compare=False, default_factory=list)


class RelationType(Enum):
    TABLE = auto()
    VIEW = auto()


@dataclass(unsafe_hash=True)
class RDBMSRelation:
    id: int = field(init=False, compare=False)
    name: str = field(compare=True, hash=True)
    type: RelationType = field(compare=False)
    namespace_id: int = field(init=False, compare=True)
    comment: Optional[str] = field(compare=True, default=None)
    external_id: Optional[int] = field(compare=False, hash=False, default=None)
    attributes: list["RDBMSAttribute"] = field(compare=False, default_factory=list)
    constraints: list["RDBMSConstraint"] = field(compare=False, default_factory=list)


@dataclass(unsafe_hash=True)
class RDBMSGenericAttributeType:
    id: int = field(init=False, compare=False)
    name: str = field(compare=True, hash=True)


@dataclass(unsafe_hash=True)
class RDBMSAttributeType(RDBMSGenericAttributeType):
    system_id: int = field(init=False, compare=True)
    generic_attribute_type_id: int = field()


@dataclass(unsafe_hash=True)
class RDBMSAttribute:
    id: int = field(init=False, compare=False)
    name: str = field(compare=True, hash=True)
    position: int = field(init=True, compare=True)
    relation_id: int = field(init=False, compare=True)
    type_id: int = field(compare=True)
    size: int = field(init=False, compare=True)
    scale: int = field(init=False, compare=True)
    is_nullable: bool = field(init=False, compare=True)
    default_expression: str = field(init=False, compare=True)
    comment: Optional[str] = field(compare=True, default=None)
    external_id: Optional[int] = field(compare=False, hash=False, default=None)
    created_at: datetime = field(
        init=False, compare=False, default_factory=lambda: datetime.now(UTC)
    )
    updated_at: datetime = field(
        init=False, compare=False, default_factory=lambda: datetime.now(UTC)
    )
    constraints: list["RDBMSConstraint"] = field(compare=False, default_factory=list)


class ConstrainType(str, Enum):
    PK = "primary key"
    FK = "foreign key"
    IX = "index"
    UQ = "unique"


@dataclass(unsafe_hash=True)
class RDBMSConstraint:
    id: int = field(init=False, compare=False)
    type: ConstrainType = field(compare=True)
    name: str = field(compare=True, hash=True)
    attributes: list[RDBMSAttribute] = field(compare=False, default_factory=list)


class ETLLoadingType(Enum):
    SNAPSHOT = auto()
    INCREMENTAL = auto()


@dataclass(unsafe_hash=True)
class ETLLoading:
    id: int = field(init=False, compare=False)
    created_at: datetime = field(
        init=False, compare=False, default_factory=lambda: datetime.now(UTC)
    )
    name: str = field(compare=True, hash=True)
    type: ETLLoadingType = field(compare=True)
    has_to_hash: bool = field(default=False, compare=False)
    has_to_order_by_pk: bool = field(default=False, compare=False)
    source_relation_id: int = field(init=False, compare=True)
    destination_relation_id: int = field(init=False, compare=True)
    metrics: list["ETLLoadingMetric"] = field(default_factory=list)


@dataclass(unsafe_hash=True)
class RDBMSExcludedFromLoadingAttribute:
    attribute_id: int = field(init=False, hash=True, compare=True)
    loading_id: int = field(init=False, hash=True, compare=True)


class IncrementalLoadingMustHaveAnIncrementAttribute(ValueError):
    pass


@dataclass(unsafe_hash=True)
class ETLIncrementalLoading(ETLLoading):
    loading_id: int = field(init=False, compare=False, default_factory=int)
    is_overlapping: bool = field(default=False, compare=False)
    increment_attribute_id: int = field(hash=True, compare=True, default=None)

    def __post_init__(self):
        if self.increment_attribute_id is None:
            raise IncrementalLoadingMustHaveAnIncrementAttribute(
                "ETLIncrementalLoading has no attribute"
            )


@dataclass(eq=False)
class ETLLoadingMetric:
    id: int = field(init=False, compare=False)
    created_at: datetime = field(
        init=False, compare=False, default_factory=lambda: datetime.now(UTC)
    )
    etl_loading_id: int = field(init=False)
    # pipeline_id: int = field(init=False, compare=True)
    rows_set_hash: Optional[str] = field(default=None)
    records_count: Optional[int] = field(init=False, compare=False, default=None)
    increment_value: Optional[str] = field(init=False, default=None)


class ETLLoadingStatusType(Enum):
    CREATED = auto()
    IN_PROGRESS = auto()
    FAILED = auto()
    COMPLETED = auto()


@dataclass(eq=False)
class ETLLoadingStatus:
    id: int = field(init=False)
    created_at: datetime = field(init=False, default_factory=lambda: datetime.now(UTC))
    etl_loading_id: int = field(init=False)
    status: ETLLoadingStatusType = field()


@dataclass(unsafe_hash=True)
class ETLLoadingByChunksStrategy:
    chunk_size: int = field(compare=True)
    crushing_attribute_id: int = field(compare=True)
    etl_loading_id: int = field(compare=False, hash=True)
    chunks: list["ETLLoadingByChunksMetric"] = field(init=False, compare=False)


@dataclass(unsafe_hash=True)
class ETLLoadingByChunksMetric(ETLLoadingMetric):
    left_boundary: str = field(default=None, compare=False)
    right_boundary: str = field(default=None, compare=False)


@dataclass(unsafe_hash=True)
class DWHRelation:
    id: int = field(init=False, compare=False)
    fqn: str = field(compare=True, hash=True)
    has_rows_hash: bool = field(default=False, compare=False)
    has_pks_hash: bool = field(default=False, compare=False)
    has_pks_rn: bool = field(default=False, compare=False)
