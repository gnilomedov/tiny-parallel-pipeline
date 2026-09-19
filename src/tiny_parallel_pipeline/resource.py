"""Resources: the values a pipeline passes from one transition to the next."""


from abc import ABC
from dataclasses import dataclass, field, InitVar
from enum import Enum, auto
from functools import total_ordering
from typing import Any, Self


class ResourceStatus(Enum):
    """Where a resource is in its life."""
    EMPTY = auto()
    # Cached somewhere its transition can load it cheaply: still has to run, but reads nothing.
    LAZY_AVAILABLE = auto()
    IN_PROGRESS = auto()
    READY = auto()
    GARBAGE_COLLECTED = auto()
    FAILED = auto()


@total_ordering
@dataclass(frozen=True)
class ResourceID:
    """Identity of a resource: its class plus a name unique inside that class."""
    resource_class: type  # type of `class Resource`
    in_class_id: str

    def __hash__(self) -> int:
        return hash((self.resource_class, self.in_class_id))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ResourceID):
            return False
        return self.resource_class == other.resource_class and self.in_class_id == other.in_class_id

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ResourceID):
            return NotImplemented
        return (
            (self.resource_class.__name__, self.in_class_id) <
                (other.resource_class.__name__, other.in_class_id))

    def __str__(self) -> str:
        return f'{self.resource_class.__name__}:{self.in_class_id}'


@dataclass
class Resource(ABC):
    """.data: Any -- one value in the pipeline, written by one transition and read by later ones."""
    # e.g. `class MyResource(Resource):` this will be id in domain of MyResource
    # i.e. id of partucular instance of MyResource
    in_class_id: InitVar[str]

    id: ResourceID = field(init=False)
    status: ResourceStatus = ResourceStatus.EMPTY
    failed_reason: str | None = None
    data: Any = None
    garbage_collection_allowed: bool = True

    def __post_init__(self, in_class_id: str) -> None:
        object.__setattr__(self, 'id', ResourceID(type(self), in_class_id))

    def update_status(self, new_status: ResourceStatus,
                      failed_reason: str | None = None) -> Self:
        self.status = new_status
        self.failed_reason = failed_reason
        return self

    def populate_data(self, new_data: Any) -> Self:
        self.data = new_data
        return self

    def __repr__(self) -> str:
        return (f'<{self.__class__.__name__} id={self.id} '
                f'status={self.status.name} data={'set' if self.data is not None else 'empty'}>')

    def __str__(self) -> str:
        return repr(self)

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Resource):
            return False
        return self.id == other.id

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Resource):
            return NotImplemented
        return self.id < other.id
