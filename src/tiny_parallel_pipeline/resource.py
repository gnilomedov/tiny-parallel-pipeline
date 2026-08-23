"""Resources: the values a pipeline passes from one transition to the next."""


from abc import ABC
from dataclasses import dataclass, field, InitVar
from enum import Enum, auto
from functools import total_ordering


class ResourceStatus(Enum):
    """Where a resource is in its life: EMPTY, IN_PROGRESS, READY or FAILED."""
    EMPTY = auto()
    IN_PROGRESS = auto()
    READY = auto()
    FAILED = auto()


@total_ordering
@dataclass(frozen=True)
class ResourceID:
    """Identity of a resource: its class plus a name unique inside that class."""
    resource_class: type  # type of `class Resource`
    in_class_id: str

    def __hash__(self):
        return hash((self.resource_class, self.in_class_id))

    def __eq__(self, other):
        if not isinstance(other, ResourceID):
            return False
        return self.resource_class == other.resource_class and self.in_class_id == other.in_class_id

    def __lt__(self, other):
        if not isinstance(other, ResourceID):
            return NotImplemented
        return (
            (self.resource_class.__name__, self.in_class_id) <
                (other.resource_class.__name__, other.in_class_id))

    def __str__(self):
        return f'{self.resource_class.__name__}:{self.in_class_id}'


@dataclass
class Resource(ABC):
    """One value in the pipeline. One transition writes it, later ones read it."""
    # e.g. `class MyResource(Resource):` this will be id in domain of MyResource
    # i.e. id of partucular instance of MyResource
    in_class_id: InitVar[str]

    id: ResourceID = field(init=False)
    status: ResourceStatus = ResourceStatus.EMPTY
    failed_reason: str | None = None
    data: any = None

    def __post_init__(self, in_class_id: str):
        object.__setattr__(self, 'id', ResourceID(type(self), in_class_id))

    def update_status(self, new_status: ResourceStatus, failed_reason: str | None = None):
        self.status = new_status
        self.failed_reason = failed_reason
        return self

    def populate_data(self, new_data: any):
        self.data = new_data
        return self

    def __repr__(self):
        return (f'<{self.__class__.__name__} id={self.id} '
                f'status={self.status.name} data={'set' if self.data is not None else 'empty'}>')

    def __str__(self):
        return repr(self)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if not isinstance(other, Resource):
            return False
        return self.id == other.id

    def __lt__(self, other):
        if not isinstance(other, Resource):
            return NotImplemented
        return self.id < other.id
