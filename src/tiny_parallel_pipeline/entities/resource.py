from abc import ABC
from dataclasses import dataclass, field, InitVar
from enum import Enum, auto
from functools import total_ordering


class ResourceStatus(Enum):
    EMPTY = auto()
    IN_PROGRESS = auto()
    READY = auto()


@total_ordering
@dataclass(frozen=True)
class ResourceID:
    resource_cls: type
    in_class_id: str

    def __hash__(self):
        return hash((self.resource_cls, self.in_class_id))

    def __eq__(self, other):
        if not isinstance(other, ResourceID):
            return False
        return self.resource_cls == other.resource_cls and self.in_class_id == other.in_class_id

    def __lt__(self, other):
        if not isinstance(other, ResourceID):
            return NotImplemented
        return (
            (self.resource_cls.__name__, self.in_class_id) <
                (other.resource_cls.__name__, other.in_class_id))

    def __str__(self):
        return f'{self.resource_cls.__name__}:{self.in_class_id}'


@dataclass
class Resource(ABC):
    in_class_id: InitVar[str]

    status: ResourceStatus = ResourceStatus.EMPTY
    data: any = None
    id: ResourceID = field(init=False)

    def __post_init__(self, in_class_id: str):
        object.__setattr__(self, 'id', ResourceID(type(self), in_class_id))

    def update_status(self, new_status: ResourceStatus):
        'Update the resource status'
        self.status = new_status
        return self

    def populate_data(self, new_data: any):
        'Populate or update the data'
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
