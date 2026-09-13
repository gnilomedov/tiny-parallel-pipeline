import pytest
from sortedcontainers import SortedSet


import tiny_parallel_pipeline as tpp


#
# Test-specific subclass
#

class DummyResource(tpp.Resource):
    pass


#
# Fixtures
#

@pytest.fixture
def in_class_id():
    return '001'

@pytest.fixture
def resource(in_class_id):
    return DummyResource(in_class_id=in_class_id)


#
# Tests
#

class TestResourceBehavior:
    @staticmethod
    def _resources(*in_class_ids):
        return [DummyResource(in_class_id=i) for i in in_class_ids]

    @staticmethod
    def _short_ids(resources):
        return [str(r.id)[-1] for r in resources]

    def test_resource_initialization(self, resource):
        assert resource.id.in_class_id == '001'
        assert resource.status == tpp.ResourceStatus.EMPTY
        assert resource.data is None
        assert isinstance(resource.id, tpp.ResourceID)
        assert str(resource.id) == 'DummyResource:001'

    def test_resource_setters_are_fluent(self):
        r = DummyResource(in_class_id='001')

        assert r.update_status(tpp.ResourceStatus.IN_PROGRESS).populate_data('Data example') is r
        assert (r.status, r.data) == (tpp.ResourceStatus.IN_PROGRESS, 'Data example')

    def test_resource_equality(self):
        r1, r2, r3 = self._resources('A', 'A', 'B')
        assert r1 == r2
        assert r1 != r3

    def test_resource_ordering(self):
        r1, r2 = self._resources('A', 'B')
        assert r1 < r2 and r2 > r1 and not r1 > r2
        assert self._short_ids(sorted(self._resources('Q', 'W', 'E'))) == ['E', 'Q', 'W']

    def test_sorted_set(self):
        r1, r2, r3, r4 = self._resources('Q', 'W', 'E', 'W')
        sorted_resources = SortedSet()

        sorted_resources.add(r1)
        assert self._short_ids(sorted_resources) == ['Q']
        sorted_resources.add(r2)
        sorted_resources.add(r3)
        assert self._short_ids(sorted_resources) == ['E', 'Q', 'W']
        sorted_resources.add(r4)
        assert self._short_ids(sorted_resources) == ['E', 'Q', 'W']

    def test_hash_dict(self):
        r1, r2 = self._resources('X', 'Y')

        resource_2_count = dict()
        resource_2_count[r1] = 0
        assert r1 in resource_2_count
        assert resource_2_count[r1] == 0
        assert r2 not in resource_2_count

        resource_2_count[r1] += 1
        assert resource_2_count[r1] == 1

        resource_2_count[r2] = 123
        assert resource_2_count[r2] == 123
        assert resource_2_count[DummyResource(in_class_id='Y')] == 123
