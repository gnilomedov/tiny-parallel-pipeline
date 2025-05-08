import pytest
from sortedcontainers import SortedSet


import tiny_parallel_pipeline as tpp


# --- Test-specific subclass ---

class DummyResource(tpp.Resource):
    pass


# --- Fixtures ---

@pytest.fixture
def in_class_id():
    return '001'

@pytest.fixture
def resource(in_class_id):
    return DummyResource(in_class_id=in_class_id)


# --- Tests ---

class TestResourceBehavior:
    def test_resource_initialization(self, resource):
        assert resource.id.in_class_id == '001'
        assert resource.status == tpp.ResourceStatus.EMPTY
        assert resource.data is None
        assert isinstance(resource.id, tpp.ResourceID)
        assert str(resource.id) == 'DummyResource:001'

    def test_resource_update_status(self, resource):
        resource.update_status(tpp.ResourceStatus.IN_PROGRESS)
        assert resource.status == tpp.ResourceStatus.IN_PROGRESS

    def test_resource_populate_data(self, resource):
        resource.populate_data('Data example')
        assert resource.data == 'Data example'

    def test_resource_equality(self):
        r1 = DummyResource(in_class_id='test_resource_equality_A')
        r2 = DummyResource(in_class_id='test_resource_equality_A')
        r3 = DummyResource(in_class_id='test_resource_equality_B')
        assert r1 == r2
        assert r1 != r3

    def test_resource_comparison(self):
        r1 = DummyResource(in_class_id='test_resource_comparison_A')
        r2 = DummyResource(in_class_id='test_resource_comparison_B')
        assert r1 < r2
        assert r2 > r1
        assert not r1 > r2

    def test_resource_sortable(self):
        r1 = DummyResource(in_class_id='test_resource_sortable_Q')
        r2 = DummyResource(in_class_id='test_resource_sortable_W')
        r3 = DummyResource(in_class_id='test_resource_sortable_E')
        sorted_resources = sorted([r1, r2, r3])
        assert [str(r.id)[-1] for r in sorted_resources] == ['E', 'Q', 'W']

    def test_sorted_set(self):
        r1 = DummyResource(in_class_id='test_sorted_set_Q')
        r2 = DummyResource(in_class_id='test_sorted_set_W')
        r3 = DummyResource(in_class_id='test_sorted_set_E')
        r4 = DummyResource(in_class_id='test_sorted_set_W')

        sorted_resources = SortedSet()
        def sorted_set_short_list():
            return [str(r.id)[-1] for r in sorted_resources]

        sorted_resources.add(r1)
        assert sorted_set_short_list() == ['Q']
        sorted_resources.add(r2)
        sorted_resources.add(r3)
        assert sorted_set_short_list() == ['E', 'Q', 'W']
        sorted_resources.add(r4)
        assert sorted_set_short_list() == ['E', 'Q', 'W']

    def test_hash_dict(self):
        r1 = DummyResource(in_class_id='test_hash_set_X')
        r2 = DummyResource(in_class_id='test_hash_set_Y')

        resource_2_count = dict()
        resource_2_count[r1] = 0
        assert r1 in resource_2_count
        assert resource_2_count[r1] == 0
        assert r2 not in resource_2_count

        resource_2_count[r1] += 1
        assert resource_2_count[r1] == 1

        resource_2_count[r2] = 123
        assert resource_2_count[r2] == 123
        assert resource_2_count[DummyResource(in_class_id='test_hash_set_Y')] == 123
