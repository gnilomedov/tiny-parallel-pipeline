import pytest


import tiny_parallel_pipeline as tpp


#
# Tests
#

class TestResourceStd:
    @staticmethod
    def _state(resource):
        return str(resource.id), resource.status.name, resource.data

    def test_file_resource(self):
        r = tpp.FileResource('bin', '/tmp/f')
        assert self._state(r) == ('FileResource:bin', 'EMPTY', None)
        assert self._state(tpp.FileResource('bin', '/tmp/f', is_ready=True)) == (
            'FileResource:bin', 'READY', '/tmp/f')
        assert r.populate_data('/tmp/f') is r
        with pytest.raises(AssertionError, match='/tmp/x != /tmp/f'):
            r.populate_data('/tmp/x')

    def test_txt_resource(self):
        assert self._state(tpp.TxtResource('info')) == ('TxtResource:info', 'EMPTY', None)
        assert self._state(tpp.TxtResource('info', 'txt')) == ('TxtResource:info', 'READY', 'txt')
        assert self._state(tpp.TxtResource('info', '')) == ('TxtResource:info', 'READY', '')

    def test_url_str_resource(self):
        assert self._state(tpp.UrlStrResource('url')) == ('UrlStrResource:url', 'EMPTY', None)
        assert self._state(tpp.UrlStrResource('url', 'http://x')) == (
            'UrlStrResource:url', 'READY', 'http://x')

    def test_same_in_class_id_stays_distinct(self):
        assert len({tpp.FileResource('same', '/tmp/f'), tpp.TxtResource('same'),
                    tpp.UrlStrResource('same')}) == 3
