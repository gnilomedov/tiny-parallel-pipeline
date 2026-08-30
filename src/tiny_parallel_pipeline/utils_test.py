from tiny_parallel_pipeline import utils


#
# Tests
#

class TestTrimList:
    def test_short_enough_stays_verbatim(self):
        assert utils.trim_list(123) == '123'
        assert utils.trim_list([]) == '[]'
        assert utils.trim_list([123]) == '[123]'
        assert utils.trim_list(list(range(6))) == '[0, 1, 2, 3, 4, 5]'

    def test_long_list_keeps_both_ends(self):
        assert utils.trim_list(list(range(7))) == '[0, 1, 2 ... 4, 5, 6]'
        assert utils.trim_list(list(range(100)), edge=2) == '[0, 1 ... 98, 99]'

    def test_max_len_caps_the_result(self):
        long_list = list(range(1_000, 1_020))
        assert utils.trim_list(long_list) == '[1000, 1001, 1002 ... 1017, 1018, 1019]'
        assert utils.trim_list(long_list, max_len=20) == '[1000, 1~...~, 1019]'
        assert len(utils.trim_list(long_list, max_len=20)) == 20

    def test_non_list_falls_back_to_text(self):
        assert utils.trim_list('123456789', max_len=8) == '1~...~89'
        assert utils.trim_list(None) == 'None'


class TestTrimText:
    def test_short_enough_stays_verbatim(self):
        assert utils.trim_text('123456789', max_len=9) == '123456789'
        assert utils.trim_text('') == ''

    def test_middle_is_cut_out(self):
        assert utils.trim_text('0123456789', max_len=9) == '01~...~89'
        assert len(utils.trim_text('0123456789', max_len=9)) == 9

    def test_max_len_below_the_cut_marker(self):
        assert utils.trim_text('0123456789', max_len=1) == '~...~'

    def test_non_text_is_repred(self):
        assert utils.trim_text(None) == 'None'
        assert utils.trim_text(123) == '123'
