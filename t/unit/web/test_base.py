from faust.web.base import Request


class test_Request:

    def test_match_info(self):
        Request().match_info

    def test_query(self):
        Request().query
