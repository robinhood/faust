class test_Arena:

    def test_json(self, faust_json):
        model, stderr = faust_json('model', 'Arena')
        assert not stderr

        assert model == [
            ['points', 'typing.List[__main__.Point]', '*'],
            ['timestamp', 'float', 'None'],
        ]

    def test_tabulated(self, faust):
        stdout, stderr = faust('model', 'Arena')
        assert not stderr
        assert b'typing.List' in stdout

    def test_colors(self, faust_color):
        stdout, stderr = faust_color('model', 'Arena')
        assert not stderr
        assert b'typing.List' in stdout


class test_Point:

    def test_json(self, faust_json):
        model, stderr = faust_json('model', 'Point')
        assert not stderr

        assert model == [
            ['x', 'int', '*'],
            ['y', 'int', '*'],
        ]

    def test_tabulated(self, faust):
        stdout, stderr = faust('model', 'Point')
        assert not stderr
        assert b'int' in stdout

    def test_colors(self, faust_color):
        stdout, stderr = faust_color('model', 'Point')
        assert not stderr
        assert b'int' in stdout
