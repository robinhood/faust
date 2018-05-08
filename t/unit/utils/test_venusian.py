from faust.utils import venusian
from mode.utils.mocks import Mock, patch


def test_attach():
    callable = Mock(name='callable')
    category = 'category'
    with patch('faust.utils.venusian._attach') as _attach:
        with patch('faust.utils.venusian._on_found') as _on_found:
            venusian.attach(callable, category)
            _attach.assert_called_with(callable, _on_found, category=category)


def test_on_found():
    venusian._on_found(Mock(name='scanner'), 'name', Mock(name='obj'))
