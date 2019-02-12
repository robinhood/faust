import pytest
from mode.utils.mocks import Mock


@pytest.fixture()
def context(*, app, request):
    context = Mock()
    context.app = app
    context.find_root.return_value = context
    return context
