import sys
from pathlib import Path
import pytest

sys.path.append(str(Path(__file__).parent))


@pytest.fixture()
def app():
    from proj323 import faust
    faust.app = faust.create_app()
    return faust.app
