from pathlib import Path
import pytest
from faust.cli.clean_versions import clean_versions
from mode.utils.mocks import Mock, call, patch


class test_clean_versions:

    @pytest.fixture()
    def command(self, *, context):
        return clean_versions(context)

    @pytest.mark.asyncio
    async def test_run(self, *, command):
        command.remove_old_versiondirs = Mock()
        await command.run()
        command.remove_old_versiondirs.assert_called_with()

    def test_remove_old_versiondirs(self, *, app, command):
        app.conf.find_old_versiondirs = Mock(return_value=[
            Path('A1'), Path('B2'), Path('C3'),
        ])
        with patch('faust.cli.clean_versions.rmtree') as rmtree:
            command.remove_old_versiondirs()
            rmtree.assert_has_calls([
                call('A1'),
                call('B2'),
                call('C3'),
            ])
