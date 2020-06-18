#!/usr/bin/env python
import sys
from typing import Any, Iterator, List, Type
from faust.types.settings import Settings
from faust.types.settings.params import Param
from faust.types.settings.sections import Section

SECTION_TEMPLATE = '''\
.. _{section.refid}:

{title}

{settings}
'''

SETTING_TEMPLATE = '''\
.. setting:: {setting.name}

{title}

{metadata}

{content}

'''


class Rst:

    def to_ref(self, t: Type) -> str:
        name: str
        module = t.__module__
        if module == 'builtins':
            return self._class(t.__name__)
        elif module == 'typing':
            if t is Any:
                name = 'Any'
            else:
                name = getattr(t, '_name', None) or t.__name__
                if name == 'List':
                    list_type = t.__args__ and t.__args__[0] or Any
                    return ' '.join([
                        self.literal('['),
                        self.to_ref(list_type),
                        self.literal(']'),
                    ])
                elif name.startswith(('Dict', 'Mapping', 'MutableMapping')):
                    key_type = value_type = Any
                    if t.__args__:
                        key_type = t.__args__[0]
                    if len(t.__args__) > 1:
                        value_type = t.__args__[1]
                    return ' '.join([
                        self.literal('{'),
                        ': '.join([
                            self.to_ref(key_type),
                            self.to_ref(value_type),
                        ]),
                        self.literal('}'),
                    ])
        else:
            name = t.__name__

        return self._class(f'{module}.{name}')

    def header(self, sep: str, title: str) -> str:
        return '\n'.join([title, sep * len(title)])

    def header1(self, title: str) -> str:
        return self.header('=', title)

    def header2(self, title: str) -> str:
        return self.header('-', title)

    def header3(self, title: str) -> str:
        return self.header('~', title)

    def header4(self, title: str) -> str:
        return self.header('^', title)

    def ref(self, ref_class: str, value: str) -> str:
        return f':{ref_class}:`{value}`'

    def envvar(self, name: str) -> str:
        return self.ref('envvar', name)

    def const(self, value: str) -> str:
        return self.ref('const', value)

    def _class(self, value: str) -> str:
        if '.' in value:
            value = '~' + value
        return self.ref('class', value)

    def option(self, value: str) -> str:
        return self.ref('option', value)

    def literal(self, s: str) -> str:
        return f'``{s}``'

    def directive(self, name: str, value: str, content: str = None) -> str:
        res = f'.. {name}:: {value}\n'
        if content is not None:
            res += '\n' + self.reindent(8, content) + '\n'
        return res

    def inforow(self, name: str, value: str) -> str:
        return f':{name}: {value}'

    def normalize_docstring_indent(self, text: str) -> str:
        # docstring indentation starts at the second line
        return self.normalize_indent(text, line_start=1)

    def normalize_indent(self, text: str, line_start: int = 0) -> str:
        lines = text.splitlines()
        if len(lines) <= 1:
            return text
        # take indent to remove from second line,
        # since first line of docstring is not indented
        non_whitespace_index: int = 0
        # find first line with text in it that is not whitespace
        for line in lines[line_start:]:
            if line and not line.isspace():
                # find index of first non-whitespace character
                for i, c in enumerate(line):
                    if not c.isspace():
                        non_whitespace_index = i
                        break
                if non_whitespace_index:
                    break
        if not non_whitespace_index:
            return text
        return '\n'.join(
            self.strip_space(non_whitespace_index, line)
            for line in lines
        )

    def strip_space(self, n: int, line: str) -> str:
        sentinel = False
        result = []
        for i, c in enumerate(line):
            if not c.isspace() or i > n:
                sentinel = True
            if sentinel:
                result.append(c)
        return ''.join(result)

    def reindent(self, new_indent: int, text: str) -> str:
        return '\n'.join(
            ' ' * new_indent + line
            for line in self.normalize_docstring_indent(text).splitlines()
        )


class ConfigRef(Rst):

    def section(self, section: Section, settings: List[Param]) -> str:
        return SECTION_TEMPLATE.format(
            section=section,
            title=self.header1(section.title),
            settings=''.join(
                self.setting(setting) for setting in settings
                if not setting.deprecated
            ),
        )

    def setting(self, setting: Param) -> str:
        return SETTING_TEMPLATE.format(
            setting=setting,
            title=self.header2(self.literal(setting.name)),
            content=self.normalize_docstring_indent(setting.__doc__),
            metadata='\n'.join(self.setting_metadata(setting)),
        )

    def setting_default(self, default_value: None) -> str:
        if default_value is None:
            return self.const('None')
        elif default_value is True:
            return self.const('True')
        elif default_value is False:
            return self.const('False')
        return self.literal(repr(default_value))

    def setting_metadata(self, setting: Param) -> Iterator[str]:
        if setting.version_introduced:
            yield self.directive('versionadded', setting.version_introduced)
        if setting.version_changed:
            for version, reason in setting.version_changed.items():
                yield self.directive('versionchanged', version, reason)
        yield self.inforow('type', ' / '.join(
            self.to_ref(t) for t in setting.text_type))

        if setting.default_template:
            default_info_title = 'default (template)'
            default_value = self.setting_default(setting.default_template)
        elif setting.default_alias:
            default_info_title = 'default (alias to setting)'
            default_value = self.settingref(setting.default_alias)
        else:
            default_info_title = 'default'
            default_value = self.setting_default(setting.default)
        yield self.inforow(default_info_title, default_value)

        if setting.env_name:
            yield self.inforow(
                'environment', self.envvar(setting.env_name))

        if setting.related_cli_options:
            yield self.inforow(
                'related-command-options',
                ', '.join(
                    self.option(f'{command} {opt}')
                    for command, opts in setting.related_cli_options.items()
                    for opt in opts
                ),
            )
        if setting.related_settings:
            yield self.inforow(
                'related-settings',
                ', '.join(
                    self.settingref(setting.name)
                    for setting in setting.related_settings
                ),
            )

    def settingref(self, setting: str) -> str:
        return self.ref('setting', setting)


def render(fh=sys.stdout):
    configref = ConfigRef()
    for section, settings in Settings.SETTINGS_BY_SECTION.items():
        print(configref.section(section, settings), file=fh, end='')


if __name__ == '__main__':
    render()
