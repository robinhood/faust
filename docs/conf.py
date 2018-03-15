# -*- coding: utf-8 -*-
import os
import sys
from contextlib import suppress
from sphinx_celery import conf

extensions = []  # set by build_config
sys.path.append('.')

globals().update(conf.build_config(
    'faust', __file__,
    project='Faust',
    # version_dev='2.0',
    # version_stable='1.4',
    canonical_url='http://docs.fauststream.com',
    webdomain='',
    github_project='fauststream/faust',
    copyright='2017-2018',
    html_logo='images/logo.png',
    html_favicon='images/favicon.ico',
    html_prepend_sidebars=[],
    include_intersphinx={'python', 'sphinx'},
    extra_extensions=[
        'sphinx.ext.napoleon',
        'sphinxcontrib.asyncio',
        'alabaster',
        'typehints',
        'faustdocs',
    ],
    extra_intersphinx_mapping={
        'aiohttp': ('https://aiohttp.readthedocs.io/en/stable/', None),
        'aiokafka': ('https://aiokafka.readthedocs.io/en/stable/', None),
        'mode': ('https://mode.readthedocs.io/en/latest/', None),
    },
    # django_settings='testproj.settings',
    # from pathlib import Path
    # path_additions=[Path.cwd().parent / 'testproj']
    apicheck_ignore_modules=[
        'faust.__main__',
        'faust.app._attached',
        'faust.assignor',
        'faust.cli',
        'faust.cli._env',
        'faust.models',
        'faust.serializers',
        'faust.transport.confluent',
        'faust.types',
        'faust.utils',
        'faust.utils._iso8601_python',
        r'faust.utils.kafka.*',
        'faust.web',
        r'faust.web.apps.*',
        'faust.web.apps.stats.app',
        'faust.web.apps.router.app',
        'faust'
        'faust.web.drivers',
    ],
))


def configcheck_project_settings():
    from faust import Settings
    return Settings.setting_names()


html_theme = 'alabaster'
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'relations.html',
        'searchbox.html',
    ]
}
html_theme_options = {
    #'logo': 'logo.png',
    'description': 'A library for building streaming applications in Python with a focus on simplicity.',
    'github_banner': True,
    'travis_button': True,
    'show_related': True,
    'github_user': 'fauststream',
    'pre_bg': '#4c4c4c',
    'github_repo': 'faust',
}
templates_path = ['_templates']

autodoc_member_order = 'bysource'

pygments_style = 'monokai'

# This option is deprecated and raises an error.
with suppress(NameError):
    del(html_use_smartypants)  # noqa

if not os.environ.get('APICHECK'):
    extensions.append('sphinx_autodoc_annotation')

napoleon_use_keyword = True

applehelp_bundle_id = 'Faust'
epub_identifier = 'Faust'
latex_elements = {
    'inputenc': '',
    'utf8extra': '',
    'preamble': r'''

\usepackage{fontspec}
\setsansfont{Arial}
\setromanfont{Arial}
\setmonofont{DejaVu Sans Mono}
''',
}
