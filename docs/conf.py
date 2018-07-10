# -*- coding: utf-8 -*-
import os
import sys
from contextlib import suppress
from sphinx_celery import conf

if sys.version_info >= (3, 7):
    # Fixes bug in Sphinx 1.7.5 under CPython 3.7

    # -- RuntimeError: generator raised StopIteration
    from sphinx.ext import autodoc

    orig_process_doc = autodoc.Documenter.process_doc

    class Documenter(autodoc.Documenter):

        def process_doc(self, docstrings):
            try:
                for line in orig_process_doc(self, docstrings):
                    yield line
            except (StopIteration, RuntimeError):
                return
    autodoc.Documenter.process_doc = Documenter.process_doc

    # -- ForwardRef has no attribute __origin__

    from typing import ForwardRef
    from sphinx.util import inspect

    orig_format = inspect.Signature.format_annotation_new

    class Signature(inspect.Signature):

        def format_annotation_new(self, annotation):
            if isinstance(annotation, ForwardRef):
                return annotation.__forward_arg__
            return orig_format(self, annotation)
    inspect.Signature.format_annotation_new = Signature.format_annotation_new

extensions = []  # set by build_config
sys.path.append('.')

globals().update(conf.build_config(
    'faust', __file__,
    project='Faust',
    # version_dev='2.0',
    # version_stable='1.4',
    canonical_url='http://docs.fauststream.com',
    webdomain='',
    github_project='robinhood/faust',
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
        'click': ('http://click.pocoo.org/6/', None),
        'mode': ('https://mode.readthedocs.io/en/latest/', None),
        'mypy': ('https://mypy.readthedocs.io/en/latest/', None),
        'rocksdb': ('http://python-rocksdb.readthedocs.io/en/latest/', None),
        'terminaltables': ('https://robpol86.github.io/terminaltables/', None),
        'venusian': ('http://venusian.readthedocs.io/en/latest/', None),
        'yarl': ('http://yarl.readthedocs.io/en/latest/', None),
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
        'faust.transport.drivers.confluent',
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
    ],
}
html_theme_options = {
    'description': 'A library for building streaming applications in Python.',
    'github_banner': True,
    'travis_button': True,
    'show_related': True,
    'github_user': 'robinhood',
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
