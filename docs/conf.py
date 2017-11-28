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
    copyright='2017',
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
        'faust.assignor',
        'faust.cli',
        'faust.cli._env',
        'faust.models',
        'faust.serializers',
        'faust.streams',
        'faust.transport.confluent',
        'faust.types',
        'faust.utils',
        'faust.utils.avro',
        r'faust.utils.kafka.*',
        'faust.utils.types',
        'faust.web',
        r'faust.web.apps.*',
        'faust.web.apps.stats.app',
        'faust.web.apps.router.app',
        'faust'
        'faust.web.drivers',
    ],
))

html_theme = 'alabaster'
html_sidebars = {}
templates_path = ['_templates']

autodoc_member_order = 'bysource'

pygments_style = 'sphinx'

# This option is deprecated and raises an error.
with suppress(NameError):
    del(html_use_smartypants)  # noqa

if not os.environ.get('APICHECK'):
    extensions.append('sphinx_autodoc_annotation')

napoleon_use_keyword = True
