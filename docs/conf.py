# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import os

from sphinx_celery import conf

globals().update(conf.build_config(
    'faust', __file__,
    project='Faust',
    # version_dev='2.0',
    # version_stable='1.4',
    canonical_url='http://faust.readthedocs.org',
    webdomain='',
    github_project='robinhoodmarkets/faust',
    copyright='2016',
    html_logo='images/logo.png',
    html_favicon='images/favicon.ico',
    html_prepend_sidebars=[],
    include_intersphinx={'python', 'sphinx'},
    # django_settings='testproj.settings',
    # path_additions=[os.path.join(os.pardir, 'testproj')],
    # apicheck_ignore_modules=[
    #   'faust',
    # ],
))
