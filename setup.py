#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
try:
    import platform
    _pyimp = platform.python_implementation
except (AttributeError, ImportError):
    def _pyimp():
        return 'Python'
from setuptools import find_packages, setup

NAME = 'faust'
EXTENSIONS = {
    'ckafka',
    'datadog',
    'debug',
    'fast',
    'rocksdb',
    'setproctitle',
    'statsd',
    'uvloop',
    'gevent',
    'eventlet',
}
E_UNSUPPORTED_PYTHON = '%s 1.0 requires %%s %%s or later!' % (NAME,)

PYIMP = _pyimp()
if sys.version_info < (3, 6):
    raise Exception(E_UNSUPPORTED_PYTHON % (PYIMP, '3.6'))

from pathlib import Path  # noqa

README = Path('README.rst')

# -*- Distribution Meta -*-

re_meta = re.compile(r'__(\w+?)__\s*=\s*(.*)')
re_doc = re.compile(r'^"""(.+?)"""')


def add_default(m):
    attr_name, attr_value = m.groups()
    return ((attr_name, attr_value.strip("\"'")),)


def add_doc(m):
    return (('doc', m.groups()[0]),)


pats = {re_meta: add_default, re_doc: add_doc}
here = Path(__file__).parent.absolute()
with open(here / NAME / '__init__.py') as meta_fh:
    meta = {}
    for line in meta_fh:
        if line.strip() == '# -eof meta-':
            break
        for pattern, handler in pats.items():
            m = pattern.match(line.strip())
            if m:
                meta.update(handler(m))

# -*- Installation Requires -*-


def strip_comments(l):
    return l.split('#', 1)[0].strip()


def _pip_requirement(req, *root):
    if req.startswith('-r '):
        _, path = req.split()
        return reqs(*root, *path.split('/'))
    return [req]


def _reqs(*f):
    path = (Path.cwd() / 'requirements').joinpath(*f)
    with path.open() as fh:
        reqs = [strip_comments(l) for l in fh.readlines()]
        return [_pip_requirement(r, *f[:-1]) for r in reqs if r]


def reqs(*f):
    return [req for subreq in _reqs(*f) for req in subreq]


def extras(*p):
    """Parse requirement in the requirements/extras/ directory."""
    return reqs('extras', *p)


def extras_require():
    """Get map of all extra requirements."""
    return {x: extras(x + '.txt') for x in EXTENSIONS}


# -*- Long Description -*-


if README.exists():
    long_description = README.read_text(encoding='utf-8')
else:
    long_description = 'See http://pypi.org/project/{}'.format(NAME)

# -*- Install Requires -*-


install_requires = reqs('default.txt')
if sys.version_info < (3, 7):
    install_requires += reqs('py36.txt')

# -*- %%% -*-

setup(
    name=NAME,
    version=meta['version'],
    description=meta['doc'],
    author=meta['author'],
    author_email=meta['contact'],
    url=meta['homepage'],
    platforms=['any'],
    license='BSD',
    packages=find_packages(exclude=['examples', 'ez_setup', 't', 't.*']),
    # PEP-561: https://www.python.org/dev/peps/pep-0561/
    package_data={'faust': ['py.typed']},
    include_package_data=True,
    python_requires='>=3.6.0',
    keywords=[
        'stream',
        'processing',
        'asyncio',
        'distributed',
        'queue',
        'kafka',
    ],
    zip_safe=False,
    install_requires=install_requires,
    tests_require=reqs('test.txt'),
    extras_require=extras_require(),
    long_description=long_description,
    entry_points={
        'console_scripts': [
            'faust = faust.cli.faust:cli',
        ],
    },
    project_urls={
        'Bug Reports': 'https://github.com/robinhood/faust/issues',
        'Source': 'https://github.com/robinhood/faust',
        'Documentation': 'https://faust.readthedocs.io/',
    },
)
