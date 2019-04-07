#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
import sys
from setuptools import Extension, find_packages, setup
from distutils.command.build_ext import build_ext
from distutils.errors import (
    CCompilerError,
    DistutilsExecError,
    DistutilsPlatformError,
)

try:
    from Cython.Build import cythonize
except ImportError:
    USE_CYTHON = False
else:
    USE_CYTHON = os.environ.get('USE_CYTHON', True)


if os.environ.get('NO_CYTHON'):
    USE_CYTHON = False

NAME = 'faust'
BUNDLES = {
    'aiodns',
    'aiomonitor',
    'cchardet',
    'ckafka',
    'ciso8601',
    'cython',
    'datadog',
    'debug',
    'fast',
    'redis',
    'rocksdb',
    'setproctitle',
    'statsd',
    'uvloop',
    'gevent',
    'eventlet',
}
CFLAGS = ['-O2']
LDFLAGS = []
LIBRARIES = []
E_UNSUPPORTED_PYTHON = NAME + ' 1.0 requires Python %%s or later!'

if sys.version_info < (3, 6):
    raise Exception(E_UNSUPPORTED_PYTHON % ('3.6',))  # NOQA

from pathlib import Path  # noqa

README = Path('README.rst')

# -*- Compiler Flags -*-

if sys.platform == 'win32':
    LDFLAGS.append('ws2_32.lib')
else:
    CFLAGS.extend(['-Wall', '-Wsign-compare', '-Wconversion'])
    LIBRARIES.append('z')

# -*- C Extensions -*-
ext = '.pyx' if USE_CYTHON else '.c'

extensions = [
    Extension(
        'faust._cython.windows',
        ['faust/_cython/windows' + ext],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
    Extension(
        'faust._cython.streams',
        ['faust/_cython/streams' + ext],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
    Extension(
        'faust.transport._cython.conductor',
        ['faust/transport/_cython/conductor' + ext],
        libraries=LIBRARIES,
        extra_compile_args=CFLAGS,
        extra_link_args=LDFLAGS,
    ),
]


if USE_CYTHON:
    print('---*--- USING CYTHON ---*---')
    extensions = cythonize(extensions)


class BuildFailed(Exception):
    pass


class ve_build_ext(build_ext):
    # This class allows C extension building to fail.

    def run(self):
        try:
            build_ext.run(self)
        except (DistutilsPlatformError, FileNotFoundError):
            raise BuildFailed()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except (CCompilerError, DistutilsExecError,
                DistutilsPlatformError, ValueError):
            raise BuildFailed()


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
    return {x: extras(x + '.txt') for x in BUNDLES}


# -*- Long Description -*-


if README.exists():
    long_description = README.read_text(encoding='utf-8')
else:
    long_description = 'See http://pypi.org/project/{}'.format(NAME)

# -*- %%% -*-


def do_setup(**kwargs):
    setup(
        name=NAME,
        version=meta['version'],
        description=meta['doc'],
        long_description=long_description,
        long_description_content_type='text/x-rst',
        author=meta['author'],
        author_email=meta['contact'],
        url=meta['homepage'],
        platforms=['any'],
        license='BSD 3-Clause',
        packages=find_packages(exclude=['examples', 'ez_setup', 't', 't.*']),
        # PEP-561: https://www.python.org/dev/peps/pep-0561/
        package_data={'faust': ['py.typed']},
        include_package_data=True,
        python_requires='>=3.6.0',
        zip_safe=False,
        install_requires=reqs('default.txt'),
        tests_require=reqs('test.txt'),
        extras_require=extras_require(),
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
        keywords=[
            'stream',
            'processing',
            'asyncio',
            'distributed',
            'queue',
            'kafka',
        ],
        classifiers=[
            'Framework :: AsyncIO',
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'Natural Language :: English',
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3 :: Only',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy',
            'Operating System :: POSIX',
            'Operating System :: POSIX :: Linux',
            'Operating System :: MacOS :: MacOS X',
            'Operating System :: POSIX :: BSD',
            'Operating System :: Microsoft :: Windows',
            'Topic :: System :: Networking',
            'Topic :: System :: Distributed Computing',
        ],
        **kwargs)


try:
    do_setup(cmdclass={'build_ext': ve_build_ext},
             ext_modules=extensions)
except BuildFailed:
    print('************************************************************')
    print('Cannot compile C accelerated modules, using pure python')
    print('************************************************************')
    do_setup()
