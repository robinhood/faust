#!/usr/bin/env python
import re
from pathlib import Path
from setuptools import find_packages, setup

NAME = 'proj'

README = Path('README.rst')

# -*- Classifiers -*-

classes = """
    Development Status :: 4 - Beta
    License :: OSI Approved :: BSD License
    Programming Language :: Python
    Programming Language :: Python :: 3 :: Only
    Programming Language :: Python :: 3.6
    Operating System :: POSIX
    Operating System :: POSIX :: Linux
    Operating System :: MacOS :: MacOS X
    Operating System :: POSIX :: BSD
    Operating System :: Microsoft :: Windows
    Topic :: Framework :: Django
"""
classifiers = [s.strip() for s in classes.split('\n') if s]

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


def _pip_requirement(req):
    if req.startswith('-r '):
        _, path = req.split()
        return reqs(*path.split('/'))
    return [req]


def _reqs(*f):
    path = (Path.cwd() / 'requirements').joinpath(*f)
    with path.open() as fh:
        reqs = [strip_comments(l) for l in fh.readlines()]
        return [_pip_requirement(r) for r in reqs if r]


def reqs(*f):
    return [req for subreq in _reqs(*f) for req in subreq]

# -*- Long Description -*-


if README.exists():
    long_description = README.read_text(encoding='utf-8')
else:
    long_description = 'See http://pypi.org/project/{}'.format(NAME)

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
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    include_package_data=True,
    python_requires='>=3.6.0',
    keywords=[],
    zip_safe=False,
    install_requires=reqs('default.txt'),
    tests_require=reqs('test.txt'),
    classifiers=classifiers,
    long_description=long_description,
    entry_points={
        'console_scripts': [
            'proj = proj.__main__:main',
            'proj-faust = faustproj.app:main',
        ],
    },
)
