=====================================================================
 Stream processing
=====================================================================

|build-status| |coverage| |license| |wheel| |pyversion| |pyimp|

:Version: 1.0.0
:Web: http://faust.readthedocs.org/
:Download: http://pypi.python.org/pypi/faust
:Source: http://github.com/robinhoodmarkets/faust
:Keywords: KEYWORDS

About
=====

# I'm too lazy to edit the defaults.

.. _installation:

Installation
============

You can install faust either via the Python Package Index (PyPI)
or from source.

To install using `pip`,::

    $ pip install -U faust

.. _installing-from-source:

Downloading and installing from source
--------------------------------------

Download the latest version of faust from
http://pypi.python.org/pypi/faust

You can install it by doing the following,::

    $ tar xvfz faust-0.0.0.tar.gz
    $ cd faust-0.0.0
    $ python setup.py build
    # python setup.py install

The last command must be executed as a privileged user if
you are not currently using a virtualenv.

.. _installing-from-git:

Using the development version
-----------------------------

With pip
~~~~~~~~

You can install the latest snapshot of faust using the following
pip command::

    $ pip install https://github.com/robinhoodmarkets/faust/zipball/master#egg=faust

.. |build-status| image:: https://secure.travis-ci.org/robinhoodmarkets/faust.png?branch=master
    :alt: Build status
    :target: https://travis-ci.org/robinhoodmarkets/faust

.. |coverage| image:: https://codecov.io/github/robinhoodmarkets/faust/coverage.svg?branch=master
    :target: https://codecov.io/github/robinhoodmarkets/faust?branch=master

.. |license| image:: https://img.shields.io/pypi/l/faust.svg
    :alt: BSD License
    :target: https://opensource.org/licenses/BSD-3-Clause

.. |wheel| image:: https://img.shields.io/pypi/wheel/faust.svg
    :alt: faust can be installed via wheel
    :target: http://pypi.python.org/pypi/faust/

.. |pyversion| image:: https://img.shields.io/pypi/pyversions/faust.svg
    :alt: Supported Python versions.
    :target: http://pypi.python.org/pypi/faust/

.. |pyimp| image:: https://img.shields.io/pypi/implementation/faust.svg
    :alt: Support Python implementations.
    :target: http://pypi.python.org/pypi/faust/

