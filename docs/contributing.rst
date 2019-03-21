.. _contributing:

==============
 Contributing
==============

Welcome!

This document is fairly extensive and you aren't really expected
to study this in detail for small contributions;

    The most important rule is that contributing must be easy
    and that the community is friendly and not nitpicking on details,
    such as coding style.

If you're reporting a bug you should read the Reporting bugs section
below to ensure that your bug report contains enough information
to successfully diagnose the issue, and if you're contributing code
you should try to mimic the conventions you see surrounding the code
you're working on, but in the end all patches will be cleaned up by
the person merging the changes so don't worry too much.

.. contents::
    :local:

.. _community-code-of-conduct:

.. include:: includes/code-of-conduct.txt

.. _reporting-bugs:

Reporting Bugs
==============

.. _vulnsec:

Security
--------

You must never report security related issues, vulnerabilities or bugs
including sensitive information to the bug tracker, or elsewhere in public.
Instead sensitive bugs must be sent by email to ``security@celeryproject.org``.

If you'd like to submit the information encrypted our PGP key is::

    -----BEGIN PGP PUBLIC KEY BLOCK-----
    Version: GnuPG v1.4.15 (Darwin)

    mQENBFJpWDkBCADFIc9/Fpgse4owLNvsTC7GYfnJL19XO0hnL99sPx+DPbfr+cSE
    9wiU+Wp2TfUX7pCLEGrODiEP6ZCZbgtiPgId+JYvMxpP6GXbjiIlHRw1EQNH8RlX
    cVxy3rQfVv8PGGiJuyBBjxzvETHW25htVAZ5TI1+CkxmuyyEYqgZN2fNd0wEU19D
    +c10G1gSECbCQTCbacLSzdpngAt1Gkrc96r7wGHBBSvDaGDD2pFSkVuTLMbIRrVp
    lnKOPMsUijiip2EMr2DvfuXiUIUvaqInTPNWkDynLoh69ib5xC19CSVLONjkKBsr
    Pe+qAY29liBatatpXsydY7GIUzyBT3MzgMJlABEBAAG0MUNlbGVyeSBTZWN1cml0
    eSBUZWFtIDxzZWN1cml0eUBjZWxlcnlwcm9qZWN0Lm9yZz6JATgEEwECACIFAlJp
    WDkCGwMGCwkIBwMCBhUIAgkKCwQWAgMBAh4BAheAAAoJEOArFOUDCicIw1IH/26f
    CViDC7/P13jr+srRdjAsWvQztia9HmTlY8cUnbmkR9w6b6j3F2ayw8VhkyFWgYEJ
    wtPBv8mHKADiVSFARS+0yGsfCkia5wDSQuIv6XqRlIrXUyqJbmF4NUFTyCZYoh+C
    ZiQpN9xGhFPr5QDlMx2izWg1rvWlG1jY2Es1v/xED3AeCOB1eUGvRe/uJHKjGv7J
    rj0pFcptZX+WDF22AN235WYwgJM6TrNfSu8sv8vNAQOVnsKcgsqhuwomSGsOfMQj
    LFzIn95MKBBU1G5wOs7JtwiV9jefGqJGBO2FAvOVbvPdK/saSnB+7K36dQcIHqms
    5hU4Xj0RIJiod5idlRC5AQ0EUmlYOQEIAJs8OwHMkrdcvy9kk2HBVbdqhgAREMKy
    gmphDp7prRL9FqSY/dKpCbG0u82zyJypdb7QiaQ5pfPzPpQcd2dIcohkkh7G3E+e
    hS2L9AXHpwR26/PzMBXyr2iNnNc4vTksHvGVDxzFnRpka6vbI/hrrZmYNYh9EAiv
    uhE54b3/XhXwFgHjZXb9i8hgJ3nsO0pRwvUAM1bRGMbvf8e9F+kqgV0yWYNnh6QL
    4Vpl1+epqp2RKPHyNQftbQyrAHXT9kQF9pPlx013MKYaFTADscuAp4T3dy7xmiwS
    crqMbZLzfrxfFOsNxTUGE5vmJCcm+mybAtRo4aV6ACohAO9NevMx8pUAEQEAAYkB
    HwQYAQIACQUCUmlYOQIbDAAKCRDgKxTlAwonCNFbB/9esir/f7TufE+isNqErzR/
    aZKZo2WzZR9c75kbqo6J6DYuUHe6xI0OZ2qZ60iABDEZAiNXGulysFLCiPdatQ8x
    8zt3DF9BMkEck54ZvAjpNSern6zfZb1jPYWZq3TKxlTs/GuCgBAuV4i5vDTZ7xK/
    aF+OFY5zN7ciZHkqLgMiTZ+RhqRcK6FhVBP/Y7d9NlBOcDBTxxE1ZO1ute6n7guJ
    ciw4hfoRk8qNN19szZuq3UU64zpkM2sBsIFM9tGF2FADRxiOaOWZHmIyVZriPFqW
    RUwjSjs7jBVNq0Vy4fCu/5+e+XLOUBOoqtM5W7ELt0t1w9tXebtPEetV86in8fU2
    =0chn
    -----END PGP PUBLIC KEY BLOCK-----

Other bugs
----------

Bugs can always be described to the :ref:`mailing-list`, but the best
way to report an issue and to ensure a timely response is to use the
issue tracker.

1) **Create a GitHub account**.

You need to `create a GitHub account`_ to be able to create new issues
and participate in the discussion.

.. _`create a GitHub account`: https://github.com/signup/free

2) **Determine if your bug is really a bug**.

You shouldn't file a bug if you're requesting support. For that you can use
the :ref:`mailing-list`, or :ref:`slack-channel`.

3) **Make sure your bug hasn't already been reported**.

Search through the appropriate Issue tracker. If a bug like yours was found,
check if you have new information that could be reported to help
the developers fix the bug.

4) **Check if you're using the latest version**.

A bug could be fixed by some other improvements and fixes - it might not have an
existing report in the bug tracker. Make sure you're using the latest release
of Faust.

5) **Collect information about the bug**.

To have the best chance of having a bug fixed, we need to be able to easily
reproduce the conditions that caused it. Most of the time this information
will be from a Python traceback message, though some bugs might be in design,
spelling or other errors on the website/docs/code.

    A) If the error is from a Python traceback, include it in the bug report.

    B) We also need to know what platform you're running (Windows, macOS, Linux,
       etc.), the version of your Python interpreter, and the version of Faust,
       and related packages that you were running when the bug occurred.

    C) If you're reporting a race condition or a deadlock, tracebacks can be
       hard to get or might not be that useful. Try to inspect the process to
       get more diagnostic data. Some ideas:

       * Collect tracing data using `strace`_(Linux),
         :command:`dtruss` (macOS), and :command:`ktrace` (BSD),
         `ltrace`_, and `lsof`_.

    D) Include the output from the :command:`faust report` command:

        .. sourcecode:: console

            $ faust -A proj report

        This will also include your configuration settings and it try to
        remove values for keys known to be sensitive, but make sure you also
        verify the information before submitting so that it doesn't contain
        confidential information like API tokens and authentication
        credentials.

6) **Submit the bug**.

By default `GitHub`_ will email you to let you know when new comments have
been made on your bug. In the event you've turned this feature off, you
should check back on occasion to ensure you don't miss any questions a
developer trying to fix the bug might ask.

.. _`GitHub`: https://github.com
.. _`strace`: https://en.wikipedia.org/wiki/Strace
.. _`ltrace`: https://en.wikipedia.org/wiki/Ltrace
.. _`lsof`: https://en.wikipedia.org/wiki/Lsof

.. _issue-trackers:

Issue Trackers
--------------

Bugs for a package in the Faust ecosystem should be reported to the relevant
issue tracker.

* :pypi:`Faust` - https://github.com/robinhood/faust/issues
* :pypi:`Mode` - https://github.com/ask/mode/issues

If you're unsure of the origin of the bug you can ask the
:ref:`mailing-list`, or just use the Faust issue tracker.

Contributors guide to the code base
===================================

There's a separate section for internal details,
including details about the code base and a style guide.

Read :ref:`developers-guide` for more!

.. _versions:

Versions
========

Version numbers consists of a major version, minor version and a release number.
Faust uses the versioning semantics described by SemVer: http://semver.org.

Stable releases are published at PyPI
while development releases are only available in the GitHub git repository as tags.
All version tags starts with “v”, so version 0.8.0 is the tag v0.8.0.

.. _git-branches:

Branches
========

Current active version branches:

* dev (which git calls "master") (https://github.com/robinhood/faust/tree/master)
* 1.0 (https://github.com/robinhood/faust/tree/1.0)

You can see the state of any branch by looking at the Changelog:

    https://github.com/robinhood/faust/blob/master/Changelog.rst

If the branch is in active development the topmost version info should
contain meta-data like:

.. sourcecode:: restructuredtext

    2.4.0
    ======
    :release-date: TBA
    :status: DEVELOPMENT
    :branch: dev (git calls this master)

The ``status`` field can be one of:

* ``PLANNING``

    The branch is currently experimental and in the planning stage.

* ``DEVELOPMENT``

    The branch is in active development, but the test suite should
    be passing and the product should be working and possible for users to test.

* ``FROZEN``

    The branch is frozen, and no more features will be accepted.
    When a branch is frozen the focus is on testing the version as much
    as possible before it is released.

dev branch
----------

The dev branch (called "master" by git), is where development of the next
version happens.

Maintenance branches
--------------------

Maintenance branches are named after the version -- for example,
the maintenance branch for the 2.2.x series is named ``2.2``.

Previously these were named ``releaseXX-maint``.

The versions we currently maintain is:

* 1.0

  This is the current series.

Archived branches
-----------------

Archived branches are kept for preserving history only,
and theoretically someone could provide patches for these if they depend
on a series that's no longer officially supported.

An archived version is named ``X.Y-archived``.

Our currently archived branches are:

We don't currently have any archived branches.

Feature branches
----------------

Major new features are worked on in dedicated branches.
There's no strict naming requirement for these branches.

Feature branches are removed once they've been merged into a release branch.

Tags
====

- Tags are used exclusively for tagging releases. A release tag is
  named with the format ``vX.Y.Z`` -- for example ``v2.3.1``.

- Experimental releases contain an additional identifier ``vX.Y.Z-id`` --
  for example ``v3.0.0-rc1``.

- Experimental tags may be removed after the official release.

.. _contributing-changes:

Working on Features & Patches
=============================

.. note::

    Contributing to Faust should be as simple as possible,
    so none of these steps should be considered mandatory.

    You can even send in patches by email if that's your preferred
    work method. We won't like you any less, any contribution you make
    is always appreciated!

    However following these steps may make maintainers life easier,
    and may mean that your changes will be accepted sooner.

Forking and setting up the repository
-------------------------------------

Create your fork
~~~~~~~~~~~~~~~~

First you need to fork the Faust repository, a good introduction to this
is in the GitHub Guide: `Fork a Repo`_.

After you have cloned the repository you should checkout your copy
to a directory on your machine:

.. sourcecode:: console

    $ git clone git@github.com:username/faust.git

When the repository is cloned enter the directory to set up easy access
to upstream changes:

.. sourcecode:: console

    $ cd faust
    $ git remote add upstream git://github.com/robinhood/faust.git
    $ git fetch upstream

If you need to pull in new changes from upstream you should
always use the ``--rebase`` option to ``git pull``:

.. sourcecode:: console

    $ git pull --rebase upstream master

With this option you don't clutter the history with merging
commit notes. See `Rebasing merge commits in git`_.
If you want to learn more about rebasing see the `Rebase`_
section in the GitHub guides.

Start Developing
~~~~~~~~~~~~~~~~

To start developing Faust you should install the requirements
and setup the development environment so that Python uses the Faust
development directory.

To do so run:

.. sourcecode:: console

    $ make develop


If you want to install requirements manually you should at least install
the git pre-commit hooks (the ``make develop`` command above automatically
runs this as well):

.. sourcecode:: console

    $ make hooks


If you also want to install C extensions, including the RocksDB bindings
then you can use `make cdevelop` instead of `make develop`:

.. sourcecode:: console

    $ make cdevelop

.. note::

    If you need to work on a different branch than the
    one git calls ``master``, you can
    fetch and checkout a remote branch like this:

    .. sourcecode:: console

        $ git checkout --track -b 2.0-devel origin/2.0-devel

.. _`Fork a Repo`: http://help.github.com/fork-a-repo/
.. _`Rebasing merge commits in git`:
    http://notes.envato.com/developers/rebasing-merge-commits-in-git/
.. _`Rebase`: http://help.github.com/rebase/

.. _contributing-testing:

Running the test suite
----------------------

To run the Faust test suite you need to install a few dependencies.
A complete list of the dependencies needed are located in
:file:`requirements/test.txt`.

Both the stable and the development version have testing related
dependencies, so install these:

.. sourcecode:: console

    $ pip install -U -r requirements/test.txt
    $ pip install -U -r requirements/default.txt

After installing the dependencies required, you can now execute
the test suite by calling :pypi:`py.test <pytest`:

.. sourcecode:: console

    $ py.test

This will run the unit tests, functional tests and doc example tests,
but not integration tests or stress tests.

Some useful options to :command:`py.test` are:

* ``-x``

    Stop running the tests at the first test that fails.

* ``-s``

    Don't capture output

* ``-v``

    Run with verbose output.

If you want to run the tests for a single test file only
you can do so like this:

.. sourcecode:: console

    $ py.test t/unit/test_app.py

.. _contributing-pull-requests:

Creating pull requests
----------------------

When your feature/bugfix is complete you may want to submit
a pull requests so that it can be reviewed by the maintainers.

Creating pull requests is easy, and also let you track the progress
of your contribution. Read the `Pull Requests`_ section in the GitHub
Guide to learn how this is done.

You can also attach pull requests to existing issues by following
the steps outlined here: http://bit.ly/koJoso

.. _`Pull Requests`: http://help.github.com/send-pull-requests/

.. _contributing-tox:

Running the tests on all supported Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There's a :pypi:`tox` configuration file in the top directory of the
distribution.

To run the tests for all supported Python versions simply execute:

.. sourcecode:: console

    $ tox

Use the ``tox -e`` option if you only want to test specific Python versions:

.. sourcecode:: console

    $ tox -e 2.7

Building the documentation
--------------------------

To build the documentation you need to install the dependencies
listed in :file:`requirements/docs.txt`:

.. sourcecode:: console

    $ pip install -U -r requirements/docs.txt

After these dependencies are installed you should be able to
build the docs by running:

.. sourcecode:: console

    $ cd docs
    $ rm -rf _build
    $ make html

Make sure there are no errors or warnings in the build output.
After building succeeds the documentation is available at :file:`_build/html`.

.. _contributing-verify:

Verifying your contribution
---------------------------

To use these tools you need to install a few dependencies. These dependencies
can be found in :file:`requirements/dist.txt`.

Installing the dependencies:

.. sourcecode:: console

    $ pip install -U -r requirements/dist.txt

pyflakes & PEP-8
~~~~~~~~~~~~~~~~

To ensure that your changes conform to :pep:`8` and to run pyflakes
execute:

.. sourcecode:: console

    $ make flakecheck

To not return a negative exit code when this command fails use
the ``flakes`` target instead:

.. sourcecode:: console

    $ make flakes

API reference
~~~~~~~~~~~~~

To make sure that all modules have a corresponding section in the API
reference please execute:

.. sourcecode:: console

    $ make apicheck
    $ make indexcheck

If files are missing you can add them by copying an existing reference file.

If the module is internal it should be part of the internal reference
located in :file:`docs/internals/reference/`. If the module is public
it should be located in :file:`docs/reference/`.

For example if reference is missing for the module ``faust.worker.awesome``
and this module is considered part of the public API, use the following steps:


Use an existing file as a template:

.. sourcecode:: console

    $ cd docs/reference/
    $ cp faust.schedules.rst faust.worker.awesome.rst

Edit the file using your favorite editor:

.. sourcecode:: console

    $ vim faust.worker.awesome.rst

        # change every occurrence of ``faust.schedules`` to
        # ``faust.worker.awesome``


Edit the index using your favorite editor:

.. sourcecode:: console

    $ vim index.rst

        # Add ``faust.worker.awesome`` to the index.


Commit your changes:

.. sourcecode:: console

    # Add the file to git
    $ git add faust.worker.awesome.rst
    $ git add index.rst
    $ git commit faust.worker.awesome.rst index.rst \
        -m "Adds reference for faust.worker.awesome"

Configuration Reference
-----------------------

To make sure that all settings have a corresponding section in the
configuration reference, please execute:

.. sourcecode:: console

    $ make configcheck

If settings are missing from there an error is produced, and you can proceed
by documenting the settings in :file:`docs/userguide/settings.rst`.

.. _coding-style:

Coding Style
============

You should probably be able to pick up the coding style
from surrounding code, but it is a good idea to be aware of the
following conventions.

* We use static types and the :pypi:`mypy` type checker to verify them.

  Python code must import these static types when using them, so to
  keep static types lightweight we define interfaces for
  classes in ``faust/types/``.

  For example for the :class:`fauts.App` class, there is a corresponding
  :class:`faust.types.app.AppT`; for :class:`faust.Channel` there is a
  :class:`faust.types.channels.ChannelT` and similarly for most other classes
  in the library.

  We suffer some duplication because of this, but it keeps static typing imports
  fast and reduces the need for recursive imports.

  In some cases recursive imports still happen, in that case you can "trick"
  the type checker into importing it, while regular Python does not::

    if typing.TYPE_CHECKING:
        from faust.app import App as _App
    else:
        class _App: ...  # noqa

 Note how we prefix the symbol with underscore to make sure anybody
 reading the code will think twice before using it.

* All Python code must follow the :pep:`8` guidelines.

:pypi:`pep8` is a utility you can use to verify that your code
is following the conventions.

* Docstrings must follow the :pep:`257` conventions, and use the following
  style.

    Do this:

    .. sourcecode:: python

        def method(self, arg: str) -> None:
            """Short description.

            More details.

            """

    or:

    .. sourcecode:: python

        def method(self, arg: str) -> None:
            """Short description."""


    but not this:

    .. sourcecode:: python

        def method(self, arg: str) -> None:
            """
            Short description.
            """

* Lines shouldn't exceed 78 columns.

  You can enforce this in :command:`vim` by setting the ``textwidth`` option:

  .. sourcecode:: vim

        set textwidth=78

  If adhering to this limit makes the code less readable, you have one more
  character to go on. This means 78 is a soft limit, and 79 is the hard
  limit :)

* Import order

    * Python standard library
    * Third-party packages.
    * Other modules from the current package.

    or in case of code using Django:

    * Python standard library (`import xxx`)
    * Third-party packages.
    * Django packages.
    * Other modules from the current package.

    Within these sections the imports should be sorted by module name.

    Example:

    .. sourcecode:: python

        import threading
        import time
        from collections import deque
        from Queue import Queue, Empty

        from .platforms import Pidfile
        from .five import zip_longest, items, range
        from .utils.time import maybe_timedelta

* Wild-card imports must not be used (`from xxx import *`).

.. _feature-with-extras:

Contributing features requiring additional libraries
====================================================

Some features like a new result backend may require additional libraries
that the user must install.

We use setuptools `extra_requires` for this, and all new optional features
that require third-party libraries must be added.

1) Add a new requirements file in `requirements/extras`

    For the RocksDB store this is
    :file:`requirements/extras/rocksdb.txt`, and the file looks like this:

    .. sourcecode:: text

        python-rocksdb

    These are pip requirement files so you can have version specifiers and
    multiple packages are separated by newline. A more complex example could
    be:

    .. sourcecode:: text

        # python-rocksdb 2.0 breaks Foo
        python-rocksdb>=1.0,<2.0
        thrift

2) Modify ``setup.py``

    After the requirements file is added you need to add it as an option
    to :file:`setup.py` in the ``EXTENSIONS`` section::

        EXTENSIONS = {
            'debug',
            'fast',
            'rocksdb',
            'uvloop',
        }


3) Document the new feature in :file:`docs/includes/installation.txt`

    You must add your feature to the list in the bundles section
    of :file:`docs/includes/installation.txt`.

    After you've made changes to this file you need to render
    the distro :file:`README` file:

    .. sourcecode:: console

        $ pip install -U requirements/dist.txt
        $ make readme


.. _contact_information:

Contacts
========

This is a list of people that can be contacted for questions
regarding the official git repositories, PyPI packages
Read the Docs pages.

If the issue isn't an emergency then it's better
to :ref:`report an issue <reporting-bugs>`.


Committers
----------

Ask Solem
~~~~~~~~~

:github: https://github.com/ask
:twitter: http://twitter.com/#!/asksol

Vineet Goel
~~~~~~~~~~~

:github: https://github.com/vineet-rh
:twitter: https://twitter.com/#!/vineetik

Arpan Shah
~~~~~~~~~~

:github: https://github.com/arpanshah29

.. _packages:

Packages
========

``Faust``
---------

:git: https://github.com/robinhood/faust
:CI: http://travis-ci.org/#!/robinhood/faust
:Windows-CI: https://ci.appveyor.com/project/ask/faust
:PyPI: :pypi:`faust`
:docs: https://faust.readthedocs.io

``Mode``
--------

:git: https://github.com/ask/mode
:CI: http://travis-ci.org/#!/ask/mode
:Windows-CI: https://ci.appveyor.com/project/ask/mode
:PyPI: :pypi:`Mode`
:docs: http://mode.readthedocs.io/

.. _release-procedure:

Release Procedure
=================

Updating the version number
---------------------------

The version number must be updated two places:

    * :file:`faust/__init__.py`
    * :file:`docs/include/introduction.txt`

After you have changed these files you must render
the :file:`README` files. There's a script to convert sphinx syntax
to generic reStructured Text syntax, and the make target `readme`
does this for you:

.. sourcecode:: console

    $ make readme

Now commit the changes:

.. sourcecode:: console

    $ git commit -a -m "Bumps version to X.Y.Z"

and make a new version tag:

.. sourcecode:: console

    $ git tag vX.Y.Z
    $ git push --tags

Releasing
---------

Commands to make a new public stable release:

.. sourcecode:: console

    $ make distcheck  # checks pep8, autodoc index, runs tests and more
    $ make dist  # NOTE: Runs git clean -xdf and removes files not in the repo.
    $ python setup.py sdist upload --sign --identity='Celery Security Team'
    $ python setup.py bdist_wheel upload --sign --identity='Celery Security Team'

If this is a new release series then you also need to do the
following:

* Go to the Read The Docs management interface at:
    http://readthedocs.org/projects/faust/?fromdocs=faust

* Enter "Edit project"

    Change default branch to the branch of this series, for example, use
    the ``1.0`` branch for the 1.0 series.

* Also add the previous version under the "versions" tab.
