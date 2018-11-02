====================================
      Faust Requirement Files
====================================

+ ``default.txt``

    The requirements that are installed when you do ``pip install faust``.

    These are the core dependencies required for Faust to work.

+ ``extras/``

    Extra requirements for Faust features like RocksDB storage,
    transports, and so on.  These are added to ``setup.py`` so
    that you can do ``pip install faust[rocksdb]`` (see the
    installation guide in the documentation.)

+ ``docs.txt``

    Requirements necessary to build the documentation
    with ``make Documentation``.

+ ``test.txt``

    Requirements that are necessary to run the test suite.

+ ``ci.txt``

    Requirements that are necessary to run the continuous integration
    test suite at Travis CI.

+ ``typecheck.txt``

    Requirements that are necessary to run `make typecheck`.

+ ``dist.txt``

    Requirements that are necessary when developing Faust.
    F.example these are needed for ``make flakes`` and doing
    releases to PyPI.



