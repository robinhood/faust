PROJ ?= faust
PGPIDENT? = "Celery Security Team"
PYTHON ?= python
PYTEST ?= py.test
PIP ?= pip
GIT ?= git
TOX ?= tox
NOSETESTS ?= nosetests
ICONV ?= iconv
FLAKE8 ?= flake8
PYDOCSTYLE ?= pydocstyle
MYPY ?= mypy
SPHINX2RST ?= sphinx2rst
BUMPVERSION ?= bumpversion
VULTURE ?= vulture
VULTURE_MIN_CONFIDENCE=100
PRE_COMMIT=pre-commit

TESTDIR ?= t
EXAMPLESDIR ?= examples
SPHINX_DIR ?= docs/
SPHINX_BUILDDIR ?= "${SPHINX_DIR}/_build"
README ?= README.rst
README_SRC ?= "docs/templates/readme.txt"
CONTRIBUTING ?= CONTRIBUTING.rst
CONTRIBUTING_SRC ?= "docs/contributing.rst"
COC ?= .github/CODE_OF_CONDUCT.md
COC_SRC ?= "docs/includes/code-of-conduct.txt"
SPHINX_HTMLDIR ?= "${SPHINX_BUILDDIR}/html"
DOCUMENTATION ?= Documentation

all: help

help:
	@echo "docs                 - Build documentation."
	@echo "livedocs             - Start documentation live web server."
	@echo "develop              - Start contributing to Faust"
	@echo "  develop-hooks      - Install Git commit hooks (required)"
	@echo "  reqs"              - Install requirements
	@echo "  setup-develop      - Run setup.py develop"
	@echo "cdevelop             - Like develop but installs C extensions"
	@echo "  reqs-rocksdb       -   Install python-rocksdb (require rocksdb)"
	@echo "  reqs-fast          -   Install C optimizations"
	@echo "  reqs-uvloop        -   Install uvloop extension"
	@echo "test-all             - Run tests for all supported python versions."
	@echo "distcheck ---------- - Check distribution for problems."
	@echo "  test               - Run unittests using current python."
	@echo "  lint ------------  - Check codebase for problems."
	@echo "    apicheck         - Check API reference coverage."
	@echo "    configcheck      - Check configuration reference coverage."
	@echo "    readmecheck      - Check README.rst encoding."
	@echo "    contribcheck     - Check CONTRIBUTING.rst encoding"
	@echo "    flakes --------  - Check code for syntax and style errors."
	@echo "      flakecheck     - Run flake8 on the source code."
	@echo "    typecheck        - Run the mypy type checker"
	@echo "    pep257check      - Run pep257 on the source code."
	@echo "    vulture          - Run vulture to find unused code."
	@echo "readme               - Regenerate README.rst file."
	@echo "contrib              - Regenerate CONTRIBUTING.rst file"
	@echo "coc                  - Regenerate CODE_OF_CONDUCT.rst file"
	@echo "clean-dist --------- - Clean all distribution build artifacts."
	@echo "  clean-git-force    - Remove all uncomitted files."
	@echo "  clean ------------ - Non-destructive clean"
	@echo "    clean-pyc        - Remove .pyc/__pycache__ files"
	@echo "    clean-docs       - Remove documentation build artifacts."
	@echo "    clean-build      - Remove setup artifacts."
	@echo "bump                 - Bump patch version number."
	@echo "bump-minor           - Bump minor version number."
	@echo "bump-major           - Bump major version number."
	@echo "hooks"               - Update pre-commit hooks
	@echo "release              - Make PyPI release."

clean: clean-docs clean-pyc clean-build

clean-dist: clean clean-git-force

bump:
	$(BUMPVERSION) patch

bump-minor:
	$(BUMPVERSION) minor

bump-major:
	$(BUMPVERSION) major

release:
	$(PYTHON) setup.py register sdist bdist_wheel upload --sign --identity="$(PGPIDENT)"

. PHONY: Documentation
Documentation:
	$(PIP) install -r requirements/docs.txt
	(cd "$(SPHINX_DIR)"; $(MAKE) html)
	mv "$(SPHINX_HTMLDIR)" $(DOCUMENTATION)

. PHONY: docs
docs: Documentation

. PHONY: livedocs
livedocs:
	$(PIP) install -r requirements/docs.txt
	$(PIP) install -r requirements/dist.txt
	(cd "$(SPHINX_DIR)"; $(MAKE) livehtml)

dockerimage:
	docker build -t faust-docbuilder -f docker/docbuilder/Dockerfile .

dockerdocs: dockerimage
	-rm -rf "$(DOCUMENTATION)"
	sh docker/docbuilder/make_docs.sh

clean-docs:
	-rm -rf "$(SPHINX_BUILDDIR)"

lint: flakecheck apicheck configcheck readmecheck pep257check vulture

apicheck:
	(cd "$(SPHINX_DIR)"; $(MAKE) apicheck)

configcheck:
	(cd "$(SPHINX_DIR)"; $(MAKE) configcheck)

flakecheck:
	$(FLAKE8) "$(PROJ)" "$(TESTDIR)" examples/

pep257check:
	$(PYDOCSTYLE) "$(PROJ)"

vulture:
	$(VULTURE) "$(PROJ)" "$(TESTDIR)" "$(EXAMPLESDIR)" \
		--min-confidence="$(VULTURE_MIN_CONFIDENCE)"

flakediag:
	-$(MAKE) flakecheck

flakes: flakediag

clean-readme:
	-rm -f $(README)

readmecheck:
	$(ICONV) -f ascii -t ascii $(README) >/dev/null

$(README):
	$(SPHINX2RST) "$(README_SRC)" --ascii > $@

readme: clean-readme $(README) readmecheck

clean-contrib:
	-rm -f "$(CONTRIBUTING)"

$(CONTRIBUTING):
	$(SPHINX2RST) "$(CONTRIBUTING_SRC)" > $@

contrib: clean-contrib $(CONTRIBUTING)

clean-coc:
	-rm -f "$(COC)"

$(COC):
	$(SPHINX2RST) "$(COC_SRC)" > $@

coc: clean-coc $(COC)

clean-pyc:
	-find . -type f -a \( -name "*.pyc" -o -name "*$$py.class" \) | xargs rm
	-find . -type d -name "__pycache__" | xargs rm -r

removepyc: clean-pyc

clean-build:
	rm -rf build/ dist/ .eggs/ *.egg-info/ .tox/ .coverage cover/

clean-git:
	$(GIT) clean -xdn

clean-git-force:
	$(GIT) clean -xdf

test-all: clean-pyc
	$(TOX)

test:
	$(PYTHON) setup.py test

build:
	$(PYTHON) setup.py sdist bdist_wheel

distcheck: lint test clean

dist: readme contrib clean-dist build

typecheck:
	$(PYTHON) -m $(MYPY) $(PROJ)

.PHONY: requirements
requirements:
	$(PIP) install --upgrade pip;\
	for f in `ls requirements/` ; do $(PIP) install -r requirements/$$f ; done

.PHONY: clean-requirements
clean-requirements:
	pip freeze | xargs pip uninstall -y
	$(MAKE) requirements

.PHONY:
hooks:
	$(PRE_COMMIT) install

.PHONY:
cdevelop: develop reqs-ext

.PHONY:
develop: reqs develop-hooks setup-develop

.PHONY:
develop-hooks: hooks

.PHONY:
reqs: reqs-default reqs-test reqs-dist reqs-ci reqs-debug

.PHONY:
reqs-default:
	$(PIP) install -U -r requirements/default.txt

.PHONY:
reqs-test:
	$(PIP) install -U -r requirements/test.txt

.PHONY:
reqs-dist:
	$(PIP) install -U -r requirements/dist.txt

.PHONY:
reqs-ci:
	$(PIP) install -U -r requirements/ci.txt

.PHONY:
reqs-debug:
	$(PIP) install -U -r requirements/extras/debug.txt

.PHONY:
reqs-ext: reqs-rocksdb reqs-fast reqs-uvloop

.PHONY:
reqs-rocksdb:
	pip install --no-cache -U -r requirements/extras/rocksdb.txt

.PHONY:
reqs-fast:
	pip install --no-cache -U -r requirements/extras/fast.txt

.PHONY:
reqs-uvloop:
	pip install --no-cache -U -r requirements/extras/uvloop.txt

.PHONY:
setup-develop:
	$(PYTHON) setup.py develop
