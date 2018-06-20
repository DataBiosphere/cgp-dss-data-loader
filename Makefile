
include common.mk
MODULES=loader transformer scripts tests datasets/topmed/topmed_107_open_access

all: test

lint:
	flake8 $(MODULES)

mypy:
	mypy --ignore-missing-imports $(MODULES)

tests:=$(wildcard tests/test_*.py)

# A pattern rule that runs a single test module, for example:
#   make tests/test_gen3_input_json.py

$(tests): %.py : mypy lint
	python -m unittest --verbose $*.py

test: $(tests)

.PHONY: all lint mypy test

