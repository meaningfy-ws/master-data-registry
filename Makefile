SHELL=/bin/bash -o pipefail
BUILD_PRINT = \e[1;34mSTEP:
END_BUILD_PRINT = \e[0m

CURRENT_UID := $(shell id -u)
export CURRENT_UID
# These are constants used for make targets so we can start prod and staging services on the same machine
ENV_FILE := .env

# include .env files if they exist
-include .env

PROJECT_PATH = $(shell pwd)


#-----------------------------------------------------------------------------
# Dev commands
#-----------------------------------------------------------------------------
install:
	@ echo -e "$(BUILD_PRINT)Installing the requirements$(END_BUILD_PRINT)"
	@ python -m pip install --upgrade pip
	@ python -m pip install --no-cache-dir -r requirements.txt --force-reinstall

install-dev:
	@ echo -e "$(BUILD_PRINT)Installing the dev requirements$(END_BUILD_PRINT)"
	@ python -m pip install --upgrade pip
	@ python -m pip install --no-cache-dir -r requirements.dev.txt --force-reinstall

test: test-unit

test-unit:
	@ echo -e "$(BUILD_PRINT)Unit Testing ...$(END_BUILD_PRINT)"
	@ tox -e unit

test-features:
	@ echo -e "$(BUILD_PRINT)Gherkin Features Testing ...$(END_BUILD_PRINT)"
	@ tox -e features

test-e2e:
	@ echo -e "$(BUILD_PRINT)End to End Testing ...$(END_BUILD_PRINT)"
	@ tox -e e2e

test-all-parallel:
	@ echo -e "$(BUILD_PRINT)Complete Testing ...$(END_BUILD_PRINT)"
	@ tox -p

test-all:
	@ echo -e "$(BUILD_PRINT)Complete Testing ...$(END_BUILD_PRINT)"
	@ tox