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

build-master-data-registry:
	@ echo -e "$(BUILD_PRINT)Create master data registry ...$(END_BUILD_PRINT)"
	@ cp .env infra/mdr/.env
	@ cp requirements.txt infra/mdr/requirements.txt
	@ cp -rf master_data_registry infra/mdr/master_data_registry
	@ docker-compose -p common --file infra/mdr/docker-compose.yml --env-file ${ENV_FILE} build --no-cache --force-rm
	@ rm infra/mdr/requirements.txt
	@ rm -rf infra/mdr/master_data_registry || true
	@ rm infra/mdr/.env

start-master-data-registry:
	@ echo -e "$(BUILD_PRINT)Start master data registry ...$(END_BUILD_PRINT)"
	@ docker-compose -p common --file infra/mdr/docker-compose.yml --env-file ${ENV_FILE} up -d

stop-master-data-registry:
	@ echo -e "$(BUILD_PRINT)Stop master data registry ...$(END_BUILD_PRINT)"
	@ docker-compose -p common --file infra/mdr/docker-compose.yml --env-file ${ENV_FILE} down

pull-master-data-registry-git:
	@ echo -e "$(BUILD_PRINT)Pull master data registry ...$(END_BUILD_PRINT)"
	@ git pull

rebuild-and-start-master-data-registry: stop-master-data-registry pull-master-data-registry-git build-master-data-registry start-master-data-registry

start-uvicorn-dev-server:
	@ echo -e "$(BUILD_PRINT)Starting the dev uvicorn server ...$(END_BUILD_PRINT)"
	@ uvicorn master_data_registry.entrypoints.api.main:app --reload


guard-%:
	@ if [ "${${*}}" = "" ]; then \
        echo -e "$(BUILD_PRINT)Environment variable $* not set $(END_BUILD_PRINT)"; \
        exit 1; \
	fi

# Testing that vault is installed
vault-installed: #; @which vault1 > /dev/null
	@ if ! hash vault 2>/dev/null; then \
        echo -e "$(BUILD_PRINT)Vault is not installed, refer to https://www.vaultproject.io/downloads $(END_BUILD_PRINT)"; \
        exit 1; \
	fi
# Get secrets in dotenv format

dev-dotenv-file: guard-VAULT_ADDR guard-VAULT_TOKEN vault-installed
	@ echo -e "$(BUILD_PRINT)Create .env file $(END_BUILD_PRINT)"
	@ echo VAULT_ADDR=${VAULT_ADDR} > .env
	@ echo VAULT_TOKEN=${VAULT_TOKEN} >> .env
	@ echo DOMAIN=localhost >> .env
	@ echo ENVIRONMENT=dev >> .env
	@ echo SUBDOMAIN= >> .env
	@ vault kv get -format="json" ted-data-dev/ted-sws | jq -r ".data.data | keys[] as \$$k | \"\(\$$k)=\(.[\$$k])\"" >> .env

prod-dotenv-file: guard-VAULT_ADDR guard-VAULT_TOKEN vault-installed
	@ echo -e "$(BUILD_PRINT)Create .env file $(END_BUILD_PRINT)"
	@ echo VAULT_ADDR=${VAULT_ADDR} > .env
	@ echo VAULT_TOKEN=${VAULT_TOKEN} >> .env
	@ echo DOMAIN=ted-data.eu >> .env
	@ echo ENVIRONMENT=prod >> .env
	@ echo SUBDOMAIN= >> .env
	@ vault kv get -format="json" ted-data-prod/ted-sws | jq -r ".data.data | keys[] as \$$k | \"\(\$$k)=\(.[\$$k])\"" >> .env
