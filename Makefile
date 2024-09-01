#################################################################################
#
# Makefile to build the Streamlit project
#
#################################################################################

PROJECT_NAME = streamlit_app
REGION = eu-west-2
PYTHON_INTERPRETER = python3
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip3

## Create python interpreter environment.
create-environment:
	@echo ">>> Creating environment: $(PROJECT_NAME)..."
	@echo ">>> Checking Python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
define execute_in_env
	(source venv/bin/activate && $1)
endef

## Set up log directory
logdirs:
	mkdir -p logs

## Build the environment requirements
requirements: create-environment logdirs
	$(call execute_in_env, $(PIP) install -r ./streamlit_app/requirements.txt)

## Set up dependencies/python directory and install specific packages
# custom-dependencies: create-environment logdirs
# 	@echo ">>> Setting up dependencies/python directory..."
# 	mkdir -p dependencies/python
# 	@echo ">>> Installing pg8000 to dependencies/python..."
# 	$(call execute_in_env, $(PIP) install pg8000 -t dependencies/python --no-cache-dir)
# 	@echo ">>> Installing forex_python to dependencies/python..."
# 	$(call execute_in_env, $(PIP) install forex_python -t dependencies/python --no-cache-dir)

all-requirements: requirements custom-dependencies

## Run Terraform Init
terraform-init:
	@echo ">>> Initializing Terraform"
	cd terraform && terraform init

## Run Terraform Plan
terraform-plan: custom-dependencies terraform-init
	@echo ">>> Running Terraform Plan ..."
	cd terraform && terraform plan

## Run Terraform Apply
terraform-apply: custom-dependencies terraform-init
	@echo ">>> Running Terraform Apply ..."
	cd terraform && terraform apply -auto-approve

## Run Terraform Destroy
terraform-destroy: custom-dependencies terraform-init
	@echo ">>> Destroying Terraform-managed infrastructure ..."
	cd terraform && terraform destroy -auto-approve

## Upload Parquet files to S3
upload:
	$(call execute_in_env, python upload_script/upload_to_s3.py)

# Clean up dependencies after deployment 
clean-dependencies:
	@echo ">>> Cleaning dependencies/python ..."
	rm -rf dependencies/python
#   rm -rf venv

################################################################################################################
# Set Up Development Tools

## Install bandit for security linting
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety for dependency vulnerability checking
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install black for code formatting
black:
	$(call execute_in_env, $(PIP) install black==22.12.0)

## Install coverage for test coverage reports
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Install flake8 for code linting
flake8:
	$(call execute_in_env, $(PIP) install flake8)

## Set up development requirements (bandit, safety, flake8, black, coverage)
dev-setup: bandit safety black coverage flake8

################################################################################################################
## Test and Security

## Set up test database (example)
setup-db:
	@echo ">>> Setting up test database..."
	$(psql -f db/db.sql)
	
## Run the security test (bandit + safety)
security-test:
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, bandit -lll */*.py *c/*.py)

## Run the black code formatting check
run-black:
	$(call execute_in_env, black streamlit_app test)

## Run flake8 linting
run-flake8:
	$(call execute_in_env, flake8 ./streamlit_app/*.py ./test/*.py)

## Run unit tests
unit-test: setup-db
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vvv \
	--ignore=dependencies/python/ \
	--disable-warnings --testdox)

## Run all unit tests including additional test scripts
unit-test-all: setup-db
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vvv --disable-warnings --testdox --no-summary)

## Run test coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=streamlit_app test/)

## Run all checks (black, security, unit tests, coverage)
run-checks: run-black security-test unit-test check-coverage

################################################################################################################
## Utility

## Full workflow: Clean, set up environment, run Terraform plan
plan: clean-dependencies custom-dependencies terraform-plan

## Full workflow: Clean, set up environment, apply Terraform changes
apply: clean-dependencies custom-dependencies terraform-apply

## Full workflow: Clean, set up environment, destroy Terraform infrastructure
destroy: clean-dependencies custom-dependencies terraform-destroy
