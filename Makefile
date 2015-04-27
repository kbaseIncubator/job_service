KB_TOP ?= /kb/dev_container
KB_RUNTIME ?= /kb/runtime
DEPLOY_RUNTIME ?= $(KB_RUNTIME)
TARGET ?= /kb/deployment
CURR_DIR = $(shell pwd)
SERVICE_NAME = $(shell basename $(CURR_DIR))
SERVICE_DIR = $(TARGET)/services/$(SERVICE_NAME)
JARS_DIR = $(KB_TOP)/modules/jars/lib/jars
JARS_LIB_DIR = $(TARGET)/lib/jars
WAR_FILE = KBaseJobService.war

TARGET_PORT = 8200
THREADPOOL_SIZE = 50

default: compile

deploy-all: deploy

deploy: deploy-client deploy-service deploy-scripts deploy-docs deploy-worker

test: test-client test-service test-scripts

test-client:
	@echo "No tests for client"

test-service:
	bash ./test_scripts/prepare_test_deps.sh
	bash ./test_scripts/run_tests.sh "ant" "-Djarsdir=$(JARS_DIR)" test

test-scripts:
	@echo "No tests for scripts"

compile: src
	ant -Djarsdir=$(JARS_DIR) war

deploy-client:
	@echo "No deployment for client"

deploy-service:
	@echo "Service folder: $(SERVICE_DIR)"
	mkdir -p $(SERVICE_DIR)
	cp -f ./deploy.cfg $(SERVICE_DIR)
	cp -f ./dist/$(WAR_FILE) $(SERVICE_DIR)
	cp -f ./server_scripts/glassfish_start_service.sh $(SERVICE_DIR)
	cp -f ./server_scripts/glassfish_stop_service.sh $(SERVICE_DIR)
	echo 'if [ -z "$$KB_DEPLOYMENT_CONFIG" ]' > $(SERVICE_DIR)/start_service
	echo 'then' >> $(SERVICE_DIR)/start_service
	echo '    export KB_DEPLOYMENT_CONFIG=$(SERVICE_DIR)/deploy.cfg' >> $(SERVICE_DIR)/start_service
	echo '    #export KB_DEPLOYMENT_CONFIG=$$KB_TOP/deployment.cfg' >> $(SERVICE_DIR)/start_service
	echo 'fi' >> $(SERVICE_DIR)/start_service
	echo "./glassfish_start_service.sh $(SERVICE_DIR)/$(WAR_FILE) $(TARGET_PORT) $(THREADPOOL_SIZE)" >> $(SERVICE_DIR)/start_service
	chmod +x $(SERVICE_DIR)/start_service
	echo "./glassfish_stop_service.sh $(TARGET_PORT)" > $(SERVICE_DIR)/stop_service
	chmod +x $(SERVICE_DIR)/stop_service

deploy-scripts:
	@echo "No deployment for scripts"

deploy-docs:
	@echo "No documentation"
	
deploy-worker:
	ant -Djarsdir=$(JARS_DIR) -Djarslibdir=$(JARS_LIB_DIR) -Dservicedir=$(SERVICE_DIR) script
	cp -f ./dist/job_service_run_task.sh $(TARGET)/bin

clean:
	ant clean