#!/bin/bash
TEMP_TEST_DEPS_DIR=test_deps
if [[ -f $TEMP_TEST_DEPS_DIR/bin/shock-server && -f $TEMP_TEST_DEPS_DIR/bin/awe-server && -f $TEMP_TEST_DEPS_DIR/bin/awe-client ]]; then
	exit 0
fi
rm -rf $TEMP_TEST_DEPS_DIR
mkdir $TEMP_TEST_DEPS_DIR
cd $TEMP_TEST_DEPS_DIR
mkdir bin
mkdir gopath
export GOPATH=$PWD/gopath
mkdir -p $GOPATH/src/github.com/MG-RAST
git clone https://github.com/kbase/shock_service
cd shock_service
git submodule init
git submodule update
cp -r Shock $GOPATH/src/github.com/MG-RAST/
go get -v github.com/MG-RAST/Shock/...
cd ..
cp $GOPATH/bin/shock-server ./bin/
git clone https://github.com/kbase/awe_service
cd awe_service
git submodule init
git submodule update
cp -r AWE $GOPATH/src/github.com/MG-RAST/
go get -v github.com/MG-RAST/AWE/...
cd ..
cp $GOPATH/bin/awe-server ./bin/
cp $GOPATH/bin/awe-client ./bin/
cd ..
