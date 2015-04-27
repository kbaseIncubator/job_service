#!/bin/bash
export PATH=$PWD/test_deps/bin:$PATH
export TEST_USER=kbasetest
read -s -p "Enter password of kbasetest user: " temp_pwd
export TEST_PWD=$temp_pwd
echo $*
$*
