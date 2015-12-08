#!/bin/sh

#
# Run all unit tests
#

echo "Running gatherer tests"
./gatherer_tests.py  || exit 1;

