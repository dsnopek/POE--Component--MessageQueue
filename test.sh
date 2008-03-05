#!/bin/bash

# Note: This is not currently part of the test suite.

PERL_PATH=lib
TESTS="Storage/DBI.pm"

for i in $TESTS; do
	fullPath="test/$i"
	echo "Running $fullPath..."
	perl -I$PERL_PATH test/$i
done

